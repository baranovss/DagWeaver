"""Weave discovery and DAG construction for DagWeaver."""

from __future__ import annotations

import importlib
import inspect
import os
import pkgutil
from collections import deque

from airflow.sdk import TaskGroup

from dagweaver.errors import (
    CyclicDependencyError,
    DuplicateWeaveError,
    MultipleWeavesInModuleError,
    UnknownDependencyError,
)
from dagweaver.models import Weave, WeaveContext


# ---------------------------------------------------------------------------
# Weave discovery
# ---------------------------------------------------------------------------

def build_weaves(pkg) -> dict[str, Weave]:
    """Walk package, discover @weave-decorated functions, and build typed Weave objects."""
    weaves: dict[str, Weave] = {}
    sources: dict[str, str] = {}

    for _, module_name, _ in pkgutil.walk_packages(pkg.__path__, pkg.__name__ + "."):
        module = importlib.import_module(module_name)

        module_weaves: list[tuple[str, object]] = []
        for _, member in inspect.getmembers(module, callable):
            meta = getattr(member, "_weave_meta", None)
            if meta is None or getattr(member, "__module__", None) != module.__name__:
                continue
            module_weaves.append((member.__name__, member))

        if len(module_weaves) > 1:
            names = ", ".join(name for name, _ in module_weaves)
            raise MultipleWeavesInModuleError(
                f"Module '{module.__name__}' defines multiple @weave functions: {names}. "
                f"Keep exactly one @weave per module."
            )

        for func_name, member in module_weaves:
            meta = getattr(member, "_weave_meta")
            name = os.path.splitext(os.path.basename(inspect.getfile(member)))[0]

            if name in weaves:
                raise DuplicateWeaveError(
                    f"Weave name '{name}' is already defined by module '{sources[name]}', "
                    f"conflicting with '{module.__name__}'. "
                    f"Rename one of the files to avoid the collision."
                )

            sources[name] = module.__name__
            weaves[name] = Weave(
                name=name,
                func=member,
                upstream=list(meta.get("upstream", [])),
                group=list(meta.get("group", [])),
                xcom=dict(meta.get("xcom", {})),
                tags=list(meta.get("tags", [])),
            )

    _validate_weaves(weaves)
    return weaves


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def _validate_weaves(weaves: dict[str, Weave]) -> None:
    """Validate the weave dependency graph. Raises on first error found.

    Checks:
    - All upstream references point to existing weaves.
    - All xcom source references point to existing weaves.
    """
    for name, w in weaves.items():
        for dep in w.upstream:
            if dep not in weaves:
                raise UnknownDependencyError(
                    f"Weave '{name}' has upstream='{dep}', but no weave "
                    f"named '{dep}' exists."
                )
        for param, source in w.xcom.items():
            if source not in weaves:
                raise UnknownDependencyError(
                    f"Weave '{name}' has xcom={{'{param}': '{source}'}}, but no weave "
                    f"named '{source}' exists."
                )


def _find_cycle(weaves: dict[str, Weave], candidates: set[str]) -> list[str] | None:
    """Find one cycle path among candidate nodes via DFS."""
    UNVISITED, VISITING, DONE = 0, 1, 2
    state: dict[str, int] = {n: UNVISITED for n in candidates}
    path: list[str] = []

    def _deps(name: str) -> list[str]:
        w = weaves[name]
        return list(w.upstream) + list(w.xcom.values())

    def dfs(node: str) -> list[str] | None:
        state[node] = VISITING
        path.append(node)
        for dep in _deps(node):
            if dep not in candidates:
                continue
            if state[dep] == UNVISITED:
                result = dfs(dep)
                if result:
                    return result
            elif state[dep] == VISITING:
                start = path.index(dep)
                return path[start:] + [dep]
        path.pop()
        state[node] = DONE
        return None

    for name in sorted(candidates):
        if state[name] == UNVISITED:
            cycle = dfs(name)
            if cycle:
                return cycle
    return None


# ---------------------------------------------------------------------------
# DAG construction
# ---------------------------------------------------------------------------

def _topological_order(weaves: dict[str, Weave]) -> list[str]:
    """Return filenames in dependency order (Kahn's algorithm).

    Considers both upstream and xcom sources as dependencies.
    Raises CyclicDependencyError if the graph contains a cycle.
    """
    in_degree = {name: 0 for name in weaves}
    dependents: dict[str, list[str]] = {name: [] for name in weaves}
    for name, w in weaves.items():
        deps = set(w.upstream)
        deps.update(w.xcom.values())
        for dep in deps:
            if dep in weaves:
                in_degree[name] += 1
                dependents[dep].append(name)

    queue = deque(sorted(n for n, d in in_degree.items() if d == 0))
    order: list[str] = []
    while queue:
        node = queue.popleft()
        order.append(node)
        for child in sorted(dependents[node]):
            in_degree[child] -= 1
            if in_degree[child] == 0:
                queue.append(child)

    if len(order) != len(weaves):
        missing = set(weaves) - set(order)
        cycle = _find_cycle(weaves, missing)
        cycle_str = " -> ".join(cycle) if cycle else ", ".join(sorted(missing))
        raise CyclicDependencyError(
            f"Cycle detected in weave dependencies: {cycle_str}."
        )

    return order


def _get_or_create_group(path: list[str], registry: dict[tuple, TaskGroup]) -> TaskGroup | None:
    """Get or create nested TaskGroups for a group path."""
    parent = None
    for i in range(1, len(path) + 1):
        key = tuple(path[:i])
        if key not in registry:
            registry[key] = TaskGroup(group_id=path[i - 1], parent_group=parent)
        parent = registry[key]
    return parent


def _resolve_xcom(xcom: dict[str, str], last_task_ids: dict[str, str]) -> dict[str, str]:
    """Build kwargs with Jinja XCom pull templates from xcom metadata."""
    kwargs: dict[str, str] = {}
    for param, source_step in xcom.items():
        if source_step in last_task_ids:
            kwargs[param] = "{{ ti.xcom_pull(task_ids='" + last_task_ids[source_step] + "') }}"
    return kwargs


def build_dag(dag, weaves: dict[str, Weave]):
    """Build an Airflow DAG from discovered weaves.

    Creates TaskGroups per group path, wires upstream/xcom dependencies,
    and invokes each weave function inside its own TaskGroup.
    """
    _validate_weaves(weaves)
    order = _topological_order(weaves)

    group_registry: dict[tuple, TaskGroup] = {}
    file_groups: dict[str, TaskGroup] = {}
    last_task_ids: dict[str, str] = {}

    for name in order:
        w = weaves[name]
        ctx = WeaveContext(weave_id=name, filename=name)
        xcom_kwargs = _resolve_xcom(w.xcom, last_task_ids)

        parent = _get_or_create_group(w.group, group_registry)
        with TaskGroup(group_id=name, parent_group=parent) as tg:
            ops = w.func(dag, ctx, **xcom_kwargs)

        file_groups[name] = tg
        if isinstance(ops, list) and ops:
            last_task_ids[name] = ops[-1].task_id

    for name in order:
        w = weaves[name]
        deps = set(w.upstream)
        deps.update(w.xcom.values())
        for dep in deps:
            if dep in file_groups:
                file_groups[dep] >> file_groups[name]

    return dag
