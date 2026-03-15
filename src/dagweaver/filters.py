"""Filtering utilities for Weave collections."""

from __future__ import annotations

from copy import copy
from typing import Sequence

from dagweaver.models import Weave


def filter_weaves(
    weaves: dict[str, Weave],
    *,
    include_tags: str | Sequence[str] | None = None,
    exclude_tags: str | Sequence[str] | None = None,
    include_names: str | Sequence[str] | None = None,
    exclude_names: str | Sequence[str] | None = None,
    keep_upstream: bool = False,
    keep_downstream: bool = False,
) -> dict[str, Weave]:
    """Return a subset of *weaves* matching the given filters.

    Filtering order:
      1. ``include_names`` / ``include_tags`` select seed weaves.
      2. ``exclude_names`` / ``exclude_tags`` remove from seeds.
      3. ``keep_upstream`` / ``keep_downstream`` transitively expand the set.

    When neither ``keep_upstream`` nor ``keep_downstream`` is set, upstream
    references pointing outside the selected set are trimmed.
    """
    if not include_tags and not exclude_tags and not include_names and not exclude_names:
        return weaves

    inc_tags = {include_tags} if isinstance(include_tags, str) else set(include_tags or ())
    exc_tags = {exclude_tags} if isinstance(exclude_tags, str) else set(exclude_tags or ())
    inc_names = {include_names} if isinstance(include_names, str) else set(include_names or ())
    exc_names = {exclude_names} if isinstance(exclude_names, str) else set(exclude_names or ())

    seed: set[str] = set()
    for name, w in weaves.items():
        if inc_names and name not in inc_names:
            continue
        if name in exc_names:
            continue
        if inc_tags and not (set(w.tags) & inc_tags):
            continue
        if exc_tags and (set(w.tags) & exc_tags):
            continue
        seed.add(name)

    selected = set(seed)

    if keep_upstream:
        queue = list(selected)
        while queue:
            cur = queue.pop()
            w = weaves.get(cur)
            if not w:
                continue
            for dep in list(w.upstream) + list(w.xcom.values()):
                if dep not in selected:
                    selected.add(dep)
                    queue.append(dep)

    if keep_downstream:
        queue = list(selected)
        while queue:
            cur = queue.pop()
            for name, w in weaves.items():
                if name in selected:
                    continue
                if cur in w.upstream or cur in w.xcom.values():
                    selected.add(name)
                    queue.append(name)

    result: dict[str, Weave] = {}
    for name in selected:
        w = weaves.get(name)
        if not w:
            continue
        if keep_upstream:
            result[name] = w
        else:
            trimmed = copy(w)
            trimmed.upstream = [d for d in w.upstream if d in selected]
            result[name] = trimmed

    return result
