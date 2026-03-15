__version__ = "0.1.1"

from dagweaver.weave import weave
from dagweaver.models import Weave, WeaveContext
from dagweaver.filters import filter_weaves
from dagweaver.errors import (
    CyclicDependencyError,
    DagWeaverError,
    DuplicateWeaveError,
    MultipleWeavesInModuleError,
    UnknownDependencyError,
)

# build_dag, build_weaves: lazy-loaded on first access (require apache-airflow)

_lazy_builders = None


def __getattr__(name: str):
    if name in ("build_dag", "build_weaves"):
        global _lazy_builders
        if _lazy_builders is None:
            import logging

            _log = logging.getLogger(__name__)
            _log.info(
                "Lazy-loading dagweaver.builders (requires apache-airflow). "
                "Install with: pip install dagweaver[airflow]"
            )
            import dagweaver.builders as _mod
            _lazy_builders = _mod
        return getattr(_lazy_builders, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "__version__",
    "weave",
    "Weave",
    "WeaveContext",
    "build_dag",
    "build_weaves",
    "filter_weaves",
    "DagWeaverError",
    "DuplicateWeaveError",
    "MultipleWeavesInModuleError",
    "UnknownDependencyError",
    "CyclicDependencyError",
]