"""DagWeaver exception hierarchy."""


class DagWeaverError(Exception):
    """Base exception for all DagWeaver errors."""


class DuplicateWeaveError(DagWeaverError):
    """Two weave modules resolve to the same name."""


class MultipleWeavesInModuleError(DagWeaverError):
    """A single module contains more than one @weave-decorated function."""


class UnknownDependencyError(DagWeaverError):
    """A weave references an upstream or xcom source that does not exist."""


class CyclicDependencyError(DagWeaverError):
    """The weave dependency graph contains a cycle."""
