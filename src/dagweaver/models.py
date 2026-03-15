"""Typed models for DagWeaver."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass
class Weave:
    """Normalized weave step metadata built from @weave-decorated functions."""

    name: str
    func: Callable[..., Any]
    upstream: list[str] = field(default_factory=list)
    group: list[str] = field(default_factory=list)
    xcom: dict[str, str] = field(default_factory=dict)
    tags: list[str] = field(default_factory=list)


@dataclass
class WeaveContext:
    """Runtime context passed to each weave function during DAG construction."""

    weave_id: str
    filename: str
