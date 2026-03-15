"""Minimal weave decorator for DagWeaver."""

from __future__ import annotations

from typing import Callable, Optional


def weave(
    func: Optional[Callable] = None,
    *,
    upstream: Optional[list[str]] = None,
    group: Optional[list[str] | str] = None,
    xcom: Optional[dict[str, str]] = None,
    tags: Optional[list[str] | str] = None,
) -> Callable:
    """Decorator to mark a function as a DagWeaver weave step."""

    def decorator(fn: Callable) -> Callable:
        resolved_group = (
            [group] if isinstance(group, str)
            else list(group) if group
            else []
        )
        resolved_tags = (
            [tags] if isinstance(tags, str)
            else list(tags) if tags
            else []
        )
        fn._weave_meta = {
            "upstream": list(upstream or []),
            "group": resolved_group,
            "xcom": dict(xcom or {}),
            "tags": resolved_tags,
        }
        return fn

    if func is not None:
        return decorator(func)

    return decorator