from __future__ import annotations

import re
from typing import Any

from .dates import bounds_expression_where, partition_expression_where, portable_date_type

QUERY_WINDOW_MARKER = re.compile(
    r"\{\{\s*query_windows:([A-Za-z][A-Za-z0-9_-]*)\s*\}\}"
)
QUERY_WINDOW_PREFIX = re.compile(r"\{\{\s*query_windows\s*:")
QUERY_WINDOW_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_-]*$")


def normalize_query_windows(
    query: str | None,
    specs: Any,
    *,
    source: str,
) -> dict[str, dict[str, Any]]:
    text = str(query or "")
    markers = QUERY_WINDOW_MARKER.findall(text)
    if QUERY_WINDOW_PREFIX.search(QUERY_WINDOW_MARKER.sub("", text)):
        raise ValueError("malformed query_windows marker")
    if not markers and not specs:
        return {}
    if source.lower() != "oracle":
        raise ValueError("query_windows is currently supported only for Oracle queries")
    if not text:
        raise ValueError("query_windows requires query")
    if not isinstance(specs, dict) or not specs:
        raise ValueError("query_windows must be a non-empty object")

    declared = set(specs)
    used = set(markers)
    missing = sorted(used - declared)
    unused = sorted(declared - used)
    if missing:
        raise ValueError(f"query_windows markers lack declarations: {', '.join(missing)}")
    if unused:
        raise ValueError(f"query_windows declarations lack markers: {', '.join(unused)}")

    normalized: dict[str, dict[str, Any]] = {}
    for name, spec in specs.items():
        if not isinstance(name, str) or not QUERY_WINDOW_NAME.fullmatch(name):
            raise ValueError(f"invalid query_windows name: {name!r}")
        if not isinstance(spec, dict):
            raise TypeError(f"query_windows.{name} must be an object")
        unknown = sorted(set(spec) - {"expression", "date_type"})
        if unknown:
            raise ValueError(
                f"query_windows.{name} has unsupported fields: {', '.join(unknown)}"
            )
        expression = spec.get("expression")
        if not isinstance(expression, str) or not expression.strip():
            raise ValueError(f"query_windows.{name}.expression must be a non-empty string")
        normalized[name] = {
            "expression": expression.strip(),
            "date_type": portable_date_type(spec.get("date_type") or "date"),
        }
    return normalized


def render_query_windows(
    query: str,
    specs: dict[str, dict[str, Any]],
    *,
    dialect: str,
    partitions: list[str] | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
) -> str:
    def predicate(name: str) -> str:
        spec = specs[name]
        if partitions is not None:
            return partition_expression_where(
                spec["expression"],
                partitions,
                dialect=dialect,
                date_type=spec["date_type"],
            )
        if from_date or to_date:
            return (
                bounds_expression_where(
                    spec["expression"],
                    dialect=dialect,
                    date_type=spec["date_type"],
                    from_date=from_date,
                    to_date=to_date,
                )
                or "1=1"
            )
        return "1=1"

    rendered = QUERY_WINDOW_MARKER.sub(lambda match: predicate(match.group(1)), query)
    if QUERY_WINDOW_PREFIX.search(rendered):
        raise ValueError("malformed or unresolved query_windows marker")
    return rendered
