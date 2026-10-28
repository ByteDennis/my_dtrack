from __future__ import annotations

import json
from typing import Any

from . import runtime


def quoted_table(name: str) -> str:
    return ".".join(f"`{part}`" for part in name.split("."))


def literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _query(workspace, warehouse_id: str, statement: str, *, limit: int) -> dict:
    return runtime.run_sql(
        workspace,
        statement,
        warehouse_id,
        limit=limit,
        source="cache",
    )


def _records(result: dict) -> list[dict[str, Any]]:
    columns = result.get("columns") or []
    return [dict(zip(columns, row)) for row in result.get("rows") or []]


def show(workspace, warehouse_id: str, table: str, cache_key: str) -> dict:
    result = _query(
        workspace,
        warehouse_id,
        f"SELECT * FROM {quoted_table(table)} WHERE cache_key = {literal(cache_key)} LIMIT 1",
        limit=1,
    )
    rows = _records(result)
    if not rows:
        raise ValueError(f"cache key does not exist: {cache_key}")
    return rows[0]


def explain(workspace, warehouse_id: str, table: str, pair: str) -> dict:
    result = _query(
        workspace,
        warehouse_id,
        "SELECT cache_type, statistic_name, state, count(*) AS artifacts, "
        "coalesce(sum(size_bytes), 0) AS size_bytes "
        f"FROM {quoted_table(table)} WHERE pair_name = {literal(pair)} "
        "GROUP BY cache_type, statistic_name, state "
        "ORDER BY cache_type, statistic_name, state",
        limit=10_000,
    )
    rows = _records(result)
    return {
        "pair": pair,
        "artifacts": rows,
        "total_artifacts": sum(int(row.get("artifacts") or 0) for row in rows),
        "total_size_bytes": sum(int(row.get("size_bytes") or 0) for row in rows),
    }


def peek(
    workspace,
    warehouse_id: str,
    table: str,
    cache_key: str,
    *,
    limit: int = 5,
) -> dict:
    record = show(workspace, warehouse_id, table, cache_key)
    uri = str(record.get("uri") or "")
    format_ = str(record.get("format") or "json").lower()
    if not uri:
        raise ValueError(f"cache entry has no uri: {cache_key}")
    if format_ == "parquet":
        result = _query(
            workspace,
            warehouse_id,
            f"SELECT * FROM read_files({literal(uri)}, format => 'parquet') LIMIT {int(limit)}",
            limit=limit,
        )
        return {"cache": record, "preview": result}
    body = workspace.files.download(uri).contents.read()
    text = body.decode("utf-8")
    if format_ == "json":
        value = json.loads(text)
    else:
        value = "\n".join(text.splitlines()[:limit])
    return {"cache": record, "preview": value}


def format_explain(result: dict) -> str:
    rows = result.get("artifacts") or []
    lines = [f"Pair: {result['pair']}"]
    if not rows:
        lines.append("No cache artifacts found.")
    else:
        for row in rows:
            statistic = f"/{row['statistic_name']}" if row.get("statistic_name") else ""
            lines.append(
                f"{row.get('cache_type')}{statistic}: {row.get('state')} "
                f"artifacts={row.get('artifacts')} size_bytes={row.get('size_bytes')}"
            )
    lines.append(
        f"Total: {result.get('total_artifacts', 0)} artifact(s), "
        f"{result.get('total_size_bytes', 0)} byte(s)"
    )
    return "\n".join(lines)


def format_peek(result: dict) -> str:
    cache = result["cache"]
    header = (
        f"{cache.get('cache_key')} | {cache.get('cache_type')} | "
        f"{cache.get('format')} | {cache.get('uri')}"
    )
    preview = result.get("preview")
    if isinstance(preview, dict) and {"columns", "rows", "statement_id"} <= preview.keys():
        body = runtime.format_sql_result(preview)
    elif isinstance(preview, str):
        body = preview
    else:
        body = json.dumps(preview, ensure_ascii=False, indent=2, default=str)
    return f"{header}\n{body}"
