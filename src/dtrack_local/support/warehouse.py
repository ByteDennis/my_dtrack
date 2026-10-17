from __future__ import annotations

import logging
import os
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from urllib.parse import quote

from dtrack_left.backends.sources import write_rows

log = logging.getLogger("dtrack.local.warehouse")
RUNNING = {"PENDING", "RUNNING"}


# >>> enum members as their plain value <<< #
def _value(item: Any) -> Any:
    return getattr(item, "value", item)


# >>> corporate proxy for presigned downloads from USERNAME/PASSWORD/AWS_HOST on :8080; None falls back to HTTPS_PROXY <<< #
def chunk_proxies() -> dict[str, str] | None:
    user, password, host = (os.getenv(name) for name in ("USERNAME", "PASSWORD", "AWS_HOST"))
    if not (user and password and host):
        return None
    proxy = f"http://{quote(user, safe='')}:{quote(password, safe='')}@{host}:8080"
    return {"http": proxy, "https": proxy}


# >>> one EXTERNAL_LINKS chunk: a presigned URL to a JSON array of rows, fetched through the proxy without Databricks auth <<< #
def download_chunk(url: str, timeout: tuple[float, float] = (30, 600)) -> list[list[Any]]:
    import requests

    response = requests.get(url, proxies=chunk_proxies(), timeout=timeout)
    response.raise_for_status()
    return response.json()


# >>> a Databricks SQL warehouse read through the Statement Execution API; rows stream chunk by chunk <<< #
class Warehouse:
    def __init__(
        self,
        client,
        warehouse_id: str,
        *,
        poll: float = 2.0,
        fetch=download_chunk,
        sql_audit=None,
    ):
        if not warehouse_id:
            raise ValueError("a SQL warehouse id is required (DTRACK_DBX_WAREHOUSE_ID)")
        self.client = client
        self.warehouse_id = warehouse_id
        self.poll = poll
        self.fetch = fetch
        self.sql_audit = sql_audit

    def set_sql_audit(self, audit):
        self.sql_audit = audit
        return self

    def execute(self, sql: str, *, operation: str = "statement", external: bool = False):
        from databricks.sdk.service.sql import (
            Disposition,
            ExecuteStatementRequestOnWaitTimeout,
            Format,
        )

        log.debug("warehouse %s: %s", self.warehouse_id, sql)
        if self.sql_audit is not None:
            self.sql_audit.record(sql, backend="databricks-sql", operation=operation)
        api = self.client.statement_execution
        options = {"disposition": Disposition.EXTERNAL_LINKS} if external else {}
        response = api.execute_statement(
            statement=sql,
            warehouse_id=self.warehouse_id,
            wait_timeout="30s",
            on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE,
            format=Format.JSON_ARRAY,
            **options,
        )
        while _value(response.status.state) in RUNNING:
            time.sleep(self.poll)
            response = api.get_statement(response.statement_id)
        state = _value(response.status.state)
        if state != "SUCCEEDED":
            error = response.status.error.message if response.status.error else state
            raise RuntimeError(f"statement {state}: {error}\n{sql}")
        return response

    def chunks(self, response) -> Iterator[list[list[Any]]]:
        result = response.result
        while result is not None:
            if result.external_links:
                for link in result.external_links:
                    yield self.fetch(link.external_link)
                following = result.external_links[-1].next_chunk_index
            else:
                yield result.data_array or []
                following = result.next_chunk_index
            result = (
                None
                if following is None
                else self.client.statement_execution.get_statement_result_chunk_n(
                    response.statement_id, following
                )
            )

    def rows(self, sql: str, *, operation: str = "query") -> list[tuple[Any, ...]]:
        response = self.execute(sql, operation=operation)
        return [tuple(row) for chunk in self.chunks(response) for row in chunk]

    # >>> stream a query into a CSV via EXTERNAL_LINKS (no 25 MiB INLINE cap) with the Left pull writer; returns the row count <<< #
    def to_csv(
        self,
        sql: str,
        output: str | Path,
        columns: list[str],
        *,
        operation: str = "pull_window",
    ) -> int:
        response = self.execute(sql, operation=operation, external=True)
        counted = [0]

        def rows():
            for chunk in self.chunks(response):
                counted[0] += len(chunk)
                yield from chunk

        write_rows(output, columns, rows())
        log.info("pulled %d row(s) -> %s", counted[0], output)
        return counted[0]
