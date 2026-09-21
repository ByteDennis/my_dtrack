from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from ...engine.csv_io import write_rows
from ...engine.dates import canonical_date
from ...engine.sql import bounds_where, partition_where, quote_ident


class SqlSource(ABC):
    dialect = "sql"

    def __init__(self, config: dict[str, Any]):
        self.config = dict(config)

    @abstractmethod
    def connect(self): ...

    def relation(self) -> str:
        if self.config.get("query"):
            return f"({self.config['query']}) dtrack_source"
        parts = [self.config.get(name) for name in ("catalog", "schema", "table")]
        parts = [str(part) for part in parts if part]
        if not parts:
            raise ValueError("source requires table or query")
        return ".".join(quote_ident(part, self.dialect) for part in parts)

    def _where(self, extra: str | None = None) -> str:
        clauses = [item for item in (self.config.get("where"), extra) if item]
        return " AND ".join(f"({item})" for item in clauses) or "1=1"

    def query(self, sql: str) -> tuple[list[str], list[tuple[Any, ...]]]:
        connection = self.connect()
        cursor = connection.cursor()
        try:
            cursor.execute(sql)
            columns = [str(item[0]) for item in (cursor.description or [])]
            rows = cursor.fetchall() or [] if cursor.description else []
            return columns, rows
        finally:
            cursor.close()
            connection.close()

    def preflight(self) -> dict[str, Any]:
        try:
            self.query(f"SELECT * FROM {self.relation()} WHERE 1=0")
            return {"ok": True, "source": self.config.get("source")}
        except Exception as exc:  # noqa: BLE001 - preflight reports adapter health
            return {"ok": False, "source": self.config.get("source"), "error": str(exc)}

    def partition_counts(
        self, from_date: str | None = None, to_date: str | None = None
    ) -> dict[str, int]:
        date_col = self.config["date_col"]
        date_ref = quote_ident(date_col, self.dialect)
        bounds = bounds_where(
            date_col,
            dialect=self.dialect,
            date_type=self.config.get("date_type"),
            from_date=from_date,
            to_date=to_date,
        )
        sql = (
            f"SELECT {date_ref} AS dtrack_partition, COUNT(*) AS dtrack_count "
            f"FROM {self.relation()} WHERE {self._where(bounds)} GROUP BY {date_ref}"
        )
        _columns, rows = self.query(sql)
        return {canonical_date(row[0]): int(row[1]) for row in rows}

    def pull_window(
        self,
        partitions: list[str],
        columns: list[str],
        output: Path,
        *,
        key_columns: list[str] | None = None,
    ) -> Path:
        selected = list(dict.fromkeys([self.config["date_col"], *(key_columns or []), *columns]))
        predicate = partition_where(
            self.config["date_col"],
            partitions,
            dialect=self.dialect,
            date_type=self.config.get("date_type"),
        )
        sql = (
            f"SELECT {', '.join(quote_ident(column, self.dialect) for column in selected)} "
            f"FROM {self.relation()} WHERE {self._where(predicate)}"
        )
        _columns, rows = self.query(sql)
        return write_rows(output, selected, rows)
