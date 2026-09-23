from __future__ import annotations

import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import pandas as pd

from .csv_io import write_rows
from .dates import canonical_date
from .sql import bounds_where, partition_where, quote_ident


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
        except Exception as exc:
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


class CsvSource:
    def __init__(self, config: dict[str, Any]):
        self.config = dict(config)
        self.path = Path(config.get("path") or config.get("conn_macro") or "")

    def _frame(self) -> pd.DataFrame:
        if not self.path.is_file():
            raise FileNotFoundError(self.path)
        return pd.read_csv(self.path, dtype=object)

    def preflight(self) -> dict[str, Any]:
        try:
            columns = list(self._frame().columns)
            required = {self.config["date_col"]}
            missing = sorted(required - set(columns))
            if missing:
                raise ValueError(f"missing columns: {', '.join(missing)}")
            return {"ok": True, "source": "csv", "columns": columns}
        except Exception as exc:
            return {"ok": False, "source": "csv", "error": str(exc)}

    def partition_counts(
        self, from_date: str | None = None, to_date: str | None = None
    ) -> dict[str, int]:
        values = self._frame()[self.config["date_col"]].map(canonical_date)
        if from_date:
            values = values[values >= canonical_date(from_date)]
        if to_date:
            values = values[values <= canonical_date(to_date)]
        return {str(key): int(value) for key, value in values.value_counts().items()}

    def pull_window(
        self,
        partitions: list[str],
        columns: list[str],
        output: Path,
        *,
        key_columns: list[str] | None = None,
    ) -> Path:
        frame = self._frame()
        dates = frame[self.config["date_col"]].map(canonical_date)
        selected = list(dict.fromkeys([self.config["date_col"], *(key_columns or []), *columns]))
        output.parent.mkdir(parents=True, exist_ok=True)
        frame.loc[dates.isin(set(partitions)), selected].to_csv(output, index=False)
        return output


class OracleSource(SqlSource):
    dialect = "oracle"

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self.user = config.get("user") or os.getenv(config.get("user_env", "ORACLE_USER"))
        self.password = config.get("password") or os.getenv(
            config.get("password_env", "ORACLE_PASSWORD")
        )
        self.dsn = config.get("dsn") or os.getenv(config.get("dsn_env", "ORACLE_DSN"))

    def connect(self):
        import oracledb

        if not all((self.user, self.password, self.dsn)):
            raise ValueError("Oracle requires user, password, and dsn")
        return oracledb.connect(user=self.user, password=self.password, dsn=self.dsn)


class AthenaSource(SqlSource):
    dialect = "athena"

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self.database = config.get("database") or config.get("schema")
        self.s3_staging_dir = config.get("s3_staging_dir")
        self.region = config.get("region")

    def connect(self):
        try:
            from pyathena import connect
        except ImportError as exc:
            raise ImportError("Athena support requires dtrack-distributed[athena]") from exc
        return connect(
            s3_staging_dir=self.s3_staging_dir,
            region_name=self.region,
            schema_name=self.database,
        )


class HadoopSource(SqlSource):
    dialect = "hadoop"

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self.host = config.get("host")
        self.port = int(config.get("port") or 10000)
        self.database = config.get("database") or config.get("schema") or "default"
        self.auth_mechanism = config.get("auth_mechanism") or "PLAIN"

    def connect(self):
        try:
            from impala.dbapi import connect
        except ImportError as exc:
            raise ImportError("Hadoop support requires dtrack-distributed[hadoop]") from exc
        return connect(
            host=self.host,
            port=self.port,
            database=self.database,
            auth_mechanism=self.auth_mechanism,
        )


def source_from_config(config: dict[str, Any]):
    source = str(config.get("source") or "").lower()
    if source == "oracle":
        return OracleSource(config)
    if source in {"athena", "aws"}:
        return AthenaSource(config)
    if source == "hadoop":
        return HadoopSource(config)
    if source == "csv":
        return CsvSource(config)
    raise ValueError(f"unsupported Left source: {source}")
