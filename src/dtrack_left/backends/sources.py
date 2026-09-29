from __future__ import annotations

import csv
import os
from abc import ABC, abstractmethod
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from ..common.dates import bounds_where, canonical_date, partition_where, quote_ident


def write_rows(path: str | Path, columns: list[str], rows: Iterable[Iterable[object]]) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(columns)
        writer.writerows(rows)
    return target


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


READERS = {"csv": "read_csv", "json": "read_json", "ndjson": "read_json", "parquet": "read_parquet"}
SUFFIX_FORMATS = {".txt": "csv", ".tsv": "csv", ".jsonl": "ndjson", ".parq": "parquet"}


def _duckdb_value(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float):
        return str(value)
    if isinstance(value, list | tuple):
        return "[" + ", ".join(_duckdb_value(item) for item in value) + "]"
    return "'" + str(value).replace("'", "''") + "'"


# >>> csv / txt / json / ndjson / parquet read in place by DuckDB, then treated like any SQL source <<< #
class FileSource(SqlSource):
    dialect = "duckdb"

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self.path = str(config.get("path") or config.get("conn_macro") or "")
        suffix = Path(self.path.replace("*", "x")).suffix.lower()
        self.format = str(
            config.get("format") or SUFFIX_FORMATS.get(suffix) or suffix.lstrip(".") or "csv"
        ).lower()
        if self.format not in READERS:
            raise ValueError(f"unsupported file format: {self.format}")
        if config.get("date_format"):
            self.config["date_type"] = "date"

    def reader(self) -> str:
        options = {"all_varchar": True} if self.format == "csv" else {}
        if self.format == "ndjson":
            options["format"] = "newline_delimited"
        options.update(self.config.get("options") or {})
        arguments = [_duckdb_value(self.path)]
        arguments += [f"{key} = {_duckdb_value(value)}" for key, value in options.items()]
        return f"{READERS[self.format]}({', '.join(arguments)})"

    def relation(self) -> str:
        source = self.reader()
        if self.config.get("query"):
            source = f"({self.config['query'].replace('{src}', source)})"
        date_format = self.config.get("date_format")
        if not date_format:
            return f"{source} dtrack_source"
        column = quote_ident(self.config["date_col"], self.dialect)
        parsed = f"strptime(CAST({column} AS VARCHAR), {_duckdb_value(date_format)})::DATE"
        return f"(SELECT * REPLACE ({parsed} AS {column}) FROM {source}) dtrack_source"

    def connect(self):
        import duckdb

        return duckdb.connect()

    def preflight(self) -> dict[str, Any]:
        try:
            columns, _rows = self.query(f"SELECT * FROM {self.relation()} WHERE 1=0")
            if self.config.get("date_col") not in columns:
                raise ValueError(f"missing columns: {self.config.get('date_col')}")
            return {"ok": True, "source": "file", "columns": columns}
        except Exception as exc:
            return {"ok": False, "source": "file", "error": str(exc)}


# >>> legacy LDAP lookup: ldap://host/service,cn=OracleContext,... -> TNS descriptor <<< #
def solve_ldap(ldap_dsn: str) -> str:
    import re

    from ldap3 import ALL, Connection, Server

    match = re.match(r"^ldap://(.+)/(.+),(cn=OracleContext.*)$", ldap_dsn)
    if not match:
        raise ValueError(f"unsupported LDAP_DSN: {ldap_dsn!r}")
    host, service, context = match.groups()
    connection = Connection(Server(host, get_info=ALL))
    if not connection.bind():
        raise RuntimeError(f"LDAP bind to {host} failed: {connection.result!r}")
    connection.search(context, f"(cn={service})", attributes=["orclNetDescString"])
    if not connection.entries or not getattr(connection.entries[0], "orclNetDescString", None):
        raise RuntimeError(f"LDAP has no orclNetDescString for service {service!r} on {host}")
    return connection.entries[0].orclNetDescString.value


def oracle_macro_services() -> dict[str, str]:
    pairs = (item.split(":", 1) for item in os.getenv("DTRACK_ORACLE_MACROS", "").split(","))
    return {key.strip(): value.strip() for key, value in (p for p in pairs if len(p) == 2)}


# >>> credentials by name: explicit field, *_env, <macro>_USR/_PWD/_DSN (legacy), then ORACLE_* <<< #
class OracleSource(SqlSource):
    dialect = "oracle"

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self.macro = str(config.get("conn_macro") or "")
        self.user = self._pick("user", "USR") or os.getenv("ORACLE_USER") or os.getenv("PCDS_USR")
        self.password = self._pick("password", "PWD", "pwd") or os.getenv("ORACLE_PASSWORD")
        self.service = config.get("service") or oracle_macro_services().get(self.macro)
        self.dsn = self._pick("dsn", "DSN") or (None if self.service else os.getenv("ORACLE_DSN"))

    def _pick(self, field: str, *suffixes: str) -> str | None:
        names = [self.config.get(f"{field}_env")]
        names += [f"{self.macro}_{suffix}" for suffix in suffixes if self.macro]
        found = (os.getenv(name) for name in names if name)
        return self.config.get(field) or next((value for value in found if value), None)

    def resolve_dsn(self) -> str | None:
        template = os.getenv("LDAP_DSN", "")
        if self.dsn or not self.service or not template:
            return self.dsn or self.service
        return solve_ldap(template.format(service=self.service))

    def connect(self):
        import oracledb

        if not (self.user and self.password and (self.dsn or self.service)):
            raise ValueError("Oracle requires user, password, and dsn")
        return oracledb.connect(user=self.user, password=self.password, dsn=self.resolve_dsn())


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
            raise ImportError("Athena support requires dtrack[athena]") from exc
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
            raise ImportError("Hadoop support requires dtrack[hadoop]") from exc
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
    if source in {"file", "csv"}:
        return FileSource(config)
    raise ValueError(f"unsupported Left source: {source}")
