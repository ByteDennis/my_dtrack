from __future__ import annotations

import atexit
import csv
import os
import shutil
import tempfile
import threading
from abc import ABC, abstractmethod
from collections.abc import Iterable
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import urlparse

from ..common import disabled
from ..common.dates import (
    bounds_where,
    canonical_date,
    day_expr,
    detect_date_type,
    partition_where,
    quote_ident,
    resolve_date_type,
)
from ..common.hashing import HASH_BUCKET_SPACE, key_family
from ..common.sampling import fixed_keys_sql, sample_predicate_sql


def write_rows(path: str | Path, columns: list[str], rows: Iterable[Iterable[object]]) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(columns)
        writer.writerows(rows)
    return target


def _resolve_column_name(columns: Iterable[str], requested: str) -> str:
    """Prefer an exact column name, then accept one unambiguous case-insensitive match."""
    names = [str(column) for column in columns]
    if requested in names:
        return requested
    matches = [name for name in names if name.casefold() == requested.casefold()]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        raise ValueError(f"ambiguous column {requested!r}: {', '.join(matches)}")
    raise ValueError(f"missing columns: {requested}")


class SqlSource(ABC):
    dialect = "sql"
    date_probe_size = 16

    def __init__(self, config: dict[str, Any]):
        self.config = dict(config)
        self.sql_audit = None
        self._date_type = None
        self._date_type_lock = threading.Lock()

    def set_sql_audit(self, audit):
        self.sql_audit = audit
        return self

    def _audit(self, sql: str, operation: str = "query") -> None:
        if self.sql_audit is not None:
            self.sql_audit.record(sql, backend=self.dialect, operation=operation)

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

    def query(
        self, sql: str, *, operation: str = "query"
    ) -> tuple[list[str], list[tuple[Any, ...]]]:
        connection = self.connect()
        cursor = connection.cursor()
        try:
            self._audit(sql, operation)
            cursor.execute(sql)
            columns = [str(item[0]) for item in (cursor.description or [])]
            rows = cursor.fetchall() or [] if cursor.description else []
            return columns, rows
        finally:
            cursor.close()
            connection.close()

    def query_to_csv(
        self, sql: str, output: Path, columns: list[str], *, operation: str = "query"
    ) -> Path:
        _columns, rows = self.query(sql, operation=operation)
        return write_rows(output, columns, rows)

    def date_type(self):
        with self._date_type_lock:
            if self._date_type is not None:
                return self._date_type
            resolved = resolve_date_type(self.config.get("date_type"))
            if resolved.requires_probe:
                column = quote_ident(self.config["date_col"], self.dialect)
                limit = (
                    f"FETCH FIRST {self.date_probe_size} ROWS ONLY"
                    if self.dialect == "oracle"
                    else f"LIMIT {self.date_probe_size}"
                )
                sql = (
                    f"SELECT {column} FROM {self.relation()} "
                    f"WHERE {self._where(f'{column} IS NOT NULL')} {limit}"
                )
                _columns, rows = self.query(sql, operation="date_type_probe")
                resolved = detect_date_type(resolved, [row[0] for row in rows])
            self._date_type = resolved
            self.config["date_type"] = resolved.spec(portable=True)
            return resolved

    def preflight(self) -> dict[str, Any]:
        try:
            self.query(f"SELECT * FROM {self.relation()} WHERE 1=0", operation="preflight")
            self.date_type()
            return {"ok": True, "source": self.config.get("source")}
        except Exception as exc:
            return {"ok": False, "source": self.config.get("source"), "error": str(exc)}

    def key_types(self, key_columns: list[str]) -> list[str | None]:
        return [None] * len(key_columns)

    def partition_counts(
        self, from_date: str | None = None, to_date: str | None = None
    ) -> dict[str, int]:
        self.date_type()
        date_col = self.config["date_col"]
        date_ref = day_expr(date_col, self.dialect, self.config.get("date_type"))
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
        _columns, rows = self.query(sql, operation="partition_counts")
        counts: dict[str, int] = {}
        for value, count in rows:
            day = canonical_date(value)
            counts[day] = counts.get(day, 0) + int(count)
        return counts

    def pull_window(
        self,
        partitions: list[str],
        columns: list[str],
        output: Path,
        *,
        key_columns: list[str] | None = None,
        sample_plan: dict[str, Any] | None = None,
        key_families: list[Any] | None = None,
        side: str = "left",
        key_tuples: list[list[object]] | None = None,
    ) -> Path:
        self.date_type()
        selected = list(dict.fromkeys([self.config["date_col"], *(key_columns or []), *columns]))
        predicate = partition_where(
            self.config["date_col"],
            partitions,
            dialect=self.dialect,
            date_type=self.config.get("date_type"),
        )
        needs_sample = sample_plan and sample_plan.get("mode") != "all"
        key_types = self.key_types(key_columns or []) if needs_sample else []
        stored_hash = self.config.get("sample_hash") or {}
        if stored_hash and stored_hash.get("version") != 1:
            raise ValueError(f"unsupported materialized sample hash version: {stored_hash.get('version')}")
        family_specs = [key_family(spec).spec() for spec in key_families or []]
        use_stored_hash = (
            sample_plan
            and sample_plan.get("mode") == "fraction"
            and stored_hash
            and list(key_columns or []) == stored_hash.get("keys")
            and family_specs == stored_hash.get("families")
        )
        sample = (
            f"{quote_ident(stored_hash['column'], self.dialect)} < "
            f"{int(sample_plan.get('threshold') or round(float(sample_plan['rate']) * HASH_BUCKET_SPACE))}"
            if use_stored_hash
            else sample_predicate_sql(
                sample_plan,
                [quote_ident(column, self.dialect) for column in key_columns or []],
                key_families or [],
                self.dialect,
                side,
                key_tuples,
                key_types,
            )
        )
        predicate = " AND ".join(f"({item})" for item in (predicate, sample) if item)
        sql = (
            f"SELECT {', '.join(quote_ident(column, self.dialect) for column in selected)} "
            f"FROM {self.relation()} WHERE {self._where(predicate)}"
        )
        return self.query_to_csv(sql, output, selected, operation="pull_window")

    def sample_keys(
        self,
        partitions: list[str],
        key_columns: list[str],
        output: Path,
        *,
        count: int,
        key_families: list[Any] | None = None,
    ) -> Path:
        self.date_type()
        predicate = partition_where(
            self.config["date_col"],
            partitions,
            dialect=self.dialect,
            date_type=self.config.get("date_type"),
        )
        sql = fixed_keys_sql(
            self.relation(),
            self._where(predicate),
            [quote_ident(column, self.dialect) for column in key_columns],
            key_families or [],
            self.dialect,
            count,
            self.key_types(key_columns),
        )
        _columns, rows = self.query(sql, operation="sample_keys")
        return write_rows(output, key_columns, rows)


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

    def query_to_csv(
        self, sql: str, output: Path, columns: list[str], *, operation: str = "query"
    ) -> Path:
        target = Path(output)
        target.parent.mkdir(parents=True, exist_ok=True)
        connection = self.connect()
        try:
            self._audit(sql, operation)
            cursor = connection.execute(sql)
            with target.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.writer(handle, lineterminator="\n")
                writer.writerow(columns)
                while rows := cursor.fetchmany(10_000):
                    writer.writerows(rows)
            return target
        finally:
            connection.close()

    def preflight(self) -> dict[str, Any]:
        try:
            columns, _rows = self.query(
                f"SELECT * FROM {self.relation()} WHERE 1=0", operation="preflight"
            )
            requested = self.config.get("date_col")
            if requested:
                self.config["date_col"] = _resolve_column_name(columns, str(requested))
            self.date_type()
            return {"ok": True, "source": "file", "columns": columns}
        except Exception as exc:
            return {"ok": False, "source": "file", "error": str(exc)}


# >>> tables inside a DuckDB database (local file or s3:// / https:// URL), attached read-only under its file stem <<< #
class DuckDBSource(SqlSource):
    dialect = "duckdb"

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self.path = str(config.get("path") or config.get("conn_macro") or "")
        if not self.path:
            raise ValueError("duckdb source requires path to a .duckdb database file or URL")
        self.alias = PurePosixPath(urlparse(self.path).path or self.path).stem or "dtrack_db"
        self.snapshot = bool(config.get("snapshot")) and not urlparse(self.path).netloc
        self._copy: str | None = None
        self._copy_lock = threading.Lock()

    # >>> one copy of the database and its .wal per source, so a writer holding the lock does not block us <<< #
    def database(self) -> str:
        if not self.snapshot:
            return self.path
        with self._copy_lock:
            if self._copy is None:
                folder = tempfile.mkdtemp(prefix="dtrack-duckdb-")
                atexit.register(shutil.rmtree, folder, True)
                target = Path(folder) / Path(self.path).name
                shutil.copyfile(self.path, target)
                if Path(self.path + ".wal").is_file():
                    shutil.copyfile(self.path + ".wal", str(target) + ".wal")
                self._copy = str(target)
        return self._copy

    def relation(self) -> str:
        if self.config.get("query"):
            return super().relation()
        parts = [self.config.get(name) for name in ("catalog", "schema", "table")]
        names = [name for part in parts if part for name in str(part).split(".")]
        if not names:
            raise ValueError("source requires table or query")
        return ".".join(quote_ident(name, self.dialect) for name in names)

    def connect(self):
        import duckdb

        connection = duckdb.connect()
        alias = quote_ident(self.alias, self.dialect)
        attach = f"ATTACH {_duckdb_value(self.database())} AS {alias} (READ_ONLY)"
        try:
            self._audit(attach, "attach")
            connection.execute(attach)
        except duckdb.IOException as exc:
            connection.close()
            if "lock" not in str(exc).lower():
                raise
            raise RuntimeError(
                f"{self.path} is open read-write in another process; "
                'set "snapshot": true to read a copy, or open it read-only there'
            ) from exc
        use = f"USE {alias}"
        self._audit(use, "session")
        connection.execute(use)
        return connection

    def query(
        self, sql: str, *, operation: str = "query"
    ) -> tuple[list[str], list[tuple[Any, ...]]]:
        connection = self.connect()
        try:
            self._audit(sql, operation)
            connection.execute(sql)
            columns = [str(item[0]) for item in connection.description or []]
            return columns, connection.fetchall() if connection.description else []
        finally:
            connection.close()


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
        self._key_types: dict[tuple[str, ...], list[str | None]] = {}
        self._key_types_lock = threading.Lock()

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

    def key_types(self, key_columns: list[str]) -> list[str | None]:
        if not key_columns:
            return []
        cache_key = tuple(key_columns)
        with self._key_types_lock:
            if cache_key not in self._key_types:
                connection = self.connect()
                cursor = connection.cursor()
                try:
                    selected = ", ".join(
                        quote_ident(column, self.dialect) for column in key_columns
                    )
                    sql = f"SELECT {selected} FROM {self.relation()} WHERE {self._where('1=0')}"
                    self._audit(sql, "describe_keys")
                    cursor.execute(sql)
                    found = {}
                    for item in cursor.description or []:
                        name = str(item[0]).lower()
                        kind = item[1] if len(item) > 1 else None
                        found[name] = getattr(kind, "name", str(kind) if kind else None)
                    self._key_types[cache_key] = [
                        found.get(column.lower()) for column in key_columns
                    ]
                finally:
                    cursor.close()
                    connection.close()
            return list(self._key_types[cache_key])


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


@disabled(
    "identifiers use double quotes and there is no Kerberos/ODBC auth; export Parquet and use source: file"
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
    if source == "duckdb":
        return DuckDBSource(config)
    raise ValueError(f"unsupported Left source: {source}")
