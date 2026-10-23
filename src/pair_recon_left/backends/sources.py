from __future__ import annotations

import csv
import os
import re
import threading
from abc import ABC, abstractmethod
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from ..common.model.hashing import HASH_BUCKET_SPACE, key_family
from ..common.sql.dates import (
    bounds_where,
    canonical_date,
    day_expr,
    partition_where,
    quote_ident,
    resolve_date_type,
)
from ..common.sql.query_windows import normalize_query_windows, render_query_windows
from ..common.sql.sampling import fixed_keys_sql, sample_predicate_sql


def write_rows(path: str | Path, columns: list[str], rows: Iterable[Iterable[object]]) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    part = target.with_name(target.name + ".part")
    with part.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(columns)
        writer.writerows(rows)
    os.replace(part, target)
    return target


def _resolve_column_name(columns: Iterable[str], requested: str) -> str:
    names = [str(column) for column in columns]
    if requested in names:
        return requested
    matches = [name for name in names if name.casefold() == requested.casefold()]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        raise ValueError(f"ambiguous column {requested!r}: {', '.join(matches)}")
    raise ValueError(f"missing columns: {requested}")


def _duckdb_value(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float):
        return str(value)
    if isinstance(value, list | tuple):
        return "[" + ", ".join(_duckdb_value(item) for item in value) + "]"
    return "'" + str(value).replace("'", "''") + "'"


class SqlSource(ABC):
    dialect = "sql"
    date_probe_size = 16

    def __init__(self, config: dict[str, Any]):
        self.config = dict(config)
        windows = normalize_query_windows(
            self.config.get("query"),
            self.config.get("query_windows"),
            source=str(self.config.get("source") or self.dialect),
        )
        if windows:
            self.config["query_windows"] = windows
        else:
            self.config.pop("query_windows", None)
        self.sql_audit = None
        self._date_type = None
        self._date_type_lock = threading.Lock()

    def set_sql_audit(self, audit):
        self.sql_audit = audit
        return self

    @property
    def partition_mode(self) -> str:
        return str(self.config.get("partition_mode") or "date")

    def _audit(self, sql: str, operation: str = "query") -> None:
        if self.sql_audit is not None:
            self.sql_audit.record(sql, backend=self.dialect, operation=operation)

    @abstractmethod
    def connect(self): ...

    def relation(
        self,
        *,
        partitions: list[str] | None = None,
        from_date: str | None = None,
        to_date: str | None = None,
    ) -> str:
        if self.config.get("query"):
            query = render_query_windows(
                self.config["query"],
                self.config.get("query_windows") or {},
                dialect=self.dialect,
                partitions=partitions,
                from_date=from_date,
                to_date=to_date,
            )
            return f"({query}) pair_recon_source"
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
        target = Path(output)
        target.parent.mkdir(parents=True, exist_ok=True)
        part = target.with_name(target.name + ".part")
        connection = self.connect()
        cursor = connection.cursor()
        try:
            self._audit(sql, operation)
            cursor.execute(sql)
            with part.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.writer(handle, lineterminator="\n")
                writer.writerow(columns)
                while rows := cursor.fetchmany(10_000):
                    writer.writerows(rows)
            os.replace(part, target)
            return target
        except Exception:
            part.unlink(missing_ok=True)
            raise
        finally:
            cursor.close()
            connection.close()

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
                resolved = resolved.detect([row[0] for row in rows])
            self._date_type = resolved
            self.config["date_type"] = resolved.spec(portable=True)
            return resolved

    def preflight(self) -> dict[str, Any]:
        try:
            self.query(f"SELECT * FROM {self.relation()} WHERE 1=0", operation="preflight")
            if self.partition_mode == "date":
                self.date_type()
            return {"ok": True, "source": self.config.get("source")}
        except Exception as exc:
            return {"ok": False, "source": self.config.get("source"), "error": str(exc)}

    def key_types(self, key_columns: list[str]) -> list[str | None]:
        return [None] * len(key_columns)

    def partition_counts(
        self,
        from_date: str | None = None,
        to_date: str | None = None,
        *,
        sample: dict[str, Any] | None = None,
        key_columns: list[str] | None = None,
        key_families: list[Any] | None = None,
    ) -> dict[str, int]:
        sampled = sample_predicate_sql(
            sample,
            [quote_ident(column, self.dialect) for column in key_columns or []],
            key_families or [],
            self.dialect,
            "left",
            key_types=self.key_types(key_columns or []),
        )
        if self.partition_mode == "all":
            sql = (
                f"SELECT COUNT(*) AS pair_recon_count FROM {self.relation()} "
                f"WHERE {self._where(sampled)}"
            )
            _columns, rows = self.query(sql, operation="partition_counts")
            return {"all": int(rows[0][0]) if rows else 0}
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
        predicate = f"({bounds}) AND ({sampled})" if bounds and sampled else bounds or sampled
        sql = (
            f"SELECT {date_ref} AS pair_recon_partition, COUNT(*) AS pair_recon_count "
            f"FROM {self.relation(from_date=from_date, to_date=to_date)} "
            f"WHERE {self._where(predicate)} GROUP BY {date_ref}"
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
        if self.partition_mode == "date":
            self.date_type()
            selected = list(
                dict.fromkeys([self.config["date_col"], *(key_columns or []), *columns])
            )
            predicate = partition_where(
                self.config["date_col"],
                partitions,
                dialect=self.dialect,
                date_type=self.config.get("date_type"),
            )
        else:
            selected = list(dict.fromkeys([*(key_columns or []), *columns]))
            predicate = None
        needs_sample = sample_plan and sample_plan.get("mode") != "all"
        key_types = self.key_types(key_columns or []) if needs_sample else []
        stored_hash = self.config.get("sample_hash") or {}
        if stored_hash and stored_hash.get("version") != 1:
            raise ValueError(
                f"unsupported materialized sample hash version: {stored_hash.get('version')}"
            )
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
            f"FROM {self.relation(partitions=partitions if self.partition_mode == 'date' else None)} "
            f"WHERE {self._where(predicate)}"
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
        if self.partition_mode == "date":
            self.date_type()
            predicate = partition_where(
                self.config["date_col"],
                partitions,
                dialect=self.dialect,
                date_type=self.config.get("date_type"),
            )
        else:
            predicate = None
        sql = fixed_keys_sql(
            self.relation(partitions=partitions if self.partition_mode == "date" else None),
            self._where(predicate),
            [quote_ident(column, self.dialect) for column in key_columns],
            key_families or [],
            self.dialect,
            count,
            self.key_types(key_columns),
        )
        _columns, rows = self.query(sql, operation="sample_keys")
        return write_rows(output, key_columns, rows)


class CsvSource(SqlSource):
    dialect = "duckdb"

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self.path = str(config.get("path") or "")
        if not self.path:
            raise ValueError("CSV source requires path")
        nullstr = config.get("nullstr")
        if nullstr is not None and not isinstance(nullstr, str | list | tuple):
            raise TypeError("CSV nullstr must be a string or list")
        if isinstance(nullstr, list | tuple) and not all(isinstance(item, str) for item in nullstr):
            raise TypeError("CSV nullstr must be a string or list")
        if config.get("date_format"):
            self.config["date_type"] = "date"

    def reader(self) -> str:
        options = {"all_varchar": True, "header": True, **(self.config.get("options") or {})}
        if "nullstr" in self.config:
            options["nullstr"] = self.config["nullstr"]
        arguments = [_duckdb_value(self.path)]
        arguments += [f"{key} = {_duckdb_value(value)}" for key, value in options.items()]
        return f"read_csv({', '.join(arguments)})"

    def relation(
        self,
        *,
        partitions: list[str] | None = None,
        from_date: str | None = None,
        to_date: str | None = None,
    ) -> str:
        source = self.reader()
        if self.config.get("query"):
            source = f"({self.config['query'].replace('{src}', source)})"
        date_format = self.config.get("date_format") if self.partition_mode == "date" else None
        if not date_format:
            return f"{source} pair_recon_source"
        column = quote_ident(self.config["date_col"], self.dialect)
        parsed = f"strptime(CAST({column} AS VARCHAR), {_duckdb_value(date_format)})::DATE"
        return f"(SELECT * REPLACE ({parsed} AS {column}) FROM {source}) pair_recon_source"

    def connect(self):
        from ..common.runtime.duck import connect

        return connect(ordered=False)

    def query_to_csv(
        self, sql: str, output: Path, columns: list[str], *, operation: str = "query"
    ) -> Path:
        target = Path(output)
        target.parent.mkdir(parents=True, exist_ok=True)
        part = target.with_name(target.name + ".part")
        connection = self.connect()
        try:
            self._audit(sql, operation)
            cursor = connection.execute(sql)
            with part.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.writer(handle, lineterminator="\n")
                writer.writerow(columns)
                while rows := cursor.fetchmany(10_000):
                    writer.writerows(rows)
            os.replace(part, target)
            return target
        except Exception:
            part.unlink(missing_ok=True)
            raise
        finally:
            connection.close()

    def preflight(self) -> dict[str, Any]:
        try:
            columns, _rows = self.query(
                f"SELECT * FROM {self.relation()} WHERE 1=0", operation="preflight"
            )
            if self.partition_mode == "date":
                self.config["date_col"] = _resolve_column_name(
                    columns, str(self.config["date_col"])
                )
                self.date_type()
            return {"ok": True, "source": "csv", "columns": columns}
        except Exception as exc:
            return {"ok": False, "source": "csv", "error": str(exc)}


def oracle_macro_services() -> dict[str, str]:
    configured = os.getenv("ORACLE_SVC", "")
    pairs = (item.split(":", 1) for item in configured.split(","))
    return {key.strip(): value.strip() for key, value in (p for p in pairs if len(p) == 2)}


LDAP_DSN_PATTERN = re.compile(r"^ldap://([^/]+)/([^,]+),(cn=OracleContext.*)$")


# >>> ldap://host/service,cn=OracleContext,... -> TNS descriptor through ldap3 <<< #
def solve_ldap(ldap_dsn: str) -> str:
    match = LDAP_DSN_PATTERN.match(ldap_dsn.strip())
    if not match:
        raise ValueError(f"unsupported LDAP_DSN: {ldap_dsn!r}")
    host, service, context = match.groups()
    from ldap3 import ALL, Connection, Server

    connection = Connection(Server(host, get_info=ALL))
    if not connection.bind():
        raise RuntimeError(f"LDAP bind to {host} failed: {connection.result!r}")
    connection.search(context, f"(cn={service})", attributes=["orclNetDescString"])
    entry = connection.entries[0] if connection.entries else None
    descriptor = getattr(getattr(entry, "orclNetDescString", None), "value", None)
    if not descriptor:
        raise RuntimeError(f"LDAP has no orclNetDescString for service {service!r} on {host}")
    return str(descriptor)


# >>> credentials by name: explicit field, *_env, <macro>_USR/_PWD/_DSN/_LDAP_DSN, then ORACLE_*/LDAP_DSN <<< #
class OracleSource(SqlSource):
    dialect = "oracle"

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self.macro = str(config.get("conn_macro") or "")
        self.user = self._pick("user", "USR") or os.getenv("ORACLE_USER")
        self.password = self._pick("password", "PWD") or os.getenv("ORACLE_PASSWORD")
        self.service = config.get("service") or oracle_macro_services().get(self.macro)
        self.dsn = self._pick("dsn", "DSN") or (None if self.service else os.getenv("ORACLE_DSN"))
        self.ldap_dsn = self._pick("ldap_dsn", "LDAP_DSN") or os.getenv("LDAP_DSN")
        self._resolved_dsn: str | None = None
        self._resolve_lock = threading.Lock()
        self._key_types: dict[tuple[str, ...], list[str | None]] = {}
        self._key_types_lock = threading.Lock()

    def _pick(self, field: str, *suffixes: str) -> str | None:
        names = [self.config.get(f"{field}_env")]
        names += [f"{self.macro}_{suffix}" for suffix in suffixes if self.macro]
        found = (os.getenv(name) for name in names if name)
        return self.config.get(field) or next((value for value in found if value), None)

    # >>> explicit dsn, else LDAP lookup (cached, {service} filled from the macro), else service <<< #
    def resolve_dsn(self) -> str | None:
        if self.dsn or not self.ldap_dsn:
            return self.dsn or self.service
        with self._resolve_lock:
            if self._resolved_dsn is None:
                target = self.ldap_dsn
                if "{service}" in target:
                    if not self.service:
                        raise ValueError(
                            "LDAP_DSN uses {service} but no Oracle service is configured"
                        )
                    target = target.replace("{service}", self.service)
                self._resolved_dsn = solve_ldap(target)
            return self._resolved_dsn

    def connect(self):
        import oracledb

        if not (self.user and self.password and (self.dsn or self.service or self.ldap_dsn)):
            raise ValueError("Oracle requires user, password, and dsn, service, or LDAP_DSN")
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
                    relation = self.relation()
                    sql = f"SELECT {selected} FROM {relation} WHERE {self._where('1=0')}"
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


class HadoopSource(SqlSource):
    dialect = "hadoop"

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self.dsn = config.get("dsn") or os.getenv("HADOOP_DSN")
        self.autocommit = config.get("autocommit", True)
        if not isinstance(self.autocommit, bool):
            raise TypeError("Hadoop autocommit must be true or false")

    def connect(self):
        try:
            import pyodbc
        except ImportError as exc:
            raise ImportError("Hadoop support requires pair-recon[hadoop]") from exc
        if not self.dsn:
            raise ValueError("Hadoop requires dsn or HADOOP_DSN")
        return pyodbc.connect(f"DSN={self.dsn}", autocommit=self.autocommit)


def source_from_config(config: dict[str, Any]):
    source = str(config.get("source") or "").lower()
    if source == "oracle":
        return OracleSource(config)
    if source == "hadoop":
        return HadoopSource(config)
    if source == "csv":
        return CsvSource(config)
    raise ValueError(f"unsupported Left source: {source}")
