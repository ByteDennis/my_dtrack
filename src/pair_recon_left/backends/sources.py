from __future__ import annotations

import csv
import logging
import os
import random
import re
import threading
import time
from abc import ABC, abstractmethod
from collections.abc import Iterable
from contextlib import nullcontext
from pathlib import Path
from typing import Any

from ..common.model.hashing import HASH_BUCKET_SPACE, key_family
from ..common.sql.dates import (
    aggregate_partition_counts,
    bounds_where,
    canonical_date,
    day_expr,
    partition_where,
    quote_ident,
    resolve_date_type,
)
from ..common.sql.query_windows import normalize_query_windows, render_query_windows
from ..common.sql.sampling import fixed_keys_sql, sample_predicate_sql

log = logging.getLogger("pair_recon.left.sources")
_ORACLE_PULLS_LOCK = threading.Lock()
_ORACLE_PULLS_ACTIVE = 0
FETCH_PROGRESS: dict[str, tuple[int, float]] = {}
FETCH_LOG_INTERVAL = 300.0
SESSION_LIMIT = "ORA-02391"


def _env_number(name: str, default):
    return type(default)(os.getenv(name) or default)


# >>> rows fetched so far per running pull, read by the stage watchdog; None clears the entry <<< #
def _fetch_progress(key: str, rows: int | None) -> None:
    with _ORACLE_PULLS_LOCK:
        if rows is None:
            FETCH_PROGRESS.pop(key, None)
        else:
            FETCH_PROGRESS[key] = (rows, time.monotonic())


# >>> connection proxy that gives its session slot back exactly once on close <<< #
class _Session:
    def __init__(self, connection, release):
        self._connection, self._release = connection, release

    def __getattr__(self, name):
        return getattr(self._connection, name)

    def close(self):
        release, self._release = self._release, None
        if release is None:
            return
        try:
            self._connection.close()
        finally:
            release()


def _configured_int(config: dict[str, Any], name: str, default: int, *, minimum: int) -> int:
    try:
        value = int(config.get(name, default))
    except (TypeError, ValueError):
        raise ValueError(f"{name} must be an integer") from None
    if value < minimum:
        qualifier = "positive" if minimum == 1 else "non-negative"
        raise ValueError(f"{name} must be a {qualifier} integer")
    return value


def _transient_oracle_error(exc: BaseException) -> bool:
    transient = (
        "DPY-4011",
        "DPY-6005",
        "ORA-03113",
        "ORA-03114",
        "ORA-03135",
        "ORA-12537",
        "ORA-12547",
        "ORA-12570",
        "ORA-12571",
        "WINERROR 10054",
    )
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        details = current.args[0] if current.args else current
        if getattr(details, "isrecoverable", False):
            return True
        text = str(current).upper()
        if any(code in text for code in transient) or type(current).__name__.startswith("LDAP"):
            return True
        current = current.__cause__ or current.__context__
    return False


def _close_quietly(value) -> None:
    if value is None:
        return
    try:
        value.close()
    except Exception:
        log.debug("could not close failed Oracle resource", exc_info=True)


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
        self.stop = None
        self._date_type = None
        self._date_type_lock = threading.Lock()

    def set_sql_audit(self, audit):
        self.sql_audit = audit
        return self

    def set_stop(self, stop):
        self.stop = stop
        return self

    # >>> registers the live statement for cancellation while it runs; no-op without a stop signal <<< #
    def _guard(self, *handles):
        return self.stop.guard(*handles) if self.stop is not None else nullcontext()

    def _check_stop(self) -> None:
        if self.stop is not None:
            self.stop.check()

    @property
    def partition(self) -> str:
        return str(self.config.get("partition") or "day")

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
                partition=self.partition,
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
            with self._guard(cursor, connection):
                cursor.execute(sql)
                columns = [str(item[0]) for item in (cursor.description or [])]
                rows = cursor.fetchall() or [] if cursor.description else []
            return columns, rows
        finally:
            cursor.close()
            connection.close()

    def query_to_csv(
        self, sql: str, output: Path, columns: list[str], *, operation: str = "query"
    ) -> tuple[Path, int]:
        target = Path(output)
        target.parent.mkdir(parents=True, exist_ok=True)
        part = target.with_name(target.name + ".part")
        connection = self.connect()
        cursor = connection.cursor()
        count = 0
        try:
            self._audit(sql, operation)
            with self._guard(cursor, connection):
                cursor.execute(sql)
                with part.open("w", newline="", encoding="utf-8") as handle:
                    writer = csv.writer(handle, lineterminator="\n")
                    writer.writerow(columns)
                    while rows := cursor.fetchmany(10_000):
                        self._check_stop()
                        writer.writerows(rows)
                        count += len(rows)
            os.replace(part, target)
            return target, count
        except Exception:
            part.unlink(missing_ok=True)
            raise
        finally:
            cursor.close()
            connection.close()

    def _bounds(self, from_date: str | None, to_date: str | None) -> str | None:
        if self.partition == "all":
            return None
        self.date_type()
        return bounds_where(
            self.config["date_col"],
            dialect=self.dialect,
            date_type=self.config.get("date_type"),
            from_date=from_date,
            to_date=to_date,
        )

    # >>> a few sampled rows over the whole range with the real predicate; the smoke stage uses it <<< #
    def sample_rows(
        self,
        from_date: str | None,
        to_date: str | None,
        columns: list[str],
        *,
        key_columns: list[str],
        key_families: list[Any],
        sample: dict[str, Any] | None,
        limit: int = 100,
    ) -> tuple[list[str], list[tuple[Any, ...]]]:
        dated = self.partition != "all"
        selected = list(
            dict.fromkeys([*([self.config["date_col"]] if dated else []), *key_columns, *columns])
        )
        sampled = sample_predicate_sql(
            sample,
            [quote_ident(column, self.dialect) for column in key_columns],
            key_families,
            self.dialect,
            "left",
            key_types=self.key_types(key_columns),
        )
        predicate = " AND ".join(
            f"({item})" for item in (self._bounds(from_date, to_date), sampled) if item
        )
        tail = f"FETCH FIRST {limit} ROWS ONLY" if self.dialect == "oracle" else f"LIMIT {limit}"
        sql = (
            f"SELECT {', '.join(quote_ident(column, self.dialect) for column in selected)} "
            f"FROM {self.relation(from_date=from_date, to_date=to_date)} "
            f"WHERE {self._where(predicate or None)} {tail}"
        )
        return self.query(sql, operation="smoke_rows")

    # >>> how many of the given key tuples the real sampling predicate also selects on this side <<< #
    def count_keys(
        self,
        from_date: str | None,
        to_date: str | None,
        *,
        key_columns: list[str],
        key_families: list[Any],
        sample: dict[str, Any] | None,
        key_tuples: list[list[object]],
        side: str = "left",
    ) -> int:
        keys = [quote_ident(column, self.dialect) for column in key_columns]
        types = self.key_types(key_columns)
        sampled = sample_predicate_sql(
            sample, keys, key_families, self.dialect, side, key_types=types
        )
        fixed = sample_predicate_sql(
            {"mode": "fixed"}, keys, key_families, self.dialect, side, key_tuples, types
        )
        predicate = " AND ".join(
            f"({item})" for item in (self._bounds(from_date, to_date), sampled, fixed) if item
        )
        _columns, rows = self.query(
            f"SELECT COUNT(*) FROM {self.relation(from_date=from_date, to_date=to_date)} "
            f"WHERE {self._where(predicate or None)}",
            operation="smoke_keys",
        )
        return int(rows[0][0]) if rows else 0

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

    def health_check(self) -> dict[str, Any]:
        try:
            self.query(f"SELECT * FROM {self.relation()} WHERE 1=0", operation="health_check")
            if self.partition != "all":
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
        if self.partition == "all":
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
        return aggregate_partition_counts(counts, self.partition)

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
    ) -> tuple[Path, int]:
        if self.partition != "all":
            self.date_type()
            selected = list(
                dict.fromkeys([self.config["date_col"], *(key_columns or []), *columns])
            )
            predicate = partition_where(
                self.config["date_col"],
                partitions,
                dialect=self.dialect,
                date_type=self.config.get("date_type"),
                grain=self.partition,
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
            f"FROM {self.relation(partitions=partitions if self.partition != 'all' else None)} "
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
        if self.partition != "all":
            self.date_type()
            predicate = partition_where(
                self.config["date_col"],
                partitions,
                dialect=self.dialect,
                date_type=self.config.get("date_type"),
                grain=self.partition,
            )
        else:
            predicate = None
        sql = fixed_keys_sql(
            self.relation(partitions=partitions if self.partition != "all" else None),
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
        date_format = self.config.get("date_format") if self.partition != "all" else None
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
    ) -> tuple[Path, int]:
        target = Path(output)
        target.parent.mkdir(parents=True, exist_ok=True)
        part = target.with_name(target.name + ".part")
        connection = self.connect()
        count = 0
        try:
            self._audit(sql, operation)
            with self._guard(connection):
                cursor = connection.execute(sql)
                with part.open("w", newline="", encoding="utf-8") as handle:
                    writer = csv.writer(handle, lineterminator="\n")
                    writer.writerow(columns)
                    while rows := cursor.fetchmany(10_000):
                        self._check_stop()
                        writer.writerows(rows)
                        count += len(rows)
            os.replace(part, target)
            return target, count
        except Exception:
            part.unlink(missing_ok=True)
            raise
        finally:
            connection.close()

    def health_check(self) -> dict[str, Any]:
        try:
            columns, _rows = self.query(
                f"SELECT * FROM {self.relation()} WHERE 1=0", operation="health_check"
            )
            if self.partition != "all":
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
        self.arraysize = _configured_int(config, "arraysize", 1_000, minimum=1)
        self.prefetchrows = _configured_int(config, "prefetchrows", 1_000, minimum=0)
        self.fetch_batch_size = _configured_int(config, "fetch_batch_size", 10_000, minimum=1)
        self.fetch_retries = _configured_int(config, "fetch_retries", 3, minimum=0)
        self.max_sessions = _configured_int(
            config, "max_sessions", _env_number("ORACLE_MAX_SESSIONS", 4), minimum=1
        )
        self._sessions = threading.BoundedSemaphore(self.max_sessions)

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
        retries = _env_number("ORACLE_SESSION_RETRIES", 3)
        cooldown = _env_number("ORACLE_SESSION_COOLDOWN_SECONDS", 30.0)
        self._sessions.acquire()
        try:
            for attempt in range(retries + 1):
                try:
                    connection = oracledb.connect(
                        user=self.user, password=self.password, dsn=self.resolve_dsn()
                    )
                    return _Session(connection, self._sessions.release)
                except Exception as exc:
                    if attempt >= retries or SESSION_LIMIT not in str(exc).upper():
                        raise
                    log.warning(
                        "Oracle session limit reached (%s); retry %d/%d in %.0fs",
                        exc,
                        attempt + 1,
                        retries,
                        cooldown,
                    )
                    time.sleep(cooldown)
        except BaseException:
            self._sessions.release()
            raise
        return None

    def query_to_csv(
        self, sql: str, output: Path, columns: list[str], *, operation: str = "query"
    ) -> tuple[Path, int]:
        global _ORACLE_PULLS_ACTIVE

        target = Path(output)
        target.parent.mkdir(parents=True, exist_ok=True)
        part = target.with_name(target.name + ".part")
        self._audit(sql, operation)
        with _ORACLE_PULLS_LOCK:
            _ORACLE_PULLS_ACTIVE += 1
            active = _ORACLE_PULLS_ACTIVE
        log.info(
            "Oracle fetch started: output=%s active=%d arraysize=%d prefetchrows=%d batch_size=%d",
            target,
            active,
            self.arraysize,
            self.prefetchrows,
            self.fetch_batch_size,
        )
        try:
            for attempt in range(self.fetch_retries + 1):
                connection = cursor = None
                started = time.perf_counter()
                execute_seconds = fetch_seconds = write_seconds = 0.0
                first_batch_seconds = None
                row_count = 0
                last_log = started
                try:
                    connection = self.connect()
                    cursor = connection.cursor()
                    cursor.arraysize = self.arraysize
                    cursor.prefetchrows = self.prefetchrows
                    _fetch_progress(str(target), 0)
                    with self._guard(cursor, connection):
                        mark = time.perf_counter()
                        cursor.execute(sql)
                        execute_seconds = time.perf_counter() - mark
                        with part.open("w", newline="", encoding="utf-8") as handle:
                            writer = csv.writer(handle, lineterminator="\n")
                            writer.writerow(columns)
                            while True:
                                mark = time.perf_counter()
                                rows = cursor.fetchmany(self.fetch_batch_size)
                                batch_seconds = time.perf_counter() - mark
                                fetch_seconds += batch_seconds
                                if first_batch_seconds is None:
                                    first_batch_seconds = batch_seconds
                                if not rows:
                                    break
                                self._check_stop()
                                mark = time.perf_counter()
                                writer.writerows(rows)
                                write_seconds += time.perf_counter() - mark
                                row_count += len(rows)
                                _fetch_progress(str(target), row_count)
                                now = time.perf_counter()
                                if now - last_log >= FETCH_LOG_INTERVAL:
                                    last_log = now
                                    log.info(
                                        "Oracle fetch progress: output=%s rows=%d elapsed=%.0fs "
                                        "rows_per_second=%.1f",
                                        target,
                                        row_count,
                                        now - started,
                                        row_count / (now - started),
                                    )
                    os.replace(part, target)
                    elapsed = time.perf_counter() - started
                    size_mb = target.stat().st_size / (1024 * 1024)
                    log.info(
                        "Oracle fetch completed: output=%s rows=%d size=%.2fMiB "
                        "elapsed=%.3fs execute=%.3fs first_batch=%.3fs fetch=%.3fs "
                        "csv_write=%.3fs "
                        "throughput=%.2fMiB/s rows_per_second=%.1f attempt=%d/%d",
                        target,
                        row_count,
                        size_mb,
                        elapsed,
                        execute_seconds,
                        first_batch_seconds or 0.0,
                        fetch_seconds,
                        write_seconds,
                        size_mb / elapsed if elapsed else 0.0,
                        row_count / elapsed if elapsed else 0.0,
                        attempt + 1,
                        self.fetch_retries + 1,
                    )
                    return target, row_count
                except Exception as exc:
                    part.unlink(missing_ok=True)
                    if attempt >= self.fetch_retries or not _transient_oracle_error(exc):
                        raise
                    delay = (5 * (3**attempt)) + random.uniform(0, 1)
                    _close_quietly(cursor)
                    _close_quietly(connection)
                    cursor = connection = None
                    log.warning(
                        "Oracle fetch connection lost: output=%s attempt=%d/%d; "
                        "retrying from the beginning with a new connection in %.1fs: %s",
                        target,
                        attempt + 1,
                        self.fetch_retries + 1,
                        delay,
                        exc,
                    )
                    time.sleep(delay)
                finally:
                    _close_quietly(cursor)
                    _close_quietly(connection)
        finally:
            _fetch_progress(str(target), None)
            with _ORACLE_PULLS_LOCK:
                _ORACLE_PULLS_ACTIVE -= 1

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
                    with self._guard(cursor, connection):
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
