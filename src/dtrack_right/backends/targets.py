from __future__ import annotations

import logging
import threading
from typing import Any

from ..common.column_types import resolve_column_type
from ..common.dates import (
    bounds_where,
    canonical_date,
    day_expr,
    detect_date_type,
    partition_where,
    quote_ident,
    resolve_date_type,
)
from ..common.hashing import collapse_decimal_sql as _collapse_decimal_sql
from ..common.hashing import key_family
from ..common.sampling import sample_predicate_sql

log = logging.getLogger("dtrack.right.target")
NUMERIC_TYPES = (
    "int",
    "bigint",
    "smallint",
    "tinyint",
    "double",
    "float",
    "decimal",
    "long",
    "short",
)
INTEGRAL_TYPES = ("int", "bigint", "smallint", "tinyint", "long", "short")


# >>> normalisation of a Right key column in Spark SQL: the family's right_transform_sql <<< #
def _norm_sql(column: str, family: object) -> str:
    return key_family(family).sql(quote_ident(column, "spark"), "right")


# >>> Right side read through spark.sql: counts, sampling predicate and statistics all run in the warehouse <<< #
class SparkTarget:
    date_probe_size = 16

    def __init__(self, spark, config: dict[str, Any]):
        self.spark = spark
        self.config = dict(config)
        self.sql_audit = None
        self._date_type = None
        self._date_type_lock = threading.Lock()
        macro = str(config.get("conn_macro") or "")
        if "." in macro and not (config.get("catalog") or config.get("schema")):
            self.config["catalog"], self.config["schema"] = macro.split(".", 1)

    def set_sql_audit(self, audit):
        self.sql_audit = audit
        return self

    def _audit(self, sql: str, operation: str = "query") -> None:
        if self.sql_audit is not None:
            self.sql_audit.record(sql, backend="spark", operation=operation)

    def _files(self) -> str:
        options = {"format": self.config.get("format") or "parquet"}
        options.update(self.config.get("options") or {})
        if self.config.get("schema_hints"):
            options["schemaHints"] = self.config["schema_hints"]
        arguments = ["'" + str(self.config["path"]).replace("'", "''") + "'"]
        for key, value in options.items():
            arguments.append(f"{key} => '" + str(value).replace("'", "''") + "'")
        return f"read_files({', '.join(arguments)})"

    def relation(self) -> str:
        kind = {"volume_raw": "files"}.get(self.config.get("kind"), self.config.get("kind"))
        if kind == "files":
            query = self.config.get("query")
            if not query:
                return self._files()
            return f"({query.replace('{src}', self._files())}) dtrack_source"
        if self.config.get("query"):
            return f"({self.config['query']}) dtrack_source"
        parts = [self.config.get(name) for name in ("catalog", "schema", "table")]
        return ".".join(quote_ident(str(part), "spark") for part in parts if part)

    def _where(self, extra: str | None) -> str:
        clauses = [item for item in (self.config.get("where"), extra) if item]
        return " AND ".join(f"({item})" for item in clauses) or "1=1"

    def _rows(self, sql: str, operation: str = "query") -> list[tuple]:
        log.debug("spark.sql: %s", sql)
        self._audit(sql, operation)
        return [tuple(row) for row in self.spark.sql(sql).collect()]

    def date_type(self):
        with self._date_type_lock:
            if self._date_type is not None:
                return self._date_type
            resolved = resolve_date_type(self.config.get("date_type"))
            if resolved.requires_probe:
                column = quote_ident(self.config["date_col"], "spark")
                rows = self._rows(
                    f"SELECT {column} FROM {self.relation()} "
                    f"WHERE {self._where(f'{column} IS NOT NULL')} LIMIT {self.date_probe_size}",
                    "date_type_probe",
                )
                resolved = detect_date_type(resolved, [row[0] for row in rows])
            self._date_type = resolved
            self.config["date_type"] = resolved.spec(portable=True)
            return resolved

    def preflight(self) -> dict[str, Any]:
        try:
            columns = {
                row[0]: str(row[1]).lower()
                for row in self._rows(
                    f"DESCRIBE QUERY SELECT * FROM {self.relation()}", "preflight"
                )
                if row[0] and not str(row[0]).startswith("#")
            }
            if self.config["date_col"] not in columns:
                raise ValueError(f"date_col {self.config['date_col']!r} not in {sorted(columns)}")
            self.column_types = columns
            self.date_type()
            return {
                "ok": True,
                "source": "databricks",
                "columns": sorted(columns),
            }
        except Exception as exc:
            return {"ok": False, "source": "databricks", "error": str(exc)}

    def partition_counts(
        self, from_date: str | None = None, to_date: str | None = None
    ) -> dict[str, int]:
        self.date_type()
        date_col = self.config["date_col"]
        ref = day_expr(date_col, "spark", self.config.get("date_type"))
        bounds = bounds_where(
            date_col,
            dialect="spark",
            date_type=self.config.get("date_type"),
            from_date=from_date,
            to_date=to_date,
        )
        rows = self._rows(
            f"SELECT {ref}, COUNT(*) FROM {self.relation()} WHERE {self._where(bounds)} GROUP BY {ref}",
            "partition_counts",
        )
        counts: dict[str, int] = {}
        for value, count in rows:
            day = canonical_date(value)
            counts[day] = counts.get(day, 0) + int(count)
        return counts

    def _sample_predicate(self, manifest: dict[str, Any], window: dict[str, Any]) -> str | None:
        keys, families = manifest["right_key_cols"], manifest["key_families"]
        return sample_predicate_sql(
            manifest["sample"],
            [quote_ident(key, "spark") for key in keys],
            families,
            "spark",
            "right",
            window.get("key_tuples"),
        )

    def window_statistics(
        self, manifest: dict[str, Any], window: dict[str, Any]
    ) -> dict[str, dict[str, Any]]:
        if not hasattr(self, "column_types"):
            self.preflight()
        self.date_type()
        strict = bool(manifest.get("strict"))
        predicate = partition_where(
            self.config["date_col"],
            window["partitions"],
            dialect="spark",
            date_type=self.config.get("date_type"),
        )
        sample = self._sample_predicate(manifest, window)
        where = self._where(predicate if sample is None else f"{predicate} AND ({sample})")
        right_columns = [manifest["col_map"][column] for column in manifest["columns"]]
        overrides = manifest.get("col_type_overrides") or {}
        column_types = manifest.get("column_types") or {}
        kinds = {}
        selects = ["count(1) AS n_total"]
        for i, (left_column, column) in enumerate(zip(manifest["columns"], right_columns)):
            ref = quote_ident(column, "spark")
            column_type = self.column_types.get(column, "")
            native = any(column_type.startswith(t) for t in NUMERIC_TYPES)
            comparison = column_types.get(left_column)
            numeric = (
                not comparison
                and overrides.get(left_column, "numeric" if native else "") == "numeric"
            )
            kinds[column] = numeric
            integral = native and any(column_type.startswith(t) for t in INTEGRAL_TYPES)
            text = (
                resolve_column_type(comparison).normalize_sql(ref, "spark", "right")
                if comparison
                else f"trim(cast({ref} as string))"
            )
            if numeric:
                text = _collapse_decimal_sql(text)
            if strict:
                text = f"lower({_collapse_decimal_sql(text) if not numeric else text})"
            value = (
                (f"cast({ref} as double)" if native else f"try_cast({text} as double)")
                if numeric
                else text
            )
            extreme = ref if integral and numeric else value
            selects += [
                f"count_if({ref} IS NULL) AS n_missing_{i}",
                f"count(DISTINCT {text}) AS n_unique_{i}",
                f"sum(cast(conv(substr(sha2(coalesce({text}, '<<NULL>>'), 256), 1, 16), 16, 10) as decimal(38,0))) AS hash_sum_{i}",
                (f"avg({value}) AS mean_{i}" if numeric else f"NULL AS mean_{i}"),
                (f"stddev_samp({value}) AS std_{i}" if numeric else f"NULL AS std_{i}"),
                f"cast(min({extreme}) as string) AS min_{i}",
                f"cast(max({extreme}) as string) AS max_{i}",
                (
                    f"NULL AS length_avg_{i}"
                    if numeric
                    else f"avg(length({text})) AS length_avg_{i}"
                ),
                (
                    f"NULL AS length_max_{i}"
                    if numeric
                    else f"max(length({text})) AS length_max_{i}"
                ),
            ]
        sql = f"SELECT {', '.join(selects)} FROM {self.relation()} WHERE {where}"
        self._audit(sql, "window_statistics")
        row = self.spark.sql(sql).collect()[0].asDict()
        stats = {}
        for i, column in enumerate(right_columns):
            numeric = kinds[column]
            stats[column] = {
                "col_type": "numeric" if numeric else "categorical",
                "n_total": int(row["n_total"]),
                "n_missing": int(row[f"n_missing_{i}"] or 0),
                "n_unique": int(row[f"n_unique_{i}"] or 0),
                "hash_sum": str(int(row[f"hash_sum_{i}"] or 0)),
                "mean": None if row[f"mean_{i}"] is None else float(row[f"mean_{i}"]),
                "std": None if row[f"std_{i}"] is None else float(row[f"std_{i}"]),
                "min_val": row[f"min_{i}"],
                "max_val": row[f"max_{i}"],
                "length_avg": None
                if row[f"length_avg_{i}"] is None
                else float(row[f"length_avg_{i}"]),
                "length_max": None
                if row[f"length_max_{i}"] is None
                else int(row[f"length_max_{i}"]),
                "top_10": "",
            }
        return stats
