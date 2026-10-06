from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from ..common.dates import bounds_where, canonical_date, day_expr, partition_where, quote_ident
from ..common.hashing import collapse_decimal_sql as _collapse_decimal_sql
from ..common.hashing import key_family, sample_csv
from ..common.sampling import sample_predicate_sql
from ..common.stats import compute_csv_statistics

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
    def __init__(self, spark, config: dict[str, Any]):
        self.spark = spark
        self.config = dict(config)
        macro = str(config.get("conn_macro") or "")
        if "." in macro and not (config.get("catalog") or config.get("schema")):
            self.config["catalog"], self.config["schema"] = macro.split(".", 1)

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

    def _rows(self, sql: str) -> list[tuple]:
        log.debug("spark.sql: %s", sql)
        return [tuple(row) for row in self.spark.sql(sql).collect()]

    def preflight(self) -> dict[str, Any]:
        try:
            columns = {
                row[0]: str(row[1]).lower()
                for row in self._rows(f"DESCRIBE QUERY SELECT * FROM {self.relation()}")
                if row[0] and not str(row[0]).startswith("#")
            }
            if self.config["date_col"] not in columns:
                raise ValueError(f"date_col {self.config['date_col']!r} not in {sorted(columns)}")
            self.column_types = columns
            return {"ok": True, "source": "databricks", "columns": sorted(columns)}
        except Exception as exc:
            return {"ok": False, "source": "databricks", "error": str(exc)}

    def partition_counts(
        self, from_date: str | None = None, to_date: str | None = None
    ) -> dict[str, int]:
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
            f"SELECT {ref}, COUNT(*) FROM {self.relation()} WHERE {self._where(bounds)} GROUP BY {ref}"
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
        kinds = {}
        selects = ["count(1) AS n_total"]
        for i, (left_column, column) in enumerate(zip(manifest["columns"], right_columns)):
            ref = quote_ident(column, "spark")
            column_type = self.column_types.get(column, "")
            native = any(column_type.startswith(t) for t in NUMERIC_TYPES)
            numeric = overrides.get(left_column, "numeric" if native else "") == "numeric"
            kinds[column] = numeric
            integral = native and any(column_type.startswith(t) for t in INTEGRAL_TYPES)
            text = f"trim(cast({ref} as string))"
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


# >>> local stand-in for the Right side: a CSV per table, same pandas statistics as the left side <<< #
class CsvAnchorTarget:
    def __init__(self, config: dict[str, Any], root: str | Path, work_dir: str | Path):
        self.config = dict(config)
        self.path = Path(root) / f"{config.get('table', 'right')}.csv"
        self.work_dir = Path(work_dir)

    def _frame(self):
        import pandas as pd

        if not self.path.is_file():
            raise FileNotFoundError(self.path)
        return pd.read_csv(self.path, dtype=object)

    def preflight(self) -> dict[str, Any]:
        try:
            columns = list(self._frame().columns)
            if self.config["date_col"] not in columns:
                raise ValueError(f"missing column {self.config['date_col']}")
            return {"ok": True, "source": "csv-anchor", "columns": columns}
        except Exception as exc:
            return {"ok": False, "source": "csv-anchor", "error": str(exc)}

    def partition_counts(
        self, from_date: str | None = None, to_date: str | None = None
    ) -> dict[str, int]:
        values = self._frame()[self.config["date_col"]].map(canonical_date)
        if from_date:
            values = values[values >= canonical_date(from_date)]
        if to_date:
            values = values[values <= canonical_date(to_date)]
        return {str(key): int(value) for key, value in values.value_counts().items()}

    def window_statistics(
        self, manifest: dict[str, Any], window: dict[str, Any]
    ) -> dict[str, dict[str, Any]]:
        frame = self._frame()
        dates = frame[self.config["date_col"]].map(canonical_date)
        right_columns = [manifest["col_map"][column] for column in manifest["columns"]]
        selected = list(
            dict.fromkeys([self.config["date_col"], *manifest["right_key_cols"], *right_columns])
        )
        output = self.work_dir / manifest["pair"] / window["bucket"] / "right.csv"
        output.parent.mkdir(parents=True, exist_ok=True)
        frame.loc[dates.isin(set(window["partitions"])), selected].to_csv(output, index=False)
        plan = manifest["sample"]
        sampled = sample_csv(
            output,
            key_columns=manifest["right_key_cols"],
            mode=plan["mode"],
            rate=plan.get("rate"),
            count=plan.get("count"),
            families=manifest["key_families"],
            key_tuples=window.get("key_tuples"),
            side="right",
        )
        overrides = manifest.get("col_type_overrides") or {}
        return compute_csv_statistics(
            sampled,
            right_columns,
            strict=manifest.get("strict", False),
            col_types={manifest["col_map"][k]: v for k, v in overrides.items()},
        )
