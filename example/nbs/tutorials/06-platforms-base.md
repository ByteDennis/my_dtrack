# 06 — `platforms/base.py`

`platforms/base.py` is the **largest** module in dtrack (~2300 lines) and the
one every extractor imports. It owns five concerns:

1. The cross-engine **query result cache**.
2. **Identifier helpers**: qname rules, quoting, sas_safe_name.
3. **Date helpers**: vintage bucket expressions, range builders, sentinel
   normalization, format resolution.
4. **Generic SQL builders**: `build_stats_sql`, `build_top10_sql`, used as a
   fallback by the lifeboat path.
5. The **PlatformBuilder ABC** that each per-platform class
   (`AthenaBuilder`, `OracleBuilder`, `HadoopBuilder`, `SASBuilder`)
   inherits from.

Subsequent tutorials assume you know this module — the per-platform code
mostly composes its helpers.

## 1. The cache

### 1.1 Lifecycle

```python
set_cache_db("./csv/.cache.db")      # enable, scoped to one outdir
cache_get(sql, scope) -> dict|None    # SHA256(sql + scope), returns payload
cache_put(sql, scope, value, row_count=None)
set_cache_db(None)                    # disable
```

`scope` is whatever identifies the backend: Athena database, Oracle macro,
Databricks catalog.schema, Hive DSN. Two SQL strings that look identical
but target different schemas don't collide.

### 1.2 Schema (per-outdir SQLite)

```
query_cache(sql_hash PRIMARY KEY, sql_text, scope, row_count, created_at, payload BLOB)
```

`payload` is a pickled `(cols, rows)` tuple. Cache entries are durable —
deleting the file is the documented way to invalidate. There is no TTL.

### 1.3 Override modes

- `set_cache_overwrite(True)` — `cache_get` returns `None`, forcing re-execution,
  but `cache_put` still writes. Used by the web UI's "overwrite cache" toggle.
- env `DTRACK_QUERY_CACHE=0` (or legacy `DTRACK_ATHENA_CACHE=0`) — fully
  disables both read and write.

### 1.4 Stats

`cache_stats()` returns `(hits, misses)` since the last `set_cache_db` call.
Printed at the end of each extractor run when verbose mode is on.

## 2. Identifier helpers

### 2.1 `qualified_name(tbl_cfg)`

Stable, deterministic, the **join key** in every SQLite table. Rules:

- For `sas` source: `lib.name` (e.g. `WORK.trades`).
- For `oracle` source: `macro.name` (e.g. `pb23.trades`).
- For `aws` source: `database.name` (e.g. `analytics_db.trades`).
- For `databricks` source: `catalog.schema.name`.
- For `hadoop` source: `schema.name`.
- For `csv` source: `<folder-name>.<csv-base>` — derived from the path.

Any field that's missing falls back to a synthesized placeholder so the qname
is always non-empty. This is why every reference in the project DB (`source_table`
column) is shaped the same way regardless of upstream system.

### 2.2 `resolve_table(tbl_cfg, full_table=None)`

Returns the **literal** SQL identifier to substitute into FROM clauses.
Differs from `qualified_name`: this one applies engine-specific quoting and
inline-vs-CTE choices for vintage-bucket transforms.

### 2.3 `quote_ident(name, dialect)`

Per-dialect column quoting. Athena/Spark use double quotes; Oracle uses double
quotes but is case-sensitive; Hive uses backticks. The dialect set:

- `"oracle"` (default), `"hive"`, `"hadoop"`, `"athena"`, `"trino"`, `"aws"`,
  `"databricks"`, `"spark"`, `"sas"`.

`_sentinel_dialect_key(dialect)` collapses synonyms to a canonical key for
the sentinel lookup.

### 2.4 `sas_safe_name(name, max_len=18)`

SAS dataset names cap at 32 chars but practical pipelines hit 18 first
(prefix room for `_row`/`_col`). Lowercase, strip non-alnum, truncate.

### 2.5 `is_sas_table(tbl_cfg)` / `is_numeric_type(data_type, is_oracle=True)`

Predicates. The first checks for SAS dataset shape (libref + memname), the
second decides whether a discovered data type should be treated as numeric
for stat selection. Surprisingly nuanced: Oracle `NUMBER(*,0)` is integer,
`NUMBER` without precision is float, Athena `BIGINT`/`INT` are integer, etc.

## 3. Date helpers

### 3.1 `resolve_date_format(date_filter, tbl_cfg)`

The single dispatch point for "given the user's `date_type` config, what
literal format do I emit in WHERE clauses?" Returns one of the labels in
`DATE_TYPE_FORMATS` (`'YYYY-MM-DD'`, `'YYYYMMDD'`, …).

### 3.2 `reformat_date(d, date_format)`

Converts a normalized YYYY-MM-DD date string into the stored shape. The
`fmt_map` here is the one you extend when adding a new format (tutorial 03 §1.5).

### 3.3 `build_date_in_clause` / `build_date_between_clause` /
        `build_date_range_with_gaps`

Three increasingly clever WHERE clause builders. `build_date_in_clause` emits
`col IN (…)`, fine for small sets. `build_date_between_clause` is the classic
two-bound form. `build_date_range_with_gaps` takes a list of matching dates
and finds **contiguous spans**, emitting a smaller `(col BETWEEN A AND B) OR
(col BETWEEN C AND D)` — critical when the list is large enough to blow past
the IN-list cap.

### 3.4 `compute_date_filter(tbl_cfg, db_path, vintage)`

The single entry point used by every col-stats builder. Reads:

- `tbl_cfg._date_bounds` (set by `inject_where_from_config` at gen time);
- `_row_comparison.matching_dates` (when present, takes precedence — keeps
  comparison apples-to-apples).
- The `vintage` parameter (controls bucket alignment).

Returns the SQL WHERE fragment to AND into the col query.

### 3.5 `apply_null_sentinels(col_ref, col_type, dialect, sentinels=None)`

Wraps a column reference in a `CASE WHEN col IN (…sentinels…) THEN NULL
ELSE col END`. The `sentinels` arg defaults to `constants.NULL_SENTINELS`
for the given dialect+type. Tutorial 03 §1.3 explains why.

### 3.6 `_format_sentinel_literal(val, kind, dialect)`

Engine-aware literal renderer. For dates this expands `'9999-12-31'` into
`DATE '9999-12-31'` (Oracle/Athena/Spark) or `to_date('9999-12-31', 'YYYY-MM-DD')`
(Hive when DATE literals are unreliable).

### 3.7 `_parse_sample_vintage` / `_sample_matching_dates`

Drive the `vintage: "sample-N"` path: pick N representative dates from
`_row_comparison.matching_dates`, persist into `_sample_date`, and return
the SQL `IN (…)` clause. Idempotent — re-running on the same data picks the
same dates.

## 4. The generic SQL builders

`build_stats_sql(table, col, date_col, where="", col_type="numeric",
dialect="oracle", cte_prefix="")` and
`build_top10_sql(table, col, date_col, where="", dialect="oracle")` are the
**fallback** path used by the lifeboat when an engine-native builder fails.

They are deliberately conservative: no CTE chain, no GROUP BY trickery, no
approx-distinct. Each one emits the simplest correct SQL the platform
understands. The output flows into `parse_stats_row(row)` which extracts the
14 canonical fields.

The per-engine builders (`_extract_col_athena`, `_build_col_sql_oracle`,
`_build_col_sql_hadoop`, `_extract_col_databricks`) generate **better** SQL
when conditions allow, but if the better SQL fails (degrade path), they fall
back to these.

## 5. The portable hash

`_athena_portable_hash16` / `_oracle_portable_hash16` / `_databricks_portable_hash16`
(scattered across platform modules) all converge on the same idea: produce a
16-bit integer from any value such that `SUM(hash16(col))` over a column is
a stable, order-independent **multiset checksum**.

```
hash16(v) = CAST(SUBSTR(md5(CAST(v AS VARCHAR)), 1, 4) AS BIGINT FROM 16)
```

The interesting part: `SUBSTR(md5_hex, 1, 4)` keeps only 4 hex digits = 16
bits. Why 16? Because:

- Wider yields overflow risk in SUM().
- 16 bits is enough to make collisions rare for typical column cardinalities.
- Every supported engine has a cheap 16-bit hex→integer conversion: Athena
  `from_base(?, 16)`, Spark `conv(?, 16, 10)`, Oracle uses raw_to_int.

A column that disagrees between platforms will almost always produce a
different `hash_sum`; matching `hash_sum`s give the analyst strong evidence
that the multisets are identical even without comparing every value.

## 6. The `PlatformBuilder` ABC

```python
class PlatformBuilder(ABC):
    @abstractmethod
    def stats_sql(self, ...): ...
    @abstractmethod
    def top10_sql(self, ...): ...
    ...
```

Concrete subclasses live in each platform module:

- `AthenaBuilder` in `platforms/athena.py`
- `OracleBuilder` in `platforms/oracle.py`
- `HadoopBuilder` in `platforms/hadoop.py`
- `SASBuilder` in `platforms/sas.py`

The ABC's purpose is to keep `utils_stats.build_column_sql` polymorphic — when
the legacy code path calls `build_column_sql`, it ends up dispatching to the
right builder via the source enum.

New-style code (every extract function added since mid-2026) usually skips
the builder ABC and calls the platform module's `_build_col_sql_*` helper
directly. The ABC is still wired for `utils_stats` callers.

## 7. The Coarse-stat builder

`_build_coarse_sql` is the SQL behind the **Quick** preset's per-column
block:

```sql
SELECT
  ${dt_select} AS dt,
  COUNT(*) AS n_total,
  SUM(CASE WHEN col IS NULL THEN 1 ELSE 0 END) AS n_missing,
  MIN(col) AS min_val,
  MAX(col) AS max_val,
  AVG(LENGTH(col)) AS length_avg,
  MAX(LENGTH(col)) AS length_max
  ${, hash_sum if standard+}
  ${, n_unique  if standard+}
FROM ${table}
${WHERE date_filter}
GROUP BY ${group_by}
```

One pass per column, no freq table, partition-prune-friendly. That's why
Quick is the recommended preset when the user is paying per-byte for
Athena/Databricks scans.

## 8. Mask & merge helpers

`_mask_stats_in_rows(rows, preset, stats_override)` blanks out stat columns
that aren't part of the active preset before writing to CSV. This is how the
14-column schema stays consistent across presets without making the loader
preset-aware.

`_merge_cte_chain(cte_prefix, new_ctes)` merges WITH clauses when multiple
column-extract sub-queries share a base CTE. Avoids re-scanning the source
table N times.

## 9. Mock-mode bypass

`_extract_mock_tables(tables, outdir, types, mock_dir)` is the universal
mock-mode endpoint. When `DTRACK_MOCK` is set, the per-platform `extract_*`
function delegates to this helper instead of connecting to a real backend.
It reads `mock_dir/<qname>_row.csv` and `mock_dir/<qname>_col.csv` if they
exist, applies the date filter in Python, and writes the result to `outdir`
in the canonical schema. E2E tests rely on this — see `tests/e2e/`.

That's the toolkit every extractor builds on. Next file:
`07-platforms-athena.md` — how all of this composes into the Athena runner.
