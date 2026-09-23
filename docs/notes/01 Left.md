---
tags: [dtrack, left, row_check, col_check]
updated: 2026-09-23
---

# 01 Left

Index: [[00 Overview]] · Next: [[02 Right]]

Everything the on-prem PC does: read the config, count its side, wait for the Right side's plan,
sample and summarise its side, push, and later fetch the report. Package `src/dtrack_left`.

## Config and connection profiles (`config.py`)

```jsonc
{
  "connections": {
    "ora_pcds": {"source": "oracle", "dsn": "pcds-scan:1521/PCDS", "user_env": "PCDS_USER", "password_env": "PCDS_PASSWORD"},
    "ora_edw":  {"source": "oracle", "dsn": "edw-scan:1521/EDWPRD", "user_env": "EDW_USER",  "password_env": "EDW_PASSWORD"}
  },
  "pairs": {
    "orders": {
      "left":  {"conn_macro": "ora_pcds", "schema": "FINANCE", "table": "ORDERS", "date_col": "BUSINESS_DATE", "date_type": "date"},
      "right": {"source": "databricks", "catalog": "main", "schema": "sales", "table": "orders", "date_col": "business_date", "date_type": "date"},
      "fromDate": "2026-01-01", "toDate": "2026-02-28", "vintage": "month",
      "key": "ORDER_ID", "key_families": {"ORDER_ID": "numeric"}, "sample": 0.25, "strict": false,
      "col_map": {"ORDER_ID": "order_id", "AMOUNT": "amount"}
    }
  }
}
```

```python
def resolve_connections(config):                  # pair[side] = {**profiles[conn_macro], **pair[side]}
def validate_config(config):                      # left source in {oracle, athena, aws, hadoop, csv}; right must be databricks; col_map non-empty
```

Credentials are environment variable *names* (`*_env`), read at connect time. Before anything
reaches S3, `public_pair_config` strips `user/password/dsn/access_token/server_hostname/http_path`
and every `*_env` field; the Right side only ever sees `catalog/schema/table/date_col/date_type`.

## `dtrack row-check` (`row_check.py`)

```python
def run_row_check(config, store, *, run_id=None, pair_names=None, source_factory=source_from_config, pair_workers=4):
    run_id = run_id or make_run_id(config)                       # 20260923T113744Z-<sha256(config)[:6]>
    health = {name: source.preflight() ...}                      # SELECT * ... WHERE 1=0; fail -> RuntimeError before any count
    with ThreadPoolExecutor(pair_workers) as pool:               # pairs in parallel
        counts = {name: pool.submit(count, name).result() ...}
    for name, pair in pairs.items():
        store.put_json(f"row_check/{name}.left.json", {
            "run_id", "pair", "left", "right",                   # credential-free
            "bounds": {"from", "to"}, "date_synonyms",
            "counts": {"2026-01-01": 30, ...},                   # exact COUNT(*) GROUP BY date
            "pair_config": public, "preflight", "timing"})
    store.put_json("row_check/_LEFT_CLOSED", {"pairs": sorted(pairs), "run_id": run_id})
    store.heartbeat()
```

Generated SQL (Oracle, `date_type: date`):

```sql
SELECT "BUSINESS_DATE" AS dtrack_partition, COUNT(*) AS dtrack_count
FROM "FINANCE"."ORDERS"
WHERE ("BUSINESS_DATE" >= DATE '2026-01-01' AND "BUSINESS_DATE" <= DATE '2026-02-28')
GROUP BY "BUSINESS_DATE"
```

Exact counts only; catalog statistics such as `NUM_ROWS` can be stale. The CSV source uses pandas
`value_counts`. The Left side does not reconcile: it cannot see the Right side.

## `dtrack col-check` (`col_check.py`)

```python
def wait_for_manifests(store, *, timeout=1800, poll_interval=3):
    while not store.exists("row_check/_CLOSED"):                 # the Right side's "all pairs reconciled"
        log status changes from _STATUS.json; abort if _PREFLIGHT.json says the Right side failed
    return [manifests without _CLOSED and *.keys.json]

def publish_fixed_keys(store, manifest, work_dir):               # sample: N -> Left chooses N keys per window
    pull only the key columns per window, rank by md5, take N, write manifest/<pair>.keys.json

class ColCheckPipeline:
    def run(self, manifests):
        existing = unit ids already in _INDEX/                   # resume: skip them
        plans = iter_windows_round_robin(manifests)              # (orders, Jan) (customers, Jan) (orders, Feb) ...
        with ThreadPoolExecutor(pull_workers) as pulls, ThreadPoolExecutor(stat_workers) as stats:
            keep the pull pool full (source sessions), keep the stat pool full (local CPU)
            wait(FIRST_COMPLETED); pulled window -> stat queue; stats done -> per column:
                while store.ready_unclaimed() > wip: sleep     # backpressure cap, default 200
                store.commit_unit(seq, unit_id, stats[column], pointer)   # payload first, pointer second
            store.heartbeat()
        if all expected units indexed: store.close_manifests(pairs, expected_units)
```

One SQL per window pulls every mapped column; the split into per-column Units happens at upload.
Holes are honoured by `partition_where`, which turns the admitted-day list into a union of
`BETWEEN` ranges rather than month bounds:

```python
def partition_where(date_col, partitions, *, dialect, date_type=None):
    for start, end in contiguous_runs(partitions):               # [('2026-01-01','2026-01-14'), ('2026-01-16','2026-01-30')]
        clauses.append(f"{col} = {lo}" if start == end else f"{col} BETWEEN {lo} AND {hi}")
```

## Sampling and statistics (`normalization.py`, `sampling.py`, `statistics.py`)

Both sides pick the same rows without talking, because the rule is a formula on the normalised key:

```python
PURE_DECIMAL = re.compile(r"^-?[0-9]+\.[0-9]+$")
def collapse_decimal(text):        # '2.50'->'2.5', '2.0'->'2'; '007' and '1e3' unchanged (legacy rule)
def normalize_value(value, family): # None/blank -> '<<NULL>>'; date -> 'YYYY-MM-DD'; numeric -> collapse_decimal; string -> NFC lower strip
def hash32(value):                  # int(md5(value)[:8], 16), 0 .. 2^32-1
def selected_fraction(values, families, threshold):   # hash32('|'.join(normalised)) < threshold  (threshold = rate * 2^32)
```

`sample_csv` reads with `dtype=object` so pandas never rewrites `007` or `1.10`. Fraction mode
applies `selected_fraction`; fixed mode keeps rows whose normalised key tuple is in the
`keys.json` list. Statistics per column: `col_type`, `n_total`, `n_missing`, exact `n_unique`,
`hash_sum` (sum of sha256-first-16-hex per value), `mean`/`std` or `length_avg`/`length_max`,
`min_val`/`max_val`, `top_10`. With `strict: true` every value is `collapse_decimal(strip()).lower()`
first, so `Gold` and `GOLD` agree.

## `dtrack fetch-report` (`fetch.py`)

```python
def fetch_report(report_store, out_dir, *, run_id=None):
    run_id = run_id or report_store.get_json("latest/pointer.json")["run_id"]
    for key in report_store.list_keys(f"history/{run_id}/"): write it under out_dir
```

## Running without the console script

`python -m dtrack ...` and `python -m dtrack_left ...` are the same CLI. Use them when the
`Scripts\dtrack.exe` launcher is blocked (Windows Defender on locked-down PCs); `python -m` runs the
interpreter directly and creates no new executable.

## `dtrack debug` (`debug.py`)

```bash
dtrack debug -- row-check --config c.json --s3 s3://...          # ipdb at the CLI entry, post-mortem on exceptions
dtrack debug --attach -- col-check --s3 s3://...                 # debugpy on :5678, waits for VS Code "Remote Attach"
```

VS Code `launch.json` entry: `{"type": "debugpy", "request": "attach", "connect": {"host": "localhost", "port": 5678}}`.

## Known gaps

Left pulls whole windows and samples locally (no predicate pushdown yet), `fetchall`, heartbeat only
per window, and `col-check` waits for `row_check/_CLOSED` instead of starting per pair. See
[[plan_coverage]].
