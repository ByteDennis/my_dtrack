---
tags: [dtrack, right, databricks, row_check, col_check]
updated: 2026-09-23
---

# 02 Right

Index: [[00 Overview]] · Previous: [[01 Left]] · Next: [[03 Protocol and Report]]

Everything the Databricks job does, started once and left alone: find the run, count and reconcile
its side, plan, prefetch, compare, report, exit. Package `src/dtrack_right`, entry `notebook.py`.

## Deployment and inputs (`notebook.py`)

- Sync `src/` into a Repo. `notebook.py` inserts its parent folder (`src/`) into `sys.path`, so
  `import dtrack_right` works. No wheel.
- A job cluster is a fresh environment: cell 1 is `%pip install -q -r ./requirements.txt`
  (`openpyxl`, `boto3`), then `dbutils.library.restartPython()`. pyspark and pandas ship with the
  runtime. Alternatively attach `requirements.txt` as a job library.
- Widgets `work`, `run_id`, `report`, `gate`, `idle_timeout`, `wait_timeout` map 1:1 to job
  parameters (see [[00 Overview]] for values and the file-arrival trigger).

```python
outcome = run(dbutils.widgets.get("work"), lambda config: SparkTarget(spark, config),
              run_id=dbutils.widgets.get("run_id") or None, report=dbutils.widgets.get("report"), gate=...)
```

## Lifecycle (`app.py`)

```python
def run(work, target_factory, *, run_id=None, report=None, gate="auto", poll_interval=2, idle_timeout=900, wait_timeout=1800, ...):
    while True:                                                # 1. resolve the run and wait for the first left.json
        store, resolved = resolve_run(work, run_id)            #    root -> newest run not yet complete; explicit run_id -> that folder
        if store is not None and store.list_keys("row_check/"): break
        if elapsed > wait_timeout: raise TimeoutError
    reconcile_pending()                                        # 2. row check for every pending pair
    col_check = ColCheck(store, target_factory, on_idle=reconcile_pending, ...)
    results = col_check.run()                                  # 3. B prefetch / A compare until _CLOSED and counts agree
    files = build_report(store, out_dir)                       # 4. summary.json, CSVs, index.html, report.xlsx
    publish_report(open_store(report), run_id, files, summary) # 5. history/<run_id>/ first, latest/pointer.json last
    store.put_json("_STATUS.json", {"state": "complete", "role": "right", "run_id": resolved})
```

Exits: normal (above); Left heartbeat stale beyond `heartbeat_timeout` (non-zero, results kept);
`idle_timeout` with nothing to do; manual cancel. Restarting on the same `work` resumes.

## Row check on the Right (`row_check.py`, `targets.py`)

```python
def pending_left_documents(store):             # row_check/<pair>.left.json without row_check/<pair>.json
def reconcile_pair(store, left_doc, target, *, gate="auto"):
    health = target.preflight()                # DESCRIBE <relation>; date_col must exist
    right_counts = target.partition_counts(from, to)   # SELECT `dt`, COUNT(*) ... GROUP BY `dt`
    partitions = reconcile_partitions(left_doc["counts"], right_counts, left_doc["date_synonyms"])
    document = {..., "partitions", "summary", "gate_decision": evaluate_gate(partitions, gate), "timing"}
    store.put_json(f"row_check/{pair}.json", document)
    store.put_bytes(f"reports/{pair}_compare_row.csv", row_check_csv_bytes(document))
    if passed: store.put_json(f"manifest/{pair}.json", build_manifest(run_id, pair, left_doc["pair_config"], document))
```

| verdict | meaning | enters col check |
| --- | --- | --- |
| `match` | counts equal | yes |
| `count_diff` | both present, counts differ | no, it is a finding |
| `left_only` / `right_only` | one side lacks the day | no |

Gate is per pair: `auto` filters days and never blocks; `manual` writes verdicts only;
`ratio:x` holds a whole pair back when the excluded share exceeds `x`. `build_manifest` keeps only
`match` days, grouped by `vintage`, so windows carry holes explicitly and `expected_units =
windows × columns`. `SparkTarget.relation()` supports `catalog.schema.table`, a `query`, or
`kind: volume_raw` via `read_files('/Volumes/...')` (ADR 0005).

## Col check on the Right (`scheduler.py`, `col_check.py`)

Two kinds of work; A always outranks B so results appear the moment a Left file lands:

| kind | needs | supply |
| --- | --- | --- |
| A compare one column | Left unit file **and** Right stats for that window | Left-paced |
| B fetch own side for one window | manifest only | available as soon as a pair passes the row check |

```python
class DrainScheduler:
    add_manifest(manifest)      # every window -> B backlog
    add_unit(pointer)           # ready if Right stats exist, else pending
    mark_right_ready(...)       # promote that window's pending units
    next_task()                 # A if any ready; else B, preferring windows whose Left units already arrived, at most prefetch_depth speculative

class ColCheck:
    poll()      # new manifests (fixed-N pairs only once keys.json exists) + new _INDEX entries after the cursor
    run_task()  # B: target.window_statistics(manifest, window) -> stats keyed by Left column name
                # A: compare_statistics(left payload, right stats) -> results/<unit>.json
    _failure()  # retry up to max_attempts, then dead/<unit>.json with traceback; a dead prefetch kills the whole window
    run()       # poll -> task -> (complete? -> return) -> on_idle=reconcile_pending -> heartbeat/idle checks -> sleep
```

Resume: on start `seen = results/ ∪ dead/`, and once `complete()` holds no speculative prefetch is
issued, so a rerun on a finished run does zero scans.

## Right statistics in one `spark.sql` (`targets.py::SparkTarget`)

```python
def _collapse_decimal_sql(text):     # CASE WHEN text rlike '^-?[0-9]+\.[0-9]+$' THEN regexp_replace(...) ELSE text END
def _norm_sql(column, family):       # numeric -> collapse; date -> date_format(cast(.. as date)); string -> lower(trim(..)); NULL/blank -> '<<NULL>>'
def _sample_predicate(manifest, window):
    fraction: conv(substr(md5(concat_ws('|', norm(k1), norm(k2))), 1, 8), 16, 10) < threshold    # == hash32
    fixed:    concat_ws(...) IN ('a|b', ...)                                                        # from keys.json
def window_statistics(manifest, window):
    WHERE partition_where(admitted days, dialect="spark") AND (sample predicate)
    SELECT count(1), count_if(c IS NULL), count(DISTINCT cast(c as string)),
           sum(cast(conv(substr(sha2(coalesce(cast(c as string),'<<NULL>>'),256),1,16),16,10) as decimal(38,0))),
           avg / stddev_samp / min / max  or  avg(length) / max(length) ...   -- one row per window
```

`col_type` comes from `DESCRIBE`; `strict` wraps values in `lower(collapse(trim(..)))`.
`CsvAnchorTarget` implements the same `preflight / partition_counts / window_statistics` with pandas
and is what the e2e uses. **`SparkTarget` has not run on a real cluster yet**; string formatting of
decimals and `min/max` is the first thing to verify ([[plan_coverage]] item 0).

## Comparison rule (`comparison.py`)

Counts (`n_total`, `n_missing`, `n_unique`) must be equal; `mean`, `std`, `length_*`, `hash_sum`
must agree within a relative tolerance of 1e-6; `min`/`max` compare as trimmed strings. The result
row carries both sides' raw values plus `verdict`, `unit_id`, `right_col`, so the report needs no
second read. `unit_id = slug(pair)--slug(window)--slug(column)--sha256[:12]`, identical on both
sides (parity test).
