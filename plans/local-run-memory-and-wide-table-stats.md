# Local Run: Databricks Pull, DuckDB Memory, Wide-Table Statistics

## Context

First real `dtrack_local` run on `CR_BUREAU_TRIGGER` (1715 columns, 7.7M rows, 2 windows,
16 GB Windows laptop) on 2026-10-08. Two problems showed up, one already fixed in the working
tree, one not.

### Fixed in the working tree (not yet committed)

`statement failed: inline byte limit exceeded ... disposition=INLINE ... 26214400 bytes`.

`Warehouse.to_csv` submitted statements with the default INLINE disposition, which caps the
whole result at 25 MiB. Presigned EXTERNAL_LINKS downloads were verified by hand to work only
through the corporate proxy and only with `requests` (not `urllib.request.urlopen`).

Change in `src/dtrack_local/warehouse.py`:

- `to_csv` (every bulk pull: `pull_sample`, `key_check`) now runs with
  `Disposition.EXTERNAL_LINKS`; `rows()` (counts, DESCRIBE) stays INLINE and never touches
  the proxy.
- `download_chunk` uses `requests.get(url, proxies=chunk_proxies(), timeout=(30, 600))` with
  `raise_for_status()`. `chunk_proxies()` builds `http://USERNAME:PASSWORD@AWS_HOST:8080`
  (URL-encoded) and returns `None` when any of the three is unset, so `requests` falls back to
  `HTTPS_PROXY`. The proxy is passed per request, never written into `os.environ`, so the SDK's
  own calls to the Databricks host are unaffected.
- Caveat: Windows sets `USERNAME` to the login name; `load_dotenv` does not override an
  existing variable. If the proxy account differs from the login name the `.env` value is
  silently ignored and the proxy answers 407.

Verified: the run log shows `pulled 7402 row(s)` and `pulled 69996 row(s)` through the new
path with no inline-limit error.

### Still broken: statistics run out of memory and crawl

Timeline from the run log (UTC):

| time  | event                                                                    | elapsed |
|-------|--------------------------------------------------------------------------|---------|
| 05:08 | Right pull 2026-07-27 (7402 rows)                                        | 26 s    |
| 05:38 | Right stats 2026-07-27 ready, 0 units promoted                           | 1772 s  |
| 05:21 | Left pull 2026-08-03 (7 partitions) to stat queue                        | 13 min  |
| 05:41 | Right pull 2026-08-03 (69996 rows)                                       | 3 min   |
| 05:55 | Left pull 2026-07-27 (7402 rows) done                                    | 47 min  |
| 06:14 | Right stats 2026-08-03 OOM: `Allocation failure`                         | 33 min  |
| 06:18 | Right re-pull 2026-08-03 (attempt 2)                                     | 3 min   |
| 07:08 | Right stats OOM again: `24.9 GiB/25.0 GiB used`                          | 50 min  |
| 07:11 | Right re-pull 2026-08-03 (attempt 3)                                     | 3 min   |
| ~07:1x| Left `FileSource.query_to_csv` dies: `closed pending query result` + OOM | crash   |

Wall-clock accounting (log starts 05:07:17, last log line 07:11:39 = 2h04m; attempt-3 stats
ran unlogged until the crash, so the real total is ~2h45m-3h):

| phase                                  | wall-clock          | share  |
|----------------------------------------|---------------------|--------|
| preflight + Stage 1 row counts         | 1m18s               | ~1%    |
| Right pulls (1 + 3x the same window)   | 10m20s              | ~6%    |
| Right stats 2026-07-27 (7402 rows)     | 29m06s              | ~17%   |
| Right stats 2026-08-03 x3 (OOM, OOM, crash) | 33m + 50m + >=30m | ~70%   |
| Left pulls (concurrent, starved)       | 13m / 47m           | off critical path |
| Left stats 2026-08-03                  | started 05:21, never finished | -  |

Useful output after ~3 hours: one Right stat payload, zero units compared (`0 unit(s)
promoted`, because the matching Left stats were not done either). Throughput on the one
window that completed: 7402 x 1715 = 12.7M cells in 1772 s, about 7k cells/s; DuckDB
aggregates normally run 4 orders of magnitude faster. The bottleneck is therefore not I/O,
not Databricks, and not the proxy: it is `compute_csv_statistics` thrashing DuckDB memory
(defects 1-3 below), with defect 4 tripling the damage.

### Second run, same machine, same time: APPL_ANLYS (898 columns, 11 windows of ~270-300k rows)

Started 05:08:28, crashed 09:17:34: **4h09m**. The two runs shared the 16 GB box, so each
saw the other's DuckDB instances as well as its own (8-10 instances in total).

| phase                                         | wall-clock            | share |
|-----------------------------------------------|-----------------------|-------|
| preflight + Stage 1 (57.5M rows, 3926 parts)  | 2m11s                 | 1%    |
| Right pulls 2016 x2, 2017, 2018 (~280k rows)  | 5.5m + 4.8m + 6.4m + 5.7m = 22m | 9% |
| Right stats 2016 #1 (OOM after 2.5m), #2      | 2.5m + 99m            |       |
| Right stats 2017                               | 68m                   | 90%   |
| Right stats 2018                               | 54m                   |       |
| Left pulls 2017 / 2016 (concurrent, starved)   | 26m / 30m             | -     |
| Left `Landing.land` 2017 (271k rows x 898)     | 28m                   | -     |
| Left `Landing.land` 2016                       | OOM, ~06:35 (unlogged)| -     |
| Left stats 2017                                | queued 06:05, never finished | - |

Useful output: three Right stat payloads, zero units compared. Right throughput on the
2016 window: 296k x 898 = 266M cells in 99 min, ~45k cells/s (better than CR_BUREAU's 7k/s
because 898 DISTINCT aggregates fit more comfortably than 1715, still ~3 orders off).

Two extra observations from this log:

- Stat time for near-identical windows fell 99m -> 68m -> 54m as the other run and this run's
  own Left side died and freed memory. Same query, same data, 45% faster with less contention:
  memory oversubscription, not CPU, sets the pace.
- The Left side died around 06:35 (Landing OOM on the 2016 window), but the process only
  noticed at 09:17 because `producer_health` is consulted from `ColCheck.run`'s `on_idle`
  hook, which fires only when the Right B-queue is empty. The Right side spent ~2h40m computing
  stats for windows whose Left counterpart could never arrive. `Landing.land` itself (CSV ->
  file DuckDB, `INSERT BY NAME SELECT` over 898 varchar columns) is on the critical path of
  Left stats and took 28 min for 271k rows.

Four separate defects compound here.

**1. `compute_csv_statistics` builds one SELECT with ~15k aggregates, 1715 of them
`count(DISTINCT ...)`.** DuckDB executes each DISTINCT aggregate as its own hash table over
the full input. With 1715 of them in one query, memory is roughly 1715 x (distinct values x
row width) plus the per-aggregate state for the other 8 aggregates per column. On 70k rows
this exceeds 25 GiB. The 7402-row window survived only by swapping for 30 minutes.

**2. Top-10 does 1715 separate full CSV re-parses, each with a window function.** For every
categorical column, `row_number() OVER ()` materializes the whole CSV again to get a stable
tie-break position. That is 1715 parses of a 70k x 1715 CSV, each one building a
window-function buffer over every column (DuckDB cannot project the CSV scan down to one
column before the window).

**3. Every `duckdb.connect()` in the process is an independent database with its own default
`memory_limit` (80% of what DuckDB thinks RAM is).** On this 16 GB box DuckDB reported a
25 GiB ceiling (it counts committed/page-file memory on Windows), so even one connection is
allowed past physical RAM. The local run opens several at once:

- Left `FileSource.query_to_csv` (pull, `pull_workers=2`)
- Left `compute_csv_statistics` (`stat_workers=2`)
- Right `WarehouseTarget.window_statistics` -> `compute_csv_statistics` (Right ColCheck thread)
- `Landing.land` (its own file-backed DuckDB, serialized by a lock)

Five instances per run each believing they may use 25 GiB on a 16 GB machine; two runs at
once doubles that. Nothing sets
`memory_limit`, `threads`, or `preserve_insertion_order`. This is why the Left pull of 7402
rows took 47 minutes: it was not slow, it was starved while a Right stat query swapped.

**4. Retry re-pulls the window from Databricks every attempt.** `ColCheck.run_task` calls
`window_statistics`, which always calls `warehouse.to_csv` before computing. The 69996-row
`right.csv` was downloaded three times (3 min each) even though the pull succeeded every time
and only the stat step failed. The stat failure is deterministic (same CSV, same query, same
memory limit), so three attempts guarantee three OOMs.

The final crash is 3 killing a Left connection: when a sibling DuckDB instance OOMs the
process-wide allocator failure surfaces in whichever `fetchmany` is running, hence
`InvalidInputException: closed pending query result` on the Left side with
`Error: Out of Memory Error: Allocation failure` as the true cause.

## Behavior lock

- Statistics payloads stay byte-for-byte identical: same keys, same `hash_sum`, `n_unique`
  exact (no APPROX), same `top_10` ordering (count desc, first appearance asc), same
  numeric/categorical inference.
- Left and Right call the same `compute_csv_statistics`; whatever changes must change both.
- Protocol (manifest, unit keys, S3 layout) untouched.

## Changes

### A. Bound DuckDB memory process-wide (smallest change, do first)

Add a single helper, e.g. `dtrack_left/common/duck.py::connect(**kw)`, that every
`duckdb.connect()` in `sources.py`, `stats.py`, `pipeline.Landing` goes through, and have it
apply:

```
SET memory_limit = '<DTRACK_DUCKDB_MEMORY or (physical RAM * 0.6) / expected_connections>'
SET threads = <DTRACK_DUCKDB_THREADS or max(1, cpu // expected_connections)>
SET preserve_insertion_order = false
SET temp_directory = '<run>/_local/tmp'
```

Use `psutil`-free detection: `os.sysconf` on Linux, `ctypes.windll.kernel32.GlobalMemoryStatusEx`
on Windows, so the 25 GiB phantom goes away. `expected_connections` =
`pull_workers + stat_workers + 1 (Right) + 1 (Landing)`; expose
`DTRACK_DUCKDB_MEMORY` as a hard override. With a temp directory set, an over-budget
aggregate spills instead of throwing.

This alone turns "OOM, re-pull, OOM" into "slow but finishes" and stops the Left pull from
being starved. It does not fix the 30-minute stat time.

### B. Compute statistics in column batches, not one 15k-aggregate SELECT

In `compute_csv_statistics`:

1. Materialize the CSV once per call: `CREATE TEMP TABLE t AS SELECT <columns> FROM read_csv(...)`.
   One parse instead of 1 + 1 + 1715.
2. Run the inference query and the main aggregate query in batches of `DTRACK_STATS_BATCH`
   columns (default 64) over `t`. DuckDB then holds 64 DISTINCT hash tables at a time, not 1715.
   Concatenate the per-batch rows into the same `row` dict the current code reads; the result
   assembly loop below it does not change.
3. Replace the per-column top-10 query with one that does not need a window over the whole
   table: add `row_number() OVER () AS __pos` once when creating `t` (one window pass, one
   column), then per categorical column
   `SELECT value, count(*) n FROM (SELECT <value> value, __pos FROM t) WHERE value IS NOT NULL
   GROUP BY value ORDER BY n DESC, min(__pos) LIMIT 10`. Same ordering, no re-parse, projects to
   two columns.

Expected: 7402 x 1715 from ~30 min to well under a minute; 70k x 1715 finishes inside a
few GB.

### C. Do not re-pull on a stat retry

In `WarehouseTarget.window_statistics` (pipeline.py), skip `warehouse.to_csv` when
`output` already exists and has a non-zero size and a sidecar marker (`right.csv.ok`) written
after a complete pull is present; `to_csv` writes to `output.part` and renames, then writes
the marker. Retry then only recomputes stats. Same guard in `LandingLeftRuntime.pull_window`
is already provided by the Left pipeline's `.part` convention; verify rather than add.

Also: `ColCheck.run_task` treats a DuckDB `OutOfMemoryException` as deterministic and should
not burn `max_attempts` on it; either mark dead on first OOM with a clear message, or retry
once with `memory_limit` halved (requires A to be parameterizable per call).

### D. Serialize the two heavy stat consumers on small machines

The local runner starts Left stats (`stat_workers=2`) and Right stats in parallel. On 16 GB
with a 1715-column table that is three concurrent 1715-column aggregations. Add
`--stat-workers` / `DTRACK_STAT_WORKERS` to `dtrack_local run` (default 1 when the
manifest has > 500 columns, else 2) and a process-wide `threading.Semaphore` shared by Left
and Right `compute_csv_statistics` calls, sized by the same knob. B makes each call cheap
enough that serializing costs little wall-clock; D keeps the peak bounded regardless of
column count.

### D2. Fail fast when the Left producer dies

`_col_check` passes `producer_health` as `on_idle`; `ColCheck.run` should also call it
before dequeuing each B task (or `ColCheck` grows an `on_tick` hook). With `max_attempts`
and hour-long stat tasks, "only on idle" means a dead Left side is discovered hours late.
Also move `Landing.land` off the Left stat critical path: land after stats are published
(or in its own single worker), and under the memory helper from A.

### E. Observability

- Log per-window stat timing and the DuckDB `memory_limit` / `threads` in effect at the start
  of `col_check` (one line) so the next run shows the budget.
- `Warehouse.execute` still polls with no total deadline; legacy had
  `DTRACK_DATABRICKS_TIMEOUT`. Add `DTRACK_DBX_TIMEOUT` (seconds, 0 = none) to the poll loop and
  raise `TimeoutError` with the statement id so a hung warehouse does not hang the run.

### F. Window-level resume on the Left

`ColCheckPipeline.run` filters already-indexed units only at publish time; `plans` still
iterates every window, so a re-run re-pulls and recomputes stats for windows that are fully
published. Skip a window up front when every `unit_id(pair, bucket, column)` is in `existing`
(log one line per skipped window). Applies to both `dtrack col-check` and `dtrack_local`.

## Mode decision (2026-10-08)

Machine is 32 GB with ~16 GB free for dtrack. Going back to Left + Right mode:

- Left: `dtrack row-check --config c.json --run-s3 s3://.../runs --run-id R`, then
  `dtrack col-check --run-s3 ... --run-id R`. Already per-window: pull -> stats -> publish
  units -> next window; CSVs stay on disk, nothing accumulates in memory across windows.
- Right: `dtrack_right.job.run(run_s3, target_factory, run_id=R)` on Databricks (notebook or
  job) polls the same prefix, reconciles counts, drains units with Spark statistics.

Nothing new to build for the split itself. What still matters for Left on this machine is
A (`DTRACK_DUCKDB_MEMORY=16GB` as a process budget, divided across `pull_workers +
stat_workers` connections, `temp_directory` set so over-budget aggregates spill) and B
(batched aggregates, single CSV parse); F avoids redoing finished windows on a resume.
C, D, D2, E are `dtrack_local`-only and can wait.

## Status (2026-10-08, working tree, not committed)

A, B, F implemented; 767 unit tests pass.

- A: `dtrack_left/common/duck.py` — `connect(path, read_only, ordered)` applies
  `memory_limit` (= `DTRACK_DUCKDB_MEMORY` or 60% of physical RAM, divided by
  `DTRACK_DUCKDB_SHARE`, default 4), `threads` (`DTRACK_DUCKDB_THREADS` or cpu/share),
  `temp_directory` (`DTRACK_DUCKDB_TEMP`), and `preserve_insertion_order=false` when
  `ordered=False`. Used by `FileSource`, `DuckDBSource`, `Landing`, `compute_csv_statistics`.
  Physical RAM comes from `sysconf` or `GlobalMemoryStatusEx`, so the Windows 25 GiB phantom
  is gone. For the 32 GB laptop: `DTRACK_DUCKDB_MEMORY=16GB` -> 4 GiB per connection.
- B: `compute_csv_statistics` reads the header with `csv.reader` (no DESCRIBE sniff), loads
  the CSV once into a file-backed DuckDB under the temp directory (`stats-<uuid>.duckdb`,
  deleted afterwards), runs inference and aggregates in `DTRACK_STATS_BATCH` (64) column
  batches, and takes top-10 from the table using `rowid` as first-appearance order. In strict
  mode the numeric re-collapse is skipped (collapse is idempotent). Payload unchanged
  (`test_batched_aggregates_match_the_single_query_payload`, tie-order test).
- F: `ColCheckPipeline.run` skips a window up front when every unit is already in `_INDEX/`.
- C (partial): `WarehouseTarget(reuse=True)` / `LandingLeftRuntime(reuse=True)` keep an existing
  `right.csv` / `left.csv`; `open_run(reuse_pulled=True, land=False)` threads it, and
  `examples/nbs/05_reuse_pulled.ipynb` finishes an interrupted run from its pulled windows.
  `write_rows` and `FileSource.query_to_csv` now write `.part` and rename, so an existing CSV is
  complete. Still open: the `dtrack local run` CLI does not expose the flag, and a stat OOM still
  burns `max_attempts` (each attempt re-pulls unless reuse is on).

Findings while benchmarking (dev box, 4 threads, 4 GiB budget):

- 7402 x 1715 strict: 1772 s before -> 70.7 s (load 18 s, aggregates 43 s, top-10 9 s).
  Remaining cost is per-cell regex/sha256 in the existing expressions, not memory.
- 69996 x 1715 strict (821 MB CSV, the window that OOMed at 25 GiB after 33-50 min and
  crashed the run): 448 s end to end inside the 4 GiB budget; temp database removed after.
- The parallel CSV reader buffers every column per in-flight batch: 70k x 1715 OOMs at
  4 GiB with 4 threads even with `preserve_insertion_order=false`, but loads in 95 s with
  `threads=1` (339 MB on disk). The load therefore runs single-threaded, then threads go back
  up for the aggregates. An in-memory temp table + `row_number() OVER ()` also OOMed
  (window materializes all 1715 columns); the file-backed table pages out instead.

## Order

A -> B -> F for Left + Right mode; C -> D -> D2 -> E only if `dtrack_local` stays in use. A and C are small and make the next run finish; B is the real fix;
D and E are polish.

## Verification

- Unit: `tests/unit/dtrack_left/common/test_stats.py` must pass unchanged (payload parity);
  add a case with 300 columns x 1000 rows asserting batch output equals the single-query
  output on the old code path (keep the old path behind a flag for the test only, then delete).
- Unit: `test_warehouse.py` retry-without-repull case; `test_pipeline.py` memory knob plumbing.
- Live: re-run `just run local run --config ssot/CR_BUREAU_TRIGGER_FACT/cr_bureau.json
  --run-id CR_BUREAU_TRIGGER_2` and compare `right.csv` row counts and the stat payload for
  2026-07-27 against the first run's `_local/right/.../2026-07-27` (the only window that
  completed) to prove parity, then time 2026-08-03.

## Non-goals

- No change to the sampling predicate, key families, or the Spark path in `dtrack_right`.
- No APPROX_COUNT_DISTINCT (project rule: `n_unique` is exact).
- No switch to pandas for statistics; DuckDB stays the engine.
