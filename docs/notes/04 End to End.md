---
tags: [dtrack, e2e, tests, moto]
updated: 2026-09-23
---

# 04 End to End

Index: [[00 Overview]] · Previous: [[03 Protocol and Report]]

```bash
just e2e            # docker compose up moto + ./examples/e2e/run_e2e.sh
```

Two processes, as in production: the on-prem CLI (`dtrack`, no Databricks credential) and the
Right job (`python -m dtrack_right`, the notebook's local stand-in with `CsvAnchorTarget` instead
of `spark.sql`). S3 is the `dtrack-moto-1` container. The record starts with
`databricks env vars present: 0`.

## Synthetic data (`examples/e2e/make_synthetic.py`)

```python
if iso == "2026-02-05": right_row[2] = f"{amount + 0.01:.2f}"    # AMOUNT diff in window 2026-02
if iso == "2026-01-03": right_row[3] = status.lower()             # STATUS diff in window 2026-01
if iso != "2026-01-31": left.append(left_row)                     # missing on the left  -> right_only
if iso != "2026-02-10": right.append(right_row)                   # missing on the right -> left_only
right = right without 3 rows of 2026-01-15                        # count_diff
# customers: right TIER upper-cased, pair has strict=true and sample=40 (fixed-N)
```

## Steps

```bash
RIGHT_CMD="env PYTHONPATH=$REPO/src uv run python -m dtrack_right"
$RIGHT_CMD --work "$S3" --report "$REPORT_S3" --right-csv-dir "$RIGHT" ... &     # 05: Right first, background
uv run dtrack row-check --config ... --s3 "$S3" --run-id "$RUN_ID"                # 06
uv run dtrack col-check --s3 "$S3" --wait-timeout 120                             # 07: waits for row_check/_CLOSED
wait $RIGHT_PID                                                                   # 05 ends: report published
uv run dtrack fetch-report --report-s3 "$REPORT_S3" --out "$OUT/fetched_report"   # 10
```

| Step | Side | Proves |
| --- | --- | --- |
| 01–02 | — | unit tests, right-side tests, parity test, Moto protocol test |
| 03–04 | — | data; both Moto buckets created and emptied, so the mirrors hold only this run |
| 05 | Right | starts first, waits, moves only when `row_check/*.left.json` appears |
| 06 | Left | `row-check` writes left counts and `_LEFT_CLOSED`, no credentials in the document |
| 05 | Right | reconciles, gates, writes manifests and `row_check/_CLOSED`, prefetches its own windows before any Unit exists |
| 07 | Left | `col-check` waits, writes `customers.keys.json`, pushes 11 Units round-robin, writes `manifest/_CLOSED` |
| 05 | Right | compares as Units land, builds and publishes the report, exits |
| 08–10 | both | rerun `col-check` → 0 Units; rerun the Right job with runs root + `--run-id` → 0 scans; `fetch-report` downloads the set |
| 11–12 | both | `--gate manual` → no manifest; `--gate ratio:0.02` → orders held (5.08 % excluded), customers released; these Right runs end by idle timeout on purpose |
| 13–14 | — | work and report buckets mirrored to `output/` |

## Timeline excerpt (`execution.txt`, run 20260923T114826Z-e2e)

```
11:48:26  RIGHT  right job start ... polling row_check/
11:48:29  LEFT   END 06_row_check_left rc=0
11:48:31  RIGHT  right count orders/customers, reconcile, manifest ×2, row_check closed
11:48:31  RIGHT  B fetch_right orders 2026-01-01 / 2026-02-01          ← Left has not pushed yet
11:48:33  LEFT   row_check/_CLOSED seen: 2 manifest(s) released; fixed sample keys customers -> keys.json
11:48:35  LEFT   manifest/_CLOSED written: expected_units=11
11:48:36  RIGHT  col_check complete: 11 result(s), 0 dead; published latest/pointer.json
```

## Outputs

| Path | Content |
| --- | --- |
| `examples/e2e/execution.txt` | timestamped record: every command, rc, elapsed, stdout |
| `examples/e2e/logs/NN_step.log` | stderr per step; `05_right_job.log` is the whole Right run |
| `examples/e2e/output/report/` | what the Right job built: `summary.json`, 4 CSVs, `index.html`, `report.xlsx` |
| `examples/e2e/output/fetched_report/` | the same set downloaded with `fetch-report` |
| `examples/e2e/output/s3_mirror/`, `report_store/` | byte copies of the two buckets |
| `examples/e2e/work/` | git-ignored scratch |

## Expected results

- row check: orders 56 match / 1 count_diff / 1 left_only / 1 right_only; customers 31 match.
- manifest: orders windows of 29 and 27 days, the three holes absent; `customers.keys.json` has 40 keys.
- col check: `orders/2026-01-01/STATUS` diff (n_unique 4→7), `orders/2026-02-01/AMOUNT` diff
  (mean, hash_sum), the other 9 units match; customers match under `strict`.
- work store: left=2 verdicts=2 manifests=2 fixed_keys=1 units=11 index=11 results=11 dead=0.

## Tests: one folder per side, real data everywhere it matters

| Folder | What runs | Data | Default? |
| --- | --- | --- | --- |
| `tests/left/` | config, row-check document, col-check pipeline, dependency boundary, **real CSV left pipeline** (`test_real_csv_left.py`: counts, holes, deterministic cross-side sampling, drift statistics, row-check → col-check on a real directory store) | synthetic CSVs from `make_synthetic.build()` | yes |
| `tests/right/` | reconcile, gates/manifest, scheduler, resume/fixed-N/dead-letter, VolumeStore, run resolution, report + fetch, parity, **whole protocol in-process on real files** (`test_real_pipeline.py`) | synthetic CSVs + `VolumeStore` in `tmp_path` | yes |
| `tests/integration/` | `S3Store` data-before-pointer and `StartAfter` on Moto | Moto | `--run-integration` |
| `tests/spark/` | **`SparkTarget` vs pandas** on a local pyspark session: counts, fraction sample predicate, fixed keys, every statistic field, strict and non-strict | synthetic CSV loaded with an explicit schema (`amount DECIMAL(10,2)`) | `--run-spark` (needs Java) |
| `tests/containers/` | **`OracleSource` on a real Oracle** (`gvenzl/oracle-free` via testcontainers): preflight, exact counts, window pull with holes, statistics equal to the CSV's | synthetic rows loaded into `ORDERS` | `--run-containers` (Docker) |
| `tests/e2e/` | `examples/e2e/run_e2e.sh` as a pytest, asserting the documented outcome | Moto | `--run-e2e` |
| `tests/live/` | placeholders for production credentials: Oracle preflight + counts, `SparkTarget` on a real table | your tables, via `DTRACK_LIVE_*` env vars | `--run-live` |

Opt-in groups are skipped by default with a reason; `just test-left`, `test-right`, `test-s3`,
`test-spark`, `test-containers`, `test-e2e`, `test-live` run each group. The synthetic generator
is shared with the e2e (`support.synthetic_data`), so every layer sees the same injected findings.

What the real-data layers caught on their first run: Spark formatted `min/max` of integer columns
as `100003.0` while pandas gives `100003`, and decimal text such as `7.50` (CSV, Databricks
`DECIMAL`) hashed differently from Oracle's `7.5`. Both are fixed by the rule below.

## Numeric canonicalisation rule

For a numeric column, both sides hash and count distinct values on their **canonical decimal text**
(`collapse_decimal`: trailing zeros dropped on pure decimals, leading zeros kept), and `min/max` are
formatted like pandas: integral columns as integers, others as doubles. pandas: `statistics.py`;
Spark: `targets.py` (`_collapse_decimal_sql`, `INTEGRAL_TYPES`). This is what makes Oracle
`NUMBER(10,2)`, CSV `7.50` and Spark `DECIMAL(10,2)` agree on `hash_sum`, `n_unique`, `min`, `max`.

## Debugging a test

```bash
just test-debug tests/right/test_real_pipeline.py -k full     # pytest -x -s --pdb with ipdb; drops into the failing frame
just test-attach tests/left -k sampling                        # python -m debugpy --listen 5678 --wait-for-client -m pytest ...
uv run pytest -x -s --pdb tests/left                           # plain pdb post-mortem
```

Inside a test, `breakpoint()` works with `-s`. For the attach mode use the VS Code entry
`{"type": "debugpy", "request": "attach", "connect": {"host": "localhost", "port": 5678}}`.
