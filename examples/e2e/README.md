# End-to-end run on the local S3 simulator

Everything in this directory is produced by one command from the repository root:

```bash
just e2e            # starts docker/compose.s3.yaml (Moto on 127.0.0.1:4566) and runs the script
# or
./examples/e2e/run_e2e.sh
```

Two processes, exactly like production: the on-prem CLI (`dtrack`, no Databricks credential) and
the Right job (`python -m dtrack_local.support.right_job`, a local CSV adapter driving the same
`dtrack_right.engine.job` orchestration instead of `spark.sql`). The S3 protocol, Barrier, Producer, Consumer, gates, report and
resume semantics are the real code paths.

## Files

| Path | What it is |
| --- | --- |
| `config.json` | Two Pairs, plus `connections` profiles showing several Oracle servers and Databricks workspaces referenced via `conn_macro` |
| `make_synthetic.py` | Deterministic generator; injected findings are listed in `data/SYNTHETIC.json` |
| `data/left/*.csv`, `data/right/*.csv` | Left and Right populations (upper-case vs lower-case column names, mapped by `col_map`) |
| `run_e2e.sh` | Driver; every step is stamped with a UTC time, the command line, exit code and elapsed seconds. Step 05 is the Right job running in the background for the whole run |
| `execution.txt` | The complete timestamped record of the last run (with `logs/` and `output/`: written by `just e2e`, git-ignored) |
| `logs/NN_<step>.log` | stderr of each step: UTC-stamped lines from `dtrack.left.*` and `dtrack.right.*`; each step's stdout JSON is inlined in `execution.txt` |
| `output/report/` | The deliverables the Right job built: `summary.json`, `pairs/<pair>/row_check.csv`, `pairs/<pair>/col_check.csv`, `index.html`, `report.xlsx` |
| `output/s3_mirror/` | Byte copy of the work store `s3://dtrack-e2e-work/dtrack/v1/runs/<run_id>/` |
| `work/` | Local scratch (git-ignored): window pulls, sampled CSVs, fixed-N key lists, checkpoint, gate-policy mirrors |

## Synthetic findings and what the run must report

| Pair | Injected | Expected | Where it shows |
| --- | --- | --- | --- |
| orders | 2026-01-15 right side drops 3 rows | `count_diff` | `pairs/orders/row_check/result.json`, `report/pairs/orders/row_check.csv` |
| orders | 2026-01-31 missing on the left | `right_only` | same |
| orders | 2026-02-10 missing on the right | `left_only` | same |
| orders | window 2026-01-01 excludes 01-15 and 01-31, window 2026-02-01 excludes 02-10 | holes preserved | `pairs/orders/plan.json` `windows[].partitions` |
| orders | 2026-01-03 right STATUS lower-cased | `STATUS` diff in window 2026-01-01 | `output/report/pairs/orders/col_check.csv` |
| orders | 2026-02-05 right AMOUNT +0.01 | `AMOUNT` diff in window 2026-02-01 | `output/report/pairs/orders/col_check.csv` |
| customers | right TIER upper-cased, `strict: true` | all three columns `match` | `output/report/pairs/customers/col_check.csv` |
| customers | `sample: 40` (fixed-N) | Left `col-check` writes 40 keys to `pairs/customers/fixed_keys.json` | `pairs/customers/fixed_keys.json` |

## Sequence the script proves

1. Unit tests, right-side tests, the parity test and the Moto protocol test pass.
2. The Right job starts first and polls the empty work prefix.
3. Left `row-check` counts the Left side and writes `pairs/<pair>/row_check/left.json` + `_status/left_row_check.closed.json`, with the pair config stripped of credentials.
4. The Right job picks the left documents up, counts its side, reconciles, gates, writes `pairs/<pair>/row_check/result.json`, `pairs/<pair>/plan.json`, `_status/row_check.closed.json`, and immediately prefetches its own windows (B-queue).
5. Left `col-check` waits for `_status/row_check.closed.json`, writes `pairs/customers/fixed_keys.json` (fixed-N), and atomically publishes each Unit as one JSON document under `pairs/<pair>/units/`. It then writes `_status/col_check.closed.json`.
6. The Right job compares Units as they land (A-queue), writes Pair-scoped `col_check/results/`, continuously refreshes `_status/units.csv`, builds the report under the same run's `report/` prefix, and exits.
7. Re-running `col-check` publishes 0 Units; re-running the Right job (runs root + `--run-id`) returns 0 new results; the published report remains under that run's `report/` prefix.
8. `--gate manual` on the Right writes row-check verdicts and no manifest; `--gate ratio:0.02` blocks `orders` (5.08 % excluded) and releases `customers`. Those two Right runs end by idle timeout because nobody runs `col-check` for them, which is the expected exit.
