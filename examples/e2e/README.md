# End-to-end run on the local S3 simulator

Everything in this directory is produced by one command from the repository root:

```bash
just e2e            # starts docker/compose.s3.yaml (Moto on 127.0.0.1:4566) and runs the script
# or
./examples/e2e/run_e2e.sh
```

Two processes, exactly like production: the on-prem CLI (`dtrack-distributed`, no Databricks
credential) and the cloud job (`python -m dtrack_cloud`, the notebook's local stand-in with a CSV
anchor instead of `spark.sql`). The S3 protocol, Barrier, Producer, Consumer, gates, report and
resume semantics are the real code paths.

## Files

| Path | What it is |
| --- | --- |
| `config.json` | Two Pairs, plus `connections` profiles showing several Oracle servers and Databricks workspaces referenced via `conn_macro` |
| `make_synthetic.py` | Deterministic generator; injected findings are listed in `data/SYNTHETIC.json` |
| `data/left/*.csv`, `data/right/*.csv` | Left and Right populations (upper-case vs lower-case column names, mapped by `col_map`) |
| `run_e2e.sh` | Driver; every step is stamped with a UTC time, the command line, exit code and elapsed seconds. Step 05 is the cloud job running in the background for the whole run |
| `execution.txt` | The complete timestamped record of the last run |
| `logs/NN_<step>.log` | stderr of each step: UTC-stamped lines from `dtrack.stage1` / `dtrack.producer` (left) and `dtrack.cloud.*` (right); each step's stdout JSON is inlined in `execution.txt` |
| `output/report/` | The deliverables the cloud job built: `summary.json`, `<pair>_compare_row.csv`, `<pair>_compare_col.csv`, `index.html`, `report.xlsx` |
| `output/fetched_report/` | The same set downloaded on-prem with `fetch-report` (follows `latest/pointer.json`) |
| `output/s3_mirror/` | Byte copy of the work store `s3://dtrack-e2e-work/dtrack/v1/runs/<run_id>/` |
| `output/report_store/` | Byte copy of the report store `s3://dtrack-e2e-report/`: `history/<run_id>/…` plus `latest/pointer.json` (older runs stay, the bucket is an immutable archive) |
| `work/` | Local scratch (git-ignored): window pulls, sampled CSVs, fixed-N key lists, checkpoint, gate-policy mirrors |

## Synthetic findings and what the run must report

| Pair | Injected | Expected | Where it shows |
| --- | --- | --- | --- |
| orders | 2026-01-15 right side drops 3 rows | `count_diff` | `stage1/orders.json`, `reports/orders_compare_row.csv` |
| orders | 2026-01-31 missing on the left | `right_only` | same |
| orders | 2026-02-10 missing on the right | `left_only` | same |
| orders | window 2026-01-01 excludes 01-15 and 01-31, window 2026-02-01 excludes 02-10 | holes preserved | `manifest/orders.json` `windows[].partitions` |
| orders | 2026-01-03 right STATUS lower-cased | `STATUS` diff in window 2026-01-01 | `output/report/orders_compare_col.csv` |
| orders | 2026-02-05 right AMOUNT +0.01 | `AMOUNT` diff in window 2026-02-01 | `output/report/orders_compare_col.csv` |
| customers | right TIER upper-cased, `strict: true` | all three columns `match` | `output/report/customers_compare_col.csv` |
| customers | `sample: 40` (fixed-N) | Stage 1 materialises 40 `key_tuples` in the manifest before Stage 2 | `manifest/customers.json` |

## Sequence the script proves

1. Unit tests, cloud tests, the parity test and the Moto protocol test pass.
2. The cloud job starts first and polls the empty work prefix.
3. Left `stage1` counts the Left side and writes `stage1/<pair>.left.json` + `stage1/_LEFT_CLOSED`, with the pair config stripped of credentials.
4. The cloud job picks the left documents up, counts its side, reconciles, gates, writes `stage1/<pair>.json`, `manifest/<pair>.json`, `stage1/_CLOSED`, and immediately prefetches its own windows (B-queue).
5. Left `produce` waits for `stage1/_CLOSED`, writes `manifest/customers.keys.json` (fixed-N), pulls windows round-robin, commits `units/` before `_INDEX/`, and writes `manifest/_CLOSED`.
6. The cloud job compares Units as they land (A-queue), writes `results/`, builds the report, publishes `history/<run_id>/` then `latest/pointer.json`, and exits.
7. Re-running `produce` publishes 0 Units; re-running the cloud job returns 0 new results; `fetch-report` downloads the published set on-prem.
8. `--gate manual` on the cloud side writes Stage 1 reports and no manifest; `--gate ratio:0.02` blocks `orders` (5.08 % excluded) and releases `customers`. Those two cloud runs end by idle timeout because no producer feeds them, which is the expected exit.
