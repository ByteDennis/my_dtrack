# Distributed reconciliation workflow

This repository contains only the distributed reconciliation package. It does not vendor the
original `dtrack` application or `ccsv`; those were references used to preserve comparison
semantics.

## Two codebases (ADR 0006)

| Side | Code | Runs on | Needs |
| --- | --- | --- | --- |
| Left | `src/dtrack_distributed` (`dtrack-distributed` CLI): `stage1`, `produce`, `fetch-report`, `doctor` | on-prem PC | Left-side DB access, S3 write on one prefix, S3 read on the report store |
| Right | `databricks/dtrack_cloud` + `databricks/consumer_notebook.py` | a Databricks job, started once | table or Volume read, work/report location |

The on-prem side holds no Databricks credential and installs no Databricks package. The cloud side
never imports the on-prem package. Sync the `databricks/` folder to a Repo; the notebook's folder is
on `sys.path`, so `import dtrack_cloud` works without a wheel.

`work` may be a run folder or the runs root; with the root and no `run_id` the job picks the newest
run whose `_STATUS.json` is not `complete`. On Databricks the recommended trigger is a file arrival
trigger on the runs Volume path, so the job starts by itself when `stage1/*.left.json` lands.

## Runtime sequence

Step-by-step notes with code excerpts live in `docs/notes/` (start at `00 索引.md`); the
plan-vs-implementation matrix and improvement list are in `docs/plans/plan_coverage.md`.

1. Right job starts (by hand or on a schedule) and polls the work location for `stage1/*.left.json`.
2. Left `stage1` counts every Left-side Partition and writes `stage1/<pair>.left.json` plus
   `stage1/_LEFT_CLOSED`. The document carries the pair config without credentials.
3. Right job preflights and counts its side with `spark.sql`, reconciles, gates, writes
   `stage1/<pair>.json`, `reports/<pair>_compare_row.csv`, `manifest/<pair>.json`, then `stage1/_CLOSED`.
4. Left `produce` waits for `stage1/_CLOSED`, publishes `manifest/<pair>.keys.json` for fixed-N
   pairs, scans admitted Windows, computes statistics, writes each Unit payload and only then its
   Ready pointer, and closes with `manifest/_CLOSED`.
5. Right job prefetches its own side per Window (B), compares as Units arrive (A), writes
   `results/` and `dead/`, builds `summary.json` + CSV + HTML + XLSX, publishes `history/<run_id>/`
   then `latest/pointer.json`, and exits.

## Dependencies

- Base install (on-prem): boto3, pandas, oracledb, python-dotenv. Nothing for reporting.
- Cloud side: `databricks/requirements.txt` (`openpyxl`, `boto3`); the notebook's first cell runs
  `%pip install -r ./requirements.txt` because a job cluster is a fresh environment.

## Key normalisation (ported from the legacy branch)

Both sides normalise a key the same way before hashing: strings `lower(trim(NFC))`, dates to
`YYYY-MM-DD`, numerics collapse trailing zeros **only on pure decimals** (`2.50 → 2.5`, `2.0 → 2`)
while leading zeros and exponents are kept (`007`, `1e3` stay). Blank or NULL becomes `<<NULL>>`.
The Python rule lives in `engine/normalization.py` (vendored into the cloud package, parity-tested)
and its Spark SQL twin in `dtrack_cloud/targets.py::_norm_sql`. The bucket is `md5(...)` first 8 hex
as a 32-bit integer, identical to the legacy `_hash32_expr`.
- `.[athena]`: Athena Left-side adapter.
- `.[hadoop]`: Hadoop/Impala Left-side adapter.
- SAS: deliberately deferred and absent.

Install optional adapters with `uv sync --extra athena` or `uv sync --extra hadoop`.

## Commands

```bash
# on-prem
dtrack-distributed doctor  --config config.json
dtrack-distributed stage1  --config config.json --s3 s3://bucket/run-prefix --run-id <id>
dtrack-distributed produce --s3 s3://bucket/run-prefix            # waits for the Right side
dtrack-distributed fetch-report --report-s3 s3://report-bucket --out ./report [--run-id <id>]   # download what the cloud published

# Databricks (notebook widgets: work, report, gate) — or the local stand-in for tests:
PYTHONPATH=databricks python -m dtrack_cloud --work s3://bucket/dtrack/v1/runs [--run-id <id>] --report s3://report-bucket --right-csv-dir DIR
```

The cloud job builds the report at the end of every run: `summary.json`, `<pair>_compare_row.csv`,
`<pair>_compare_col.csv`, a self-contained `index.html` (same look as the original dtrack `--html`
reports) and `report.xlsx` (`<pair>_row` and `<pair>_col` sheets, findings in red). It uploads the
immutable `history/<run_id>/` set first and `latest/pointer.json` last, so a reader of the pointer
never sees a half-written run. `fetch-report` downloads that set on-prem; without `--run-id` it
follows the pointer.

`python -m dtrack_cloud --right-csv-dir DIR` is the local stand-in for the notebook: the Right side
of each Pair is read from `DIR/<table>.csv` with the same pandas statistics as the Left side.
Production uses `SparkTarget`. Every command accepts `--log-level` and writes UTC-stamped progress
lines to stderr; the final JSON summary goes to stdout.

## Connection profiles

Several Oracle servers (different accounts, passwords, service names) and several Databricks
workspaces (different warehouses, catalogs, schemas) are described once under `connections` and
referenced per side with `conn_macro`. Fields written directly on the side override the profile.

```jsonc
{
  "connections": {
    "ora_pcds": {"source": "oracle", "dsn": "pcds-scan:1521/PCDS",
                 "user_env": "PCDS_USER", "password_env": "PCDS_PASSWORD"},
    "ora_edw":  {"source": "oracle", "dsn": "edw-scan:1521/EDWPRD",
                 "user_env": "EDW_USER", "password_env": "EDW_PASSWORD"},
    "dbx_main": {"source": "databricks", "server_hostname": "adb-main...net",
                 "http_path": "/sql/1.0/warehouses/main", "access_token_env": "DBX_MAIN_TOKEN"}
  },
  "pairs": {
    "orders": {
      "left":  {"conn_macro": "ora_pcds", "schema": "FINANCE", "table": "ORDERS", "date_col": "BUSINESS_DATE", "date_type": "date"},
      "right": {"conn_macro": "dbx_main", "catalog": "main", "schema": "sales", "table": "orders", "date_col": "business_date", "date_type": "date"}
    }
  }
}
```

Credentials never live in the config file: `user_env` / `password_env` / `access_token_env` name
the environment variables (or `.env` entries) to read.

## End-to-end example

`just e2e` (or `examples/e2e/run_e2e.sh`) generates synthetic data, runs Stage 1, starts the
Consumer before the Producer, drains, re-runs both for resume semantics, exercises the `manual`
and `ratio` gates, builds the report, and mirrors the work store and the report store to
`examples/e2e/output/`. The timestamped record is `examples/e2e/execution.txt`; per-command logs
are in `examples/e2e/logs/`.

## S3 object contract

```text
run-prefix/
├── _PREFLIGHT.json
├── _STATUS.json
├── _HEARTBEAT
├── stage1/<pair>.left.json      written by the Left side
├── stage1/_LEFT_CLOSED
├── stage1/<pair>.json           written by the Right side
├── stage1/_CLOSED
├── manifest/<pair>.json         written by the Right side
├── manifest/<pair>.keys.json    fixed-N only, written by the Left side
├── manifest/_CLOSED
├── units/<unit-id>.json
├── _INDEX/<sequence>-<unit-id>.json
├── results/<unit-id>.json
├── dead/<unit-id>.json
└── reports/<pair>_compare_row.csv
```

The payload under `units/` must exist before its `_INDEX/` pointer. Consumers use `StartAfter` to
advance monotonically. A Unit ends in exactly one of `results/` or `dead/`.

## Local S3 simulator

The simulator is useful for protocol integration tests but is not required for unit tests or
production:

```bash
docker compose -f docker/compose.s3.yaml up -d
DTRACK_S3_ENDPOINT_URL=http://127.0.0.1:4566 \
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 \
uv run pytest -m integration
```

## Acceptance boundary

Local tests prove configuration, domain rules, scheduling, retry terminal states, and the S3
protocol. Production acceptance still requires live Oracle/Databricks credentials, representative
tables, and a real bucket policy/KMS validation.
