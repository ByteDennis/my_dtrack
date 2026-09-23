# Workflow

Two codebases, one S3 (or Volume) work location, no direct link between the sides (ADR 0006).

| Side | Package | Runs on | Needs |
| --- | --- | --- | --- |
| Left | `src/dtrack_left`, CLI `dtrack` | on-prem PC | Left-side DB access, S3 write on one prefix, S3 read on the report store |
| Right | `src/dtrack_right` + `src/dtrack_right/notebook.py` | a Databricks job, started once | table or Volume read, work/report locations |

Sync `src/` to a Databricks Repo; the notebook puts its parent folder on `sys.path`, so
`import dtrack_right` works without a wheel. The Right side never imports `dtrack_left`
(`tests/test_dependencies.py`). The Left side has no Databricks package or credential.

Step-by-step notes with code excerpts live in `docs/notes/`; the plan-vs-implementation matrix and
improvement list are in `docs/plans/plan_coverage.md`.

## Commands

```bash
# on-prem
dtrack doctor       --config config.json
dtrack row-check    --config config.json --s3 s3://work/dtrack/v1/runs/<run_id> [--run-id <id>]
dtrack col-check    --s3 s3://work/dtrack/v1/runs/<run_id>              # waits for the Right side's manifests
dtrack fetch-report --report-s3 s3://report --out ./report [--run-id <id>]
dtrack debug [--attach [PORT]] -- <subcommand and its arguments>         # ipdb, or wait for VS Code
python -m dtrack <subcommand ...>      # same CLI without the Scripts/dtrack.exe launcher (Windows Defender blocks it on some PCs)

# Databricks: notebook widgets work / run_id / report / gate — or the local stand-in used by the e2e:
PYTHONPATH=src python -m dtrack_right --work s3://work/dtrack/v1/runs [--run-id <id>] --report s3://report --right-csv-dir DIR
```

`work` may be a run folder or the runs root; with the root and no `run_id` the Right job picks the
newest run whose `_STATUS.json` is not `complete`. The recommended Databricks trigger is a file
arrival trigger on the runs Volume path, so the job starts by itself when `row_check/*.left.json`
lands. Every command accepts `--log-level` and writes UTC-stamped lines to stderr; the final JSON
summary goes to stdout.

## Runtime sequence

1. Right job starts (by hand, schedule, or file arrival) and polls the runs location.
2. Left `row-check` counts every Left-side Partition and writes `row_check/<pair>.left.json`
   (counts plus the pair config stripped of credentials) and `row_check/_LEFT_CLOSED`.
3. Right job preflights and counts its side with `spark.sql`, reconciles per Partition, gates, and
   writes `row_check/<pair>.json`, `reports/<pair>_compare_row.csv`, `manifest/<pair>.json`, then
   `row_check/_CLOSED`.
4. Left `col-check` waits for `row_check/_CLOSED`, publishes `manifest/<pair>.keys.json` for fixed-N
   pairs, scans admitted Windows, computes statistics, writes each Unit payload and only then its
   Ready pointer, and closes with `manifest/_CLOSED`.
5. Right job prefetches its own side per Window (B), compares as Units arrive (A), writes
   `results/` and `dead/`, builds `summary.json` + CSV + HTML + XLSX, publishes
   `history/<run_id>/` then `latest/pointer.json`, and exits.
6. Left `fetch-report` downloads that set; without `--run-id` it follows the pointer.

## S3 object contract

```text
<work>/dtrack/v1/runs/<run_id>/
├── _STATUS.json                 whoever is working overwrites it (role: left | right)
├── _HEARTBEAT                   Left col-check liveness
├── row_check/<pair>.left.json   LEFT  counts + credential-free pair config   ← Right's first signal
├── row_check/_LEFT_CLOSED       LEFT
├── row_check/<pair>.json        RIGHT verdict per Partition
├── row_check/_CLOSED            RIGHT                                        ← Left col-check waits for it
├── reports/<pair>_compare_row.csv  RIGHT
├── manifest/<pair>.json         RIGHT admitted days, sampling rule, columns
├── manifest/<pair>.keys.json    LEFT  fixed-N only
├── manifest/_CLOSED             LEFT  {pairs, expected_units}
├── units/<unit-id>.json         LEFT  payload
├── _INDEX/<sequence>-<unit-id>.json  LEFT pointer, written after the payload
├── results/<unit-id>.json       RIGHT
└── dead/<unit-id>.json          RIGHT
<report>/
├── history/<run_id>/            summary.json · <pair>_compare_row.csv · <pair>_compare_col.csv · index.html · report.xlsx
└── latest/pointer.json          written last
```

The payload under `units/` must exist before its `_INDEX/` pointer. The Right side discovers Units
only through `_INDEX/` with `StartAfter`. A Unit ends in exactly one of `results/` or `dead/`.

## Connection profiles

Several Oracle servers and Databricks workspaces are described once under `connections` and
referenced per side with `conn_macro`; fields on the side override the profile. Credentials never
live in the file: `user_env` / `password_env` name the environment variables (or `.env` entries).
The Right side only receives `catalog/schema/table/date_col/date_type`.

## Key normalisation (ported from the legacy branch)

Both sides normalise a key the same way before hashing: strings `lower(trim(NFC))`, dates to
`YYYY-MM-DD`, numerics collapse trailing zeros only on pure decimals (`2.50 → 2.5`) while leading
zeros and exponents stay (`007`, `1e3`). Blank or NULL becomes `<<NULL>>`. Python rule in
`dtrack_left/normalization.py` (vendored into `dtrack_right`, parity-tested); Spark SQL twin in
`dtrack_right/targets.py::_norm_sql`. The bucket is `md5(...)` first 8 hex as a 32-bit integer.

## Dependencies

- Left: boto3, pandas, oracledb, openpyxl, python-dotenv. Extras: `.[athena]`, `.[hadoop]`.
  Dev: pytest, ruff, ipdb, debugpy, pyspark (local `SparkTarget` verification), testcontainers
  (Oracle container verification).
- Right: `src/dtrack_right/requirements.txt` (`openpyxl`, `boto3`); the notebook's first cell runs
  `%pip install -r ./requirements.txt` because a job cluster is a fresh environment.

## Local S3 simulator and end-to-end run

```bash
just s3-up          # Moto on 127.0.0.1:4566
just test-s3        # protocol integration test
just e2e            # examples/e2e/run_e2e.sh: both sides, synthetic data, timestamped record
```

## Tests

```bash
uv run pytest                    # tests/left + tests/right on real synthetic files (default)
just test-s3                     # tests/integration on Moto        (--run-integration)
just test-spark                  # tests/spark: SparkTarget vs pandas on local pyspark (--run-spark)
just test-containers             # tests/containers: OracleSource on gvenzl/oracle-free (--run-containers)
just test-e2e                    # tests/e2e: the examples script   (--run-e2e)
just test-live                   # tests/live: your real tables     (--run-live, DTRACK_LIVE_* env)
just test-debug <pytest args>    # ipdb post-mortem;  just test-attach <args> waits for VS Code on :5678
```

Numeric columns are hashed and counted on their canonical decimal text on both sides
(`collapse_decimal`), and `min/max` follow pandas formatting; this is verified against a local
Spark session and a real Oracle container.

## Acceptance boundary

Default tests prove configuration, domain rules, scheduling, retry terminal states, the protocol on a
real directory store and left/right parity on real files. `SparkTarget` is verified on a local
pyspark session and `OracleSource` on an Oracle container; production acceptance still needs a
Databricks cluster, representative tables and a real bucket policy (`tests/live`).
