# Distributed reconciliation workflow

This repository contains only the distributed reconciliation package. It does not vendor the
original `dtrack` application or `ccsv`; those were references used to preserve comparison
semantics.

## Runtime sequence

1. `stage1` preflights every Databricks Right side.
2. It completes every exact Right-side Partition count before starting Left-side counts.
3. Each Pair receives its own Barrier; only exact count-matching Partitions enter its Manifest.
4. `produce` scans admitted Left-side Windows, calculates exact local statistics, writes each Unit
   payload, and only then writes the corresponding Ready pointer.
5. `consume` gives ready comparisons priority over speculative Right-side prefetch, checkpoints
   each result, and writes a comparison CSV.

## Dependencies

- Base install: Oracle, Databricks, S3 protocol, local statistics.
- `.[athena]`: Athena Left-side adapter.
- `.[hadoop]`: Hadoop/Impala Left-side adapter.
- SAS: deliberately deferred and absent.

Install optional adapters with `uv sync --extra athena` or `uv sync --extra hadoop`.

## Commands

```bash
dtrack-distributed doctor --config config.json
dtrack-distributed stage1 --config config.json --s3 s3://bucket/run-prefix
dtrack-distributed produce --s3 s3://bucket/run-prefix
dtrack-distributed consume --s3 s3://bucket/run-prefix --output compare_col.csv
```

`consume --right-csv-dir DIR` is a local correctness anchor for tests and walkthroughs. Production
uses Databricks.

## S3 object contract

```text
run-prefix/
├── _PREFLIGHT.json
├── _STATUS.json
├── _HEARTBEAT
├── stage1/<pair>.json
├── stage1/_CLOSED
├── manifest/<pair>.json
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
