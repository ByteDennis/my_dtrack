# ADR 0006: Two codebases; the on-prem side never holds Databricks credentials

## Status

Accepted

## Context

The first PoC ran Stage 1 preflight, Right-side counts and the optional job kickstart from the
on-prem machine through the Databricks SQL connector. That required `DATABRICKS_SERVER_HOSTNAME`,
`DATABRICKS_HTTP_PATH` and `DATABRICKS_TOKEN` on a PC that should only ever reach the Left side and
one S3 prefix. It also pulled whole Right-side windows back to the driver instead of computing in
the warehouse.

## Decision

- `src/dtrack_distributed` is the Left side only: `stage1` (left counts), `produce`, `report`,
  `doctor`. It depends on boto3, pandas, oracledb and openpyxl. No Databricks package, no
  Databricks environment variable.
- `databricks/dtrack_cloud` is the Right side: a self-contained package plus
  `databricks/consumer_notebook.py`. It is started once by hand or by a scheduled Workflow, watches
  the work location, counts the Right side with `spark.sql`, reconciles, writes the verdicts and
  manifests, drains the queue, builds and publishes the report, and exits. It never imports the
  Left-side package (`tests/test_dependencies.py`).
- The hand-off moves one step earlier: the Left side writes `stage1/<pair>.left.json` (counts plus
  the credential-free pair config) and `stage1/_LEFT_CLOSED`; the Right side writes
  `stage1/<pair>.json`, `manifest/<pair>.json` and `stage1/_CLOSED`. `produce` polls for
  `stage1/_CLOSED` before it starts.
- Fixed-N sampling keeps the Left side as the chooser: `produce` writes
  `manifest/<pair>.keys.json` and the Right side prefetches a window only after that file exists.
- The Right side can address the work location either as `s3://…` (boto3, instance profile) or as a
  Unity Catalog Volume path (`VolumeStore`, plain file IO, no AWS credentials on the cluster).
- Protocol and domain modules (dates, normalization, sql, gates, manifest, comparison, scheduler,
  reporting) are vendored into the cloud package. `tests/cloud/test_parity.py` asserts the shared
  functions are byte-identical so the two copies cannot drift unnoticed.

## Consequences

The on-prem machine needs only Left-side database access and S3 write on one prefix. The cloud job
needs only table (or Volume) read and the work/report locations. Statistics on the Right side are
computed inside the warehouse and only one row per window leaves it. The price is a vendored copy of
about 600 lines of pure-Python protocol code, guarded by the parity test, and a Stage 2 start that
waits for the Right side to close Stage 1 instead of proceeding per Pair.
