# ADR 0005: The Right side is a Databricks table or an S3-backed Databricks Volume

## Status

Accepted (PoC scope: table only; Volume adapter pending)

## Context

The Right side is always Databricks (ADR 0002, `config.py`). Some datasets are not accessible as
Unity Catalog tables in time for a reconciliation run. Data owners can instead upload files to a
Databricks Volume backed by S3, and may also upload metadata or already-processed statistics there.

## Decision

`right.kind` selects one of three Right-side shapes:

| kind | What the Consumer reads | Stage 1 | Stage 2 |
| --- | --- | --- | --- |
| `table` (default) | a Unity Catalog table | `COUNT(*) GROUP BY date_col` | fetch + sample + stats |
| `volume_raw` | files under `/Volumes/<catalog>/<schema>/<volume>/<path>` via `read_files(...)` | same SQL over `read_files` | same |
| `volume_stats` | metadata / statistics the uploader already computed, in the Unit payload schema | read counts from metadata | read stats, skip the scan |

The PoC implements `table`. `volume_raw` is the next adapter: it is the same `SqlSource` with
`relation()` returning `read_files('/Volumes/...', format => ...)`. `volume_stats` requires the
uploader to produce the `units/` payload schema and is deferred until a real consumer of it exists.

## Consequences

The Right side stays Databricks-only, so the Consumer still ships one adapter family. Volume input
keeps the same Barrier and the same comparison semantics because it is read through the same SQL.
`volume_stats` shifts statistic computation to the uploader and therefore needs a schema version
in the manifest before it can be trusted.
