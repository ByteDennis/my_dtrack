# dtrack

Two-stage data reconciliation between on-prem sources and Databricks. The distributed packages
only exchange protocol payloads through S3; the local runner executes both sides against a local
directory with the same run layout:

| Package | Runs on | Commands / entry | Needs |
| --- | --- | --- | --- |
| `src/dtrack_left` | on-prem PC | `dtrack left-run`, `dtrack row-check`, `dtrack col-check`, `dtrack tools ...` | Left DB access; shared runs root |
| `src/dtrack_right` | a Databricks job, started once | `src/dtrack_right/notebook.py` | table/Volume read; same runs root |
| `src/dtrack_local` | one operator machine | `dtrack local ...` | both connections; runs root optional for backup |

- **row check** (stage 1): exact row counts per day on both sides; only days whose counts agree
  go on. Left and Right publish Pair-scoped inputs/verdicts; a passed Pair receives an immutable
  `pairs/<pair>/plan.json`.
- **col check** (stage 2): deterministic sampling and per-column statistics on both sides,
  compared on the Right. Left atomically publishes one statistics-bearing Unit object; Right
  prefetches its own side, compares, writes Pair-scoped results, and builds the report.

Top-level config `version: 1` keeps the Right-admitted flow above. With `version: 2`, `dtrack
left-run` publishes Left counts and statistics first. Right later persists row-count evidence and
fails before any statistics scan unless every mismatch is explicitly listed in
`excluded_partitions`. Rerunning Left with the same run id reuses unchanged Unit identities and
republishes only vintages or columns affected by exclusion changes.

When Oracle is reachable only from SAS, keep the same `left-run` workflow and split it into export
and import phases:

```bash
dtrack left-run --config config.json --run-id "$RUN_ID" --export-sas ./sas-run
# Copy the whole directory to the SAS host, edit dtrack_bundle / dtrack_connect_* in left-run.sas,
# and run left-run.sas there.
dtrack left-run --config config.json --run-s3 "$RUN_S3" --import-sas ./sas-run
```

After applying the configured source query/`where`, the generated script counts every returned
partition by default, then loops over every configured vintage and exports sampled data.
`fromDate`, `toDate`, and `excluded_partitions` are the explicit DTrack partition filters. The
import validates the bundle, computes the normal Left statistics, and uploads protocol documents
and Units—not the raw SAS CSV files. The existing Right job must still run afterward to compare row
counts and column statistics and to publish the final report.

The on-prem side holds no Databricks credential and installs no Databricks package. The Right
side never imports the Left package and contains no CSV/pandas fallback or source-platform sampling
SQL. Oracle is installed by default; Athena and Hadoop are extras.
`dtrack_local` deliberately combines both packages, but writes the same `_status/`,
`pairs/<pair>/`, and `report/` layout through a directory-backed store. `_status/units.csv` is the
human-readable progress view; protocol JSON remains authoritative.
That keeps sampling/hash behavior and rollback semantics common without making local runs depend
on S3 or making the distributed packages depend on the local runner.

`date_type` has four values only: `date`, `string`, `number`, and `sas`. For example,
`2025-04-02` uses `string`, while `01AUG2026:09:45:35` uses `sas`. Preflight probes a small
non-null sample for `string`/`sas`, then carries the detected internal format through the row and
manifest payloads. The standalone Right package contains only the Spark/Databricks SQL adapter;
source-platform adapters stay on the Left.

An Oracle `query` may be inline or a `.sql` path relative to the config file. Large consolidation
queries can push the selected DTrack window into each dated driver before the join with controlled
markers such as `{{query_windows:driver_one}}`; declare each marker under the Left
`query_windows` map. DTrack still applies the final-result date predicate and samples only after
the consolidated relation is built. If that query returns more than Oracle's portable 1000-column
limit, replace its final select list with `{{dtrack_projection}}` and map each configured Left
column to its original Oracle expression under `left.query_projection`. Preflight renders the
complete configured projection and lets Oracle reject an unsupported select list immediately;
col-check then pulls the full projection once per vintage. Statistics run parallel column shards
inside that vintage and publish each completed `DTRACK_STATS_BATCH` wave immediately. Queries
without this marker keep their existing behavior. Oracle always receives the complete rendered
SQL. Human audit files compact SELECT lists
larger than 20 attributes to the first two and last two attributes with an ellipsis and no visible
SQL hash; repeated read-query shapes reuse the same detailed audit file across command restarts.

Rerunning `row-check`, `col-check`, or `left-run` with the same `--run-id` resumes by default:
compatible row counts and existing Units are reused, and incomplete local pulls are reused only
when their provenance sidecar matches the run, plan, window, columns, and sampled keys. Keep the
same `--work-dir` to reuse those local CSVs. Add `--force` to clear the selected run stage and
recompute it; prior Units/results are removed first, so the rerun does not create duplicate active
Units. `left-run --force --import-sas` recomputes from the validated SAS output, while
`--force --export-sas` is rejected because export has no uploaded run state.

```bash
uv sync
uv run pytest
uv run dtrack --help          # or: python -m dtrack --help (no console-script launcher)
uv run dtrack tools --help    # reports, diagnostics, sampling, rollback, Databricks, Confluence
just e2e            # both sides against a local Moto S3
```

With the `dbx` extra installed, Workspace package transfer is symmetric and profile-aware:

```bash
dtrack tools dbx workspace upload src/dtrack_right /Users/me/dtrack/dtrack_right --overwrite
dtrack tools dbx workspace download /Users/me/dtrack/dtrack_right ./downloaded-right
```

Download recursively mirrors Workspace files and notebooks, restores source-notebook suffixes,
and supports `--dry-run` and overwrite protection.

`dtrack local key-check` can override `--key`, `--family KEY=...`, and `--sample` to compare
the exact sampled key set selected by the Left source and Databricks. `dtrack tools blog preview`
renders a local tracking-page preview; `blog publish`, `inspect`, and `delete` make the Confluence
lifecycle explicit. Editable storage-XHTML templates and JSON examples live under
`templates/confluence/`; publishing is view/edit restricted to the current user unless
`--space-visible` is given. `blog update` merges table, evidence, and status changes into the
page's versioned structured state before redrawing the exact page id.
Add `--debug ipdb` or `--debug attach` directly to `row-check`, `col-check`, or any
`dtrack local` subcommand. Attach mode waits for debugpy on port 5678.
Install that optional integration with `uv sync --extra confluence`. Server/Data Center PAT auth uses
`CONFLUENCE_URL` + `CONFLUENCE_TOKEN`; Cloud additionally needs `CONFLUENCE_USERNAME` and `--cloud`.
The integration is pinned to `atlassian-python-api==5.0.5` and uses Confluence REST API v1 by
default (`/rest/api` on Server/Data Center and `/wiki/rest/api` on Cloud).

See `docs/workflow.md` for commands and the S3 contract, `docs/notes/` for the walkthrough, and
`docs/plans/plan_coverage.md` for what is and is not implemented.

## Repository shape

```text
src/dtrack_left/     cli, config, sources (oracle/csv/athena/hadoop), s3, row_check, col_check, fetch, debug,
                     manifest, sampling, statistics, normalization, dates, sql
src/dtrack_right/    notebook, Spark-only target/sampling, row/col comparison, scheduler,
                     S3/Volume store, and report
tests/               backend, protocol, behavioral parity, local Spark, and live integration tests
examples/e2e/        two-process end-to-end run with synthetic data
docker/              Moto S3 simulator
docs/                workflow, ADRs, plans, notes, slides
```
