# dtrack

Two-stage data reconciliation between on-prem sources and Databricks. The distributed packages
only exchange protocol payloads through S3; the local runner executes both sides against a local
directory with the same run layout:

| Package | Runs on | Commands / entry | Needs |
| --- | --- | --- | --- |
| `src/dtrack_left` | on-prem PC | `dtrack row-check`, `dtrack col-check`, `dtrack tools ...` | Left DB access; shared runs root |
| `src/dtrack_right` | a Databricks job, started once | `src/dtrack_right/notebook.py` | table/Volume read; same runs root |
| `src/dtrack_local` | one operator machine | `dtrack local ...` | both connections; runs root optional for backup |

- **row check** (stage 1): exact row counts per day on both sides; only days whose counts agree
  go on. Left writes `row_check/<pair>.left.json`; Right reconciles and writes the verdicts and the
  `manifest/<pair>.json` plan.
- **col check** (stage 2): deterministic sampling and per-column statistics on both sides,
  compared on the Right. Left publishes `units/` + `_INDEX/` pointers; Right prefetches its own
  side, compares, writes `results/`, builds and publishes the report.

The on-prem side holds no Databricks credential and installs no Databricks package. The Right
side never imports the Left package and contains no CSV/pandas fallback or source-platform sampling
SQL. Oracle is installed by default; Athena and Hadoop are extras.
`dtrack_local` deliberately combines both packages, but writes the same `row_check/`, `manifest/`,
`units/`, `_INDEX/`, `results/`, `dead/`, representative SQL `audit.txt`, and report payloads
through a directory-backed store.
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
the consolidated relation is built.

```bash
uv sync
uv run pytest
uv run dtrack --help          # or: python -m dtrack --help (no console-script launcher)
uv run dtrack tools --help    # reports, diagnostics, sampling, rollback, Databricks, Confluence
just e2e            # both sides against a local Moto S3
```

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
