# dtrack

Two-stage data reconciliation between on-prem sources and Databricks. The distributed packages
only exchange protocol payloads through S3; the local runner executes both sides against a local
directory with the same run layout:

| Package | Runs on | Commands / entry | Needs |
| --- | --- | --- | --- |
| `src/dtrack_left` | on-prem PC | `dtrack row-check`, `dtrack col-check`, `dtrack tools ...` | Left-side DB access, S3 write on one prefix, S3 read on the report store |
| `src/dtrack_right` | a Databricks job, started once | `src/dtrack_right/notebook.py` (`python -m dtrack_right` locally) | table or Volume read, work/report locations |
| `src/dtrack_local` | one operator machine | `python -m dtrack_local ...` | both source connections; S3 is optional for backup/report publishing |

- **row check** (stage 1): exact row counts per day on both sides; only days whose counts agree
  go on. Left writes `row_check/<pair>.left.json`; Right reconciles and writes the verdicts and the
  `manifest/<pair>.json` plan.
- **col check** (stage 2): deterministic sampling and per-column statistics on both sides,
  compared on the Right. Left publishes `units/` + `_INDEX/` pointers; Right prefetches its own
  side, compares, writes `results/`, builds and publishes the report.

The on-prem side holds no Databricks credential and installs no Databricks package. The Right
side never imports the Left package. Oracle is installed by default; Athena and Hadoop are extras.
`dtrack_local` deliberately combines both packages, but writes the same `row_check/`, `manifest/`,
`units/`, `_INDEX/`, `results/`, `dead/`, and report payloads through a directory-backed store.
That keeps sampling/hash behavior and rollback semantics common without making local runs depend
on S3 or making the distributed packages depend on the local runner.

```bash
uv sync
uv run pytest
uv run dtrack --help          # or: python -m dtrack --help (no console-script launcher)
uv run dtrack tools --help    # reports, diagnostics, sampling, rollback, Databricks, Confluence
just e2e            # both sides against a local Moto S3
```

`dtrack tools key-check` can override `--key`, `--family KEY=...`, and `--sample` to compare
the exact sampled key set selected by the Left source and Databricks. `dtrack tools blog` renders
a Confluence tracking-page preview from a report directory; add `--publish` for an explicit write.
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
src/dtrack_right/    notebook.py, app, row_check, col_check, scheduler, comparison, targets (SparkTarget, CsvAnchorTarget),
                     store (S3 / Volume), report, report_html, report_excel, gates, manifest, sampling, statistics, ...
tests/               left tests; tests/right for the right side and the vendored-module parity test
examples/e2e/        two-process end-to-end run with synthetic data
docker/              Moto S3 simulator
docs/                workflow, ADRs, plans, notes, slides
```
