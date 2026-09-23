# dtrack

Two-stage data reconciliation between on-prem sources and Databricks, split into two codebases
that only meet in S3 (ADR 0006):

| Package | Runs on | Commands / entry | Needs |
| --- | --- | --- | --- |
| `src/dtrack_left` | on-prem PC | `dtrack row-check`, `dtrack col-check`, `dtrack fetch-report`, `dtrack doctor`, `dtrack debug` | Left-side DB access, S3 write on one prefix, S3 read on the report store |
| `src/dtrack_right` | a Databricks job, started once | `src/dtrack_right/notebook.py` (`python -m dtrack_right` locally) | table or Volume read, work/report locations |

- **row check** (stage 1): exact row counts per day on both sides; only days whose counts agree
  go on. Left writes `row_check/<pair>.left.json`; Right reconciles and writes the verdicts and the
  `manifest/<pair>.json` plan.
- **col check** (stage 2): deterministic sampling and per-column statistics on both sides,
  compared on the Right. Left publishes `units/` + `_INDEX/` pointers; Right prefetches its own
  side, compares, writes `results/`, builds and publishes the report.

The on-prem side holds no Databricks credential and installs no Databricks package. The Right
side never imports the Left package. Oracle is installed by default; Athena and Hadoop are extras.

```bash
uv sync
uv run pytest
uv run dtrack --help          # or: python -m dtrack --help (no console-script launcher)
just e2e            # both sides against a local Moto S3
```

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
