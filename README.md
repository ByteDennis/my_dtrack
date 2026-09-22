# dtrack-distributed

Two-stage distributed reconciliation, split into two codebases that only meet in S3 (ADR 0006):

- `src/dtrack_distributed` runs on-prem: counts the Left side, waits for the Right side's verdicts,
  publishes left-side statistics with data-before-pointer commits, downloads the finished report.
  No Databricks credential, about 1.7k lines.
- `databricks/dtrack_cloud` runs as a Databricks job started once: counts the Right side with
  `spark.sql`, reconciles per Partition, prefetches its own statistics, compares Units, publishes
  the report, exits.

Oracle is installed by default on-prem; Athena and Hadoop are optional extras. SAS is out of scope.

```bash
uv sync
uv run pytest
uv run dtrack-distributed --help
```

Run the local S3 protocol simulator and its integration test:

```bash
just test-s3
```

See [docs/workflow.md](docs/workflow.md) for commands, object layout, invariants, and live-environment acceptance gates.

## Repository shape

```text
.
├── src/dtrack_distributed/   # LEFT side (on-prem)
│   ├── domain/       # Barrier, Manifest, Unit
│   ├── engine/       # exact local statistics, sampling, SQL generation
│   ├── adapters/     # Oracle/CSV/S3 plus optional Athena/Hadoop
│   └── workflows/    # stage1 (left counts), producer, fetch (report download)
├── databricks/               # RIGHT side (Databricks)
│   ├── consumer_notebook.py  # start once; watches, reconciles, drains, reports, exits
│   └── dtrack_cloud/         # self-contained: SparkTarget, stage1_right, consumer, reporting, S3/Volume store
├── tests/            # left-side tests, tests/cloud for the right side + parity
├── docker/           # optional Moto S3 simulator
├── docs/             # workflow, ADRs, source plans, plan coverage, Obsidian notes (docs/notes)
├── .agents/skills/   # grilling and domain-modeling project skills
├── CONTEXT.md
└── pyproject.toml
```
