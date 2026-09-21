# dtrack-distributed

Self-contained implementation of the two-stage distributed reconciliation workflow.

1. Stage 1 runs Databricks preflight and exact per-Partition counts.
2. A per-Pair Barrier admits only count-matching Partitions.
3. The Producer publishes left-side statistics to S3 using data-before-pointer commits.
4. The Consumer prefetches right-side statistics, compares Units, checkpoints outcomes, and emits CSV reports.

Oracle and Databricks support are installed by default. Athena and Hadoop are optional extras. SAS is intentionally out of scope.

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
├── src/dtrack_distributed/
│   ├── domain/       # Barrier, Manifest, Unit, comparison rules
│   ├── engine/       # exact local statistics, sampling, SQL generation
│   ├── ports/        # dependency boundaries
│   ├── adapters/     # Oracle/Databricks/S3 plus optional Athena/Hadoop
│   └── workflows/    # Stage 1, Producer, Consumer, scheduler
├── tests/            # only distributed-workflow tests
├── docker/           # optional Moto S3 simulator
├── docs/             # workflow, ADRs, and source plans
├── .agents/skills/   # grilling and domain-modeling project skills
├── CONTEXT.md
└── pyproject.toml
```
