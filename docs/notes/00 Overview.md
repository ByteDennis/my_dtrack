---
tags: [dtrack, overview, moc]
updated: 2026-09-23
---

# 00 Overview

Five notes, in the order a run happens. Design rationale is in [[s3_split_pipeline]] and ADR 0006;
the plan-vs-code matrix and ranked improvements are in [[plan_coverage]]; the real run record is
`examples/e2e/execution.txt`.

| # | Note | Side | Main code |
| --- | --- | --- | --- |
| 0 | this one: roles, run identity, S3 tree, triggers, workflow inputs | both | `dtrack_right/store.py::resolve_run` |
| 1 | [[01 Left]] | on-prem | `dtrack_left/{config,row_check,col_check,sampling,statistics,fetch,debug}.py` |
| 2 | [[02 Right]] | Databricks | `dtrack_right/{app,row_check,col_check,scheduler,targets,comparison}.py` |
| 3 | [[03 Protocol and Report]] | both | `dtrack_left/s3.py`, `dtrack_right/{store,report*}.py` |
| 4 | [[04 End to End]] | both | `examples/e2e/run_e2e.sh`, `tests/` |

## Two packages, one meeting point

| Side | Package | Runs on | Needs |
| --- | --- | --- | --- |
| Left | `src/dtrack_left`, CLI `dtrack` (`row-check`, `col-check`, `fetch-report`, `doctor`, `debug`) | on-prem PC | Left DB access, S3 write on one prefix, S3 read on the report store |
| Right | `src/dtrack_right` + `notebook.py`, started once | Databricks job | table or Volume read, work/report locations |

The Left side holds no Databricks credential or package; the Right side never imports the Left
package (`tests/test_dependencies.py`). Shared protocol modules are vendored into the Right package
and kept byte-identical by `tests/right/test_parity.py`.

```
RIGHT job starts, polls ──► LEFT row-check ──► RIGHT reconciles ──► LEFT col-check ──► RIGHT compares + report ──► LEFT fetch-report
```

## Vocabulary

| Term | Meaning |
| --- | --- |
| Pair | one Left table matched with one Right table |
| Partition | canonical date value; the row-check unit of agreement |
| Window | admitted Partitions grouped by `vintage`; the col-check scan unit |
| Unit | `(pair, window, column)`; one JSON file, the col-check delivery unit |
| row check | exact per-day counts on both sides; mismatching days are reported and dropped |
| col check | deterministic sampling + per-column statistics on both sides, compared on the Right |
| Finding | a data difference; goes into the report, never an alert |
| Failure | connectivity, timeout, code error; goes into `dead/` or a non-zero exit |

## Run identity and the S3 tree

```
s3://dtrack-work/            ← Volume /Volumes/main/dtrack/work
  dtrack/v1/runs/                                   ← the "runs root": the job's `work` parameter
    20260923T113744Z-a3f9c1/                         ← run_id = UTC stamp + sha256(config)[:6]
      _STATUS.json  _HEARTBEAT
      row_check/  manifest/  units/  _INDEX/  results/  dead/  reports/
    20260923T150000Z-a3f9c1/                         ← same config checked again: new stamp, new folder
    20260923T151000Z-7b21de/                         ← another config
s3://dtrack-report/          ← Volume /Volumes/main/dtrack/report
  history/<run_id>/  summary.json index.html report.xlsx *_compare_*.csv
  latest/pointer.json        ← always the newest completed run
```

- `run_id` is minted by Left `row-check` (`make_run_id`) or given with `--run-id`. Checking the
  same pair ten times means ten folders; the six-hash suffix tells "same config again" from
  "different config".
- Pairs are file names inside a run (`row_check/orders.left.json`, `manifest/orders.json`,
  `units/orders--2026-02-01--amount--<digest>.json`); many pairs share one run without collision.
- Reports are archived per run under `history/`; `latest/pointer.json` is overwritten only after
  a full set is published. `dtrack fetch-report` follows the pointer unless `--run-id` is given.

## Signals the Right side waits for

| Phase | Signal | Written by | Detection |
| --- | --- | --- | --- |
| find run | `runs/<run_id>/row_check/` exists | Left `row-check` | `resolve_run`: explicit `run_id`, or newest run whose `_STATUS.json` is not `complete` |
| row check | `row_check/<pair>.left.json` | Left `row-check` | every `poll_interval`: any pair without `row_check/<pair>.json` is counted and reconciled (idempotent) |
| row check closed | `row_check/_LEFT_CLOSED` | Left `row-check` | once all listed pairs have verdicts, Right writes `row_check/_CLOSED` |
| col check B | `manifest/<pair>.json` | Right itself | prefetch its own side immediately |
| col check fixed-N | `manifest/<pair>.keys.json` | Left `col-check` | fixed-mode pairs wait for it before prefetching |
| col check A | `_INDEX/<seq>-<unit>.json` | Left `col-check` | incremental listing with `StartAfter` |
| done | `manifest/_CLOSED` and `results + dead == expected_units` | Left writes `_CLOSED` | report, publish, exit |

## Workflow inputs (Databricks job parameters → notebook widgets)

| Parameter | Value |
| --- | --- |
| `work` | `/Volumes/main/dtrack/work/dtrack/v1/runs` (runs root) or one run folder; `s3://…` also works with an instance profile |
| `run_id` | empty (auto-pick) or a specific run |
| `report` | `/Volumes/main/dtrack/report` |
| `gate` | `auto` / `manual` / `ratio:0.02` |
| `idle_timeout`, `wait_timeout` | seconds |

Three ways to start, same inputs: **Run now** by hand; a **schedule** with a short `wait_timeout`;
or a **file arrival trigger** on the runs Volume path, which starts the job within about a minute of
`row_check/*.left.json` landing. Keep `max_concurrent_runs = 1`. Different runs may run in parallel
with explicit `run_id`s; a killed job restarted on the same `work` resumes from `results/ ∪ dead/`.
