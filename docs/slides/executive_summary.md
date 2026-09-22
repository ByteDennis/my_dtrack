# Databricks Migration Quality Check

Framework 3.0 (PoC) · left side stays on-prem, everything else runs in Databricks

## LEFT — on-prem PC
count → sample → stats → push
Sources hang below in one pill, icons only: Oracle · SAS · Hive · Excel

→ arrow labelled **stats only**

## S3 (cylinder)
Stage 1 · row counts per day — exact count, one query per side → verdict
   ▇ mismatch → reported · match → filtered in, enter Stage 2 ▇
Stage 2 · column stats — deterministic, efficient, unbiased sampling

↔ arrow labelled **auto job**

## RIGHT — Databricks job, started once per run
Databricks icon · **pull · compute · compare** · table or Volume · reads S3 directly

## Punchlines (bottom left, yellow ball bullets, titles only; meaning in speaker notes)
1. **Auto queue**
2. **Stats travel, rows don't**
3. **One job start per run**
4. **Resume, don't restart**
5. **Excluded days stay excluded**

(bottom centre left empty for the demonstrated-output menu)

## What lands in S3 (bottom right, file tree)
s3://work/runs/<run_id>/
├─ stage1/<pair>.json       verdict per day
├─ manifest/<pair>.json     admitted days
├─ units/<unit>.json        left-side stats
├─ _INDEX/<n>-<unit>.json   ready pointers
├─ results/<unit>.json      comparisons
└─ dead/<unit>.json         failed units
s3://report/
├─ history/<run_id>/        summary.json · index.html · report.xlsx
└─ latest/pointer.json      newest complete run
