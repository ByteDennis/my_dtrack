#!/usr/bin/env bash
set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
E2E="$REPO/examples/e2e"
LOGS="$E2E/logs"
OUT="$E2E/output"
WORK="$E2E/work"
EXEC="$E2E/execution.txt"
cd "$REPO"

export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
export DTRACK_S3_ENDPOINT_URL="${DTRACK_S3_ENDPOINT_URL:-http://127.0.0.1:4566}"
WORK_BUCKET="dtrack-e2e-work"
REPORT_BUCKET="dtrack-e2e-report"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-e2e"
S3="s3://$WORK_BUCKET/dtrack/v1/runs/$RUN_ID"
REPORT_S3="s3://$REPORT_BUCKET"
RIGHT="examples/e2e/data/right"
RIGHT_CMD="env PYTHONPATH=$REPO/src uv run python -m dtrack_right"

rm -rf "$LOGS" "$OUT" "$WORK" "$EXEC"
mkdir -p "$LOGS" "$OUT" "$WORK"

ts() { date -u +%Y-%m-%dT%H:%M:%S.%3NZ; }
log() { printf '%s  %s\n' "$(ts)" "$*" | tee -a "$EXEC"; }
section() { printf '\n%s\n=== %s ===\n' "$(ts)" "$*" | tee -a "$EXEC"; }
run() {
  local name="$1"; shift
  local started rc
  started="$(date +%s.%N)"
  log "START $name"
  log "  cmd: $*"
  "$@" 2>"$LOGS/$name.log" | sed 's/^/  > /' | tee -a "$EXEC"
  rc=${PIPESTATUS[0]}
  log "END   $name rc=$rc elapsed=$(printf '%.3f' "$(echo "$(date +%s.%N) - $started" | bc)")s log=$LOGS/$name.log"
  return "$rc"
}
show() { log "file: $1"; sed 's/^/  | /' "$1" | tee -a "$EXEC"; }
gate_of() { uv run python -c "import json,sys; d=json.load(open(sys.argv[1])); g=d['gate_decision']; print(d['pair'], 'passed=%s excluded_ratio=%.4f' % (g['passed'], g['excluded_ratio']))" "$1"; }

section "0. environment"
log "repo=$REPO git=$(git rev-parse --short HEAD) branch=$(git rev-parse --abbrev-ref HEAD)"
log "python=$(uv run python -c 'import sys;print(sys.version.split()[0])') dtrack=$(uv run python -c 'import importlib.metadata as m;print(m.version("dtrack"))')"
log "moto endpoint=$DTRACK_S3_ENDPOINT_URL container=$(docker ps --filter name=dtrack-moto --format '{{.Names}} {{.Image}} {{.Status}}')"
log "run_id=$RUN_ID"
log "work store=$S3   report store=$REPORT_S3"
log "LEFT process: uv run dtrack (src/dtrack_left, no Databricks credentials)"
log "RIGHT process: python -m dtrack_right (src/dtrack_right, csv anchor standing in for spark.sql)"
log "databricks env vars present: $(env | grep -c '^DATABRICKS_' || true) (expected 0)"

section "1. unit + integration tests"
run "01_pytest_unit" uv run pytest -q tests/unit
run "02_pytest_s3_protocol" uv run pytest -q --run-moto tests/local/s3

section "2. synthetic data"
run "03_make_synthetic" uv run python examples/e2e/make_synthetic.py
for f in data/left/ORDERS.csv data/left/CUSTOMERS.csv data/right/orders.csv data/right/customers.csv; do
  log "data: $E2E/$f rows=$(($(wc -l <"$E2E/$f") - 1)) header=$(head -1 "$E2E/$f")"
done

section "3. buckets (created if missing, then emptied so this run's artifacts stand alone)"
run "04_reset_buckets" uv run python -c "
import boto3, os
from botocore.config import Config
c = boto3.client('s3', endpoint_url=os.environ['DTRACK_S3_ENDPOINT_URL'], config=Config(s3={'addressing_style': 'path'}))
for b in ('$WORK_BUCKET', '$REPORT_BUCKET'):
    c.create_bucket(Bucket=b)
    removed = 0
    for page in c.get_paginator('list_objects_v2').paginate(Bucket=b):
        keys = [{'Key': row['Key']} for row in page.get('Contents') or []]
        if keys:
            c.delete_objects(Bucket=b, Delete={'Objects': keys, 'Quiet': True}); removed += len(keys)
    print(f'bucket ready: s3://{b} (removed {removed} stale object(s))')
"

section "4. RIGHT job starts first and waits (started once, by hand)"
RIGHT_START="$(date +%s.%N)"
log "START 05_right_job (background)"
log "  cmd: $RIGHT_CMD --work $S3 --report $REPORT_S3 --right-csv-dir $RIGHT --out $OUT/report --work-dir $WORK/right --poll-interval 1 --idle-timeout 120"
$RIGHT_CMD --work "$S3" --report "$REPORT_S3" --right-csv-dir "$RIGHT" --out "$OUT/report" \
  --work-dir "$WORK/right" --poll-interval 1 --idle-timeout 120 \
  >"$WORK/05_right_job.stdout" 2>"$LOGS/05_right_job.log" &
RIGHT_PID=$!
sleep 3
log "right job pid=$RIGHT_PID is polling $S3/row_check/ for left counts"

section "5. LEFT row-check: left-side counts only -> row_check/<pair>.left.json"
run "06_row_check_left" uv run dtrack row-check --config examples/e2e/config.json --s3 "$S3" --run-id "$RUN_ID"
show "$LOGS/06_row_check_left.log"

section "6. LEFT col-check: waits for the Right side's manifests, then pushes Units"
run "07_col_check_left" uv run dtrack col-check --s3 "$S3" --work-dir "$WORK/col_check" \
  --pull-workers 2 --stat-workers 2 --wip 200 --poll-interval 1 --wait-timeout 120
show "$LOGS/07_col_check_left.log"

section "7. RIGHT job drains, builds the report, publishes it, exits"
wait "$RIGHT_PID"; rc=$?
sed 's/^/  > /' "$WORK/05_right_job.stdout" | tee -a "$EXEC"
log "END   05_right_job rc=$rc elapsed=$(printf '%.3f' "$(echo "$(date +%s.%N) - $RIGHT_START" | bc)")s log=$LOGS/05_right_job.log"
show "$LOGS/05_right_job.log"

section "8. idempotent re-runs (resume semantics)"
run "08_col_check_again" uv run dtrack col-check --s3 "$S3" --work-dir "$WORK/col_check" --wait-timeout 10
run "09_right_again" $RIGHT_CMD --work "s3://$WORK_BUCKET/dtrack/v1/runs" --run-id "$RUN_ID" --right-csv-dir "$RIGHT" --out "$WORK/report-rerun" --work-dir "$WORK/right" --poll-interval 1 --idle-timeout 20
run "10_fetch_report" uv run dtrack fetch-report --report-s3 "$REPORT_S3" --out "$OUT/fetched_report"

section "9. gate policies on the Right side (separate run prefixes)"
for policy in manual ratio:0.02; do
  tag="${policy%%:*}"
  run "11_row_check_left_$tag" uv run dtrack row-check --config examples/e2e/config.json --s3 "$S3-$tag" --run-id "$RUN_ID-$tag"
  run "12_right_$tag" $RIGHT_CMD --work "$S3-$tag" --right-csv-dir "$RIGHT" --out "$WORK/report-$tag" --work-dir "$WORK/right-$tag" --gate "$policy" --poll-interval 1 --idle-timeout 8 --wait-timeout 10 || true
  uv run python examples/e2e/s3_mirror.py "$S3-$tag" "$WORK/mirror-$tag" >/dev/null 2>&1
  log "gate=$policy: manifests written=$(ls "$WORK/mirror-$tag/manifest" 2>/dev/null | grep -vc _CLOSED); $(gate_of "$WORK/mirror-$tag/row_check/orders.json"); $(gate_of "$WORK/mirror-$tag/row_check/customers.json") (right job exits rc=1 by idle timeout: nobody runs col-check in this demo, expected)"
done

section "10. mirror the work store and the report store to disk"
run "13_s3_mirror_work" uv run python examples/e2e/s3_mirror.py "$S3" "$OUT/s3_mirror"
run "14_s3_mirror_report" uv run python examples/e2e/s3_mirror.py "$REPORT_S3" "$OUT/report_store"

section "11. deliverables"
show "$OUT/report/summary.json"
show "$OUT/report/orders_compare_row.csv"
show "$OUT/report/orders_compare_col.csv"
show "$OUT/report/customers_compare_col.csv"
log "html: $OUT/report/index.html ($(wc -c <"$OUT/report/index.html") bytes)"
log "xlsx: $OUT/report/report.xlsx sheets=$(uv run python -c "from openpyxl import load_workbook; print(load_workbook('$OUT/report/report.xlsx').sheetnames)")"
log "pointer: $OUT/report_store/latest/pointer.json -> $(cat "$OUT/report_store/latest/pointer.json")"
log "fetched on-prem via fetch-report: $(ls "$OUT/fetched_report" | tr '\n' ' ')"
log "work store objects: left=$(ls "$OUT/s3_mirror/row_check" | grep -c left.json) verdicts=$(ls "$OUT/s3_mirror/row_check" | grep -v left | grep -vc _) manifests=$(ls "$OUT/s3_mirror/manifest" | grep -v _CLOSED | grep -vc keys.json) fixed_keys=$(ls "$OUT/s3_mirror/manifest" | grep -c keys.json) units=$(ls "$OUT/s3_mirror/units" | wc -l) index=$(ls "$OUT/s3_mirror/_INDEX" | wc -l) results=$(ls "$OUT/s3_mirror/results" | wc -l) dead=$(ls "$OUT/s3_mirror/dead" 2>/dev/null | wc -l)"
log "scratch (git-ignored): $WORK"

section "done"
log "execution record: $EXEC"
