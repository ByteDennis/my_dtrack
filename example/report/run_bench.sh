#!/usr/bin/env bash
# Oracle SQL Performance Benchmark Runner
#
# Usage:
#   ./run_bench.sh --table SCHEMA.ORDERS --date-col LOAD_DT --num-col AMOUNT
#   ./run_bench.sh --table SCHEMA.ORDERS --date-col LOAD_DT --experiments dtrack_row_count
#   ./run_bench.sh --table SCHEMA.ORDERS --date-col LOAD_DT --dtrack
#   ./run_bench.sh --table SCHEMA.ORDERS --date-col LOAD_DT --cat-col STATUS --dtrack
#   ./run_bench.sh --list
#
# Multi-table example (appends results):
#   ./run_bench.sh --table SCHEMA.TABLE_A --date-col DT --num-col AMT --dtrack -o results.json
#   ./run_bench.sh --table SCHEMA.TABLE_B --date-col DT --num-col QTY --dtrack -o results.json --append
#
# Environment variables (or set in bench_config.json):
#   ORACLE_USER, ORACLE_PASSWORD

set -euo pipefail
cd "$(dirname "$0")"

CONFIG="${BENCH_CONFIG:-bench_config.json}"
ITERATIONS="${BENCH_ITERATIONS:-5}"
OUTPUT="${BENCH_OUTPUT:-bench_results.json}"
PYTHON="${PYTHON:-python3}"

DTRACK_EXPERIMENTS="dtrack_row_count,dtrack_col_stats,dtrack_top10"

usage() {
    cat <<'EOF'
Usage: run_bench.sh [OPTIONS] [experiment1,experiment2,...]

Required:
  --table TABLE       Target table (e.g. SCHEMA.MY_TABLE)
  --date-col COL      Date column name (e.g. LOAD_DT)

Optional:
  --num-col COL       Numeric column for stats (e.g. AMOUNT)
  --cat-col COL       Categorical column for top10 (e.g. STATUS)
  --pk-col COL        Primary key column for bind var test
  --where FILTER      Extra WHERE clause (without WHERE keyword)
  --list              List available experiments
  --dtrack            Run only dtrack-specific experiments
  --append            Append to existing output file
  -n NUM              Number of iterations (default: 5)
  -o FILE             Output file (default: bench_results.json)
  -c FILE             Config file (default: bench_config.json)

Examples:
  # Test row count performance on two tables
  ./run_bench.sh --table HR.EMPLOYEES --date-col HIRE_DATE --dtrack
  ./run_bench.sh --table HR.DEPARTMENTS --date-col CREATE_DT --dtrack --append

  # Test all optimizations on one table
  ./run_bench.sh --table SALES.ORDERS --date-col ORDER_DT --num-col TOTAL --pk-col ORDER_ID -n 3

  # Just index and parallel experiments
  ./run_bench.sh --table SALES.ORDERS --date-col ORDER_DT index_where,parallel_query
EOF
}

EXTRA_ARGS=()
EXPERIMENTS=""
TABLE=""
DATE_COL=""
NUM_COL=""
CAT_COL=""
PK_COL=""
WHERE=""
APPEND=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --list)
            $PYTHON oracle_bench.py --list
            exit 0
            ;;
        --table)     TABLE="$2"; shift 2 ;;
        --date-col)  DATE_COL="$2"; shift 2 ;;
        --num-col)   NUM_COL="$2"; shift 2 ;;
        --cat-col)   CAT_COL="$2"; shift 2 ;;
        --pk-col)    PK_COL="$2"; shift 2 ;;
        --where)     WHERE="$2"; shift 2 ;;
        --dtrack)    EXPERIMENTS="$DTRACK_EXPERIMENTS"; shift ;;
        --append)    APPEND="--append"; shift ;;
        -n)          ITERATIONS="$2"; shift 2 ;;
        -o)          OUTPUT="$2"; shift 2 ;;
        -c)          CONFIG="$2"; shift 2 ;;
        -h|--help)   usage; exit 0 ;;
        -*)          echo "Unknown option: $1"; usage; exit 1 ;;
        *)           EXPERIMENTS="$1"; shift ;;
    esac
done

CMD=("$PYTHON" oracle_bench.py
    --config "$CONFIG"
    --iterations "$ITERATIONS"
    --output "$OUTPUT")

[[ -n "$TABLE" ]]    && CMD+=(--table "$TABLE")
[[ -n "$DATE_COL" ]] && CMD+=(--date-col "$DATE_COL")
[[ -n "$NUM_COL" ]]  && CMD+=(--num-col "$NUM_COL")
[[ -n "$CAT_COL" ]]  && CMD+=(--cat-col "$CAT_COL")
[[ -n "$PK_COL" ]]   && CMD+=(--pk-col "$PK_COL")
[[ -n "$WHERE" ]]    && CMD+=(--where "$WHERE")
[[ -n "$EXPERIMENTS" ]] && CMD+=(--experiments "$EXPERIMENTS")
[[ -n "$APPEND" ]]   && CMD+=($APPEND)

echo "Running: ${CMD[*]}"
"${CMD[@]}"
