# dtrack — quick start
#
#   just pipx               editable install (RECOMMENDED on remote)
#                            edits to .py take effect on next `just serve`
#                            edits to static/*.js & templates/*.html
#                            are picked up on browser refresh (no restart)
#   just pipx-prod          non-editable install (frozen copy)
#   just pipx-reinstall     rebuild venv (use when deps change)
#   just serve              run web UI on :8080
#   just serve-reload       uvicorn --reload (auto-restart on .py edits)
#   just init               initialize SQLite db
#   just doctor             show resolved env-file + macros + AWS vars
#
# Required env (dtrack.conf | .env | dtrack.env): PCDS_USR, <macro>_pwd for
# Oracle; AWS_DEFAULT_REGION, AWS_S3_WORK_GROUP, AWS_S3_STAGING_DIR for Athena.
# Override env-file path with --env=/path or DTRACK_ENV_FILE=/path.
# Add Oracle macros not in MACRO2SVC via DTRACK_ORACLE_MACROS=pb40:svc_x,pb50:svc_y.

# Shared working dir for BOTH the web UI and the e2e/CLI pipeline: holds
# pairs.json (config), project.db, and csv/ sas/ output/. Point web + cli at
# the same workdir so they operate on one project.
workdir := "testing"
db := workdir + "/project.db"
port := "8080"

# Where the e2e mock fixtures live. Override per machine:
#   just mockdir=example/testing/mock test-e2e
mockdir := "testing/mock"
export DTRACK_E2E_MOCK_DIR := mockdir

# Base dir for the live e2e pipeline; outputs land in {{e2edir}}/{csv,sas,output}
e2edir := workdir

# Run a subset of pairs from a dedicated config. Drop per-set files in
# {{configdir}}/ (e.g. tableA.json, tableB.json), each holding only its own
# `pairs`, then select one with `just pair=tableA e2e ...`. All sets share
# {{db}}; the HTML output and skip-cache (.state) are namespaced by the set
# name so sets never clobber each other. No pair= => {{workdir}}/pairs.json
# (the same config the web UI uses).
pair := ""
configdir := workdir
config := if pair == "" { workdir + "/pairs.json" } else { configdir + "/" + pair + ".json" }
suffix := if pair == "" { "" } else { "_" + pair }

# Where e2e comparison reports (compare_{row,col}[_pair].{html,xlsx}) are
# written. Override to keep e2e output separate from the web UI's output dir,
# e.g. when running web + e2e at once:  just resultdir=testing/e2e_out e2e all
resultdir := e2edir + "/output"

# Unified pipeline log (every CLI command + errors). See `just logs`.
export DTRACK_LOG := e2edir + "/output/run.log"

# Explicit col-mapping file for `e2e col` (CSV: pair,left,right). Optional —
# if it doesn't exist, match-columns just falls back to auto + col_rules.
mapcsv := e2edir + "/csv/col_map.csv"

# Set force=1 to ignore the e2e skip-cache and re-run every expensive stage:
#   just force=1 e2e all
force := ""

# list recipes
default:
    @just --list

# per-subcommand help for a recipe (`just help e2e`); no arg = recipe list
help topic="":
    #!/usr/bin/env bash
    case "{{topic}}" in
    "") just --list ;;
    e2e)
        echo "just [pair=NAME] [force=1] e2e <subcommand> [dtrack extract/generate flags]"
        echo
        echo "  row       generate+run-sql extract_row.<platform>.sql -> discover -> load-row -> compare-row"
        echo "  col-gen   match-columns (auto) -> generate csv/extract_col.<platform>.sql (NO run)"
        echo "            review the SQL, then col-run"
        echo "  col-run   run-sql (executes the generated SQL) -> load-col -> compare-col"
        echo "  col       col-gen then col-run (no pause)"
        echo "  all       row, then col   (default)"
        echo
        echo "  (column annotation is done in the web col_mapping page, not the CLI)"
        echo "  extra flags are forwarded to the underlying dtrack extract/generate, e.g.:"
        echo "    --vintage day|week|month|quarter|year|all   bucket granularity"
        echo "    --preset quick|standard|detailed            col-stats depth"
        echo "    --stats-override ...                        per-stat override"
        echo "    --from-date / --to-date YYYY-MM-DD          incremental window"
        echo "    e.g.  just pair=month e2e col-gen --vintage month --preset detailed"
        echo "  pair=NAME            use {{configdir}}/NAME.json; HTML + .state namespaced _NAME"
        echo "  resultdir=DIR        where reports land (default {{e2edir}}/output; separate from web)"
        echo "  force=1              ignore skip-cache; re-run row/discover/col extract"
        ;;
    *) just --show "{{topic}}" ;;
    esac

# editable pipx install — source stays in this repo, edits are live
pipx:
    pipx uninstall dtrack || true
    pipx install -e .

# frozen pipx install — copies source into the pipx venv
pipx-prod:
    pipx uninstall dtrack || true
    pipx install .

# force-reinstall (use when adding/removing dependencies)
pipx-reinstall:
    pipx install -e . --force

# install for local dev (uv editable, all extras)
install:
    uv pip install -e ".[dev,debug]"

# install core only (no optional deps)
install-core:
    uv pip install -e .

# start web UI
serve *args:
    dtrack serve {{db}} --config {{config}} --port {{port}} {{args}}

# auto-reload web UI on .py edits
serve-reload *args:
    uvicorn dtrack.web.app:app --reload --host 0.0.0.0 --port {{port}} {{args}}

# init or refresh database (just init / just init --force)
init *args:
    dtrack init {{db}} {{args}}

# diagnose env-file + Oracle macros + AWS vars resolution
doctor *args:
    dtrack doctor {{args}}

# generate SAS extraction scripts
gen-sas outdir="./sas/" *args:
    dtrack gen-sas {{config}} {{outdir}} --db {{db}} {{args}}

# generate AWS/Athena extraction scripts
gen-aws outdir="./sql/" *args:
    dtrack gen-aws {{config}} {{outdir}} --db {{db}} {{args}}

# load row count CSVs
load-row folder="./csv/" *args:
    dtrack load-row {{db}} {{folder}} {{args}}

# load column stat CSVs
load-col folder="./csv/" *args:
    dtrack load-col {{db}} {{folder}} {{args}}

# compare row counts (all pairs in config) -> output/compare_row.html
compare-row *args:
    dtrack compare-row {{db}} --config {{config}} --html {{e2edir}}/output/compare_row.html -y {{args}}

# compare column stats (all pairs in config) -> output/compare_col.html
compare-col *args:
    dtrack compare-col {{db}} --config {{config}} --html {{e2edir}}/output/compare_col.html -y {{args}}

# --- live e2e pipeline: real backends -> HTML -----------------------------
# Single entry point. type = row | col-gen | col-run | col | all. Outputs land
# in {{e2edir}}/output/compare_{row,col}[_<pair>].html. Override the backend
# with `just config=my_live.json e2e all`, or pick a pair-subset config with
# `just pair=tableA e2e ...` (see the pair/configdir vars above).
#
#   row     : generate extract_row.<platform>.sql -> run-sql -> discover -> load-row -> compare-row
#   col-gen : discover-columns -> match-columns (auto)
#             -> `dtrack generate` writes csv/extract_col.<platform>.sql, then STOPS (no run)
#   col-run : `dtrack run-sql` executes the generated SQL -> load-col -> compare-col
#   col     : col-gen then col-run (no pause)
#   all     : row, then col
#
# Mirrors the web col_gen flow: matching happens BEFORE the col SQL is
# generated, so extract_col.sql is scoped to the matched col_map. `col-gen`
# uses `dtrack generate` (writes SQL only, never runs; one
# extract_col.<platform>.sql per platform so they don't clobber), then
# `col-run` runs the reviewed file via `dtrack run-sql` (mirrors the web
# /api/extract/run-sql — executes the SAME extract_col.<platform>.sql block by
# block, not a regenerate). Extraction flags (--vintage --preset
# --stats-override --from/--to-date) are baked into the SQL at `col-gen`, so
# put them on col-gen; on col-run only --vintage / date flags still matter (they
# reach compare-col). Matching is automatic (auto-match + --map-csv + the pair's
# saved col_rules); annotate columns in the web col_mapping page, not the CLI.
#
# discover-columns (load-columns -> _column_meta + csv/{qname}_columns.csv)
# runs once per invocation (shared `discover` sentinel); it lands in the row
# phase so column/datatype metadata is downloaded as early as `just e2e row`.
#
# col SQL is filtered to the matching dates row-compare wrote, so the row
# phase MUST finish first. `all` enforces this; running `col*` on its own
# REQUIRES a prior `just e2e row` (or `all`) to have populated _row_comparison.
#
# SKIP-CACHE: the expensive backend stages (row extract, discover, col
# extract) drop a sentinel in {{e2edir}}/.state/ on success and are
# skipped while it exists. The cheap local steps (load / compare / match)
# always re-run, so reports stay fresh and the DB is repopulated from the
# cached CSVs even after a reset. Force a fresh pull with `just force=1 e2e
# ...` or wipe the cache with `just e2e-clean`.
e2e type="all" *flags:
    #!/usr/bin/env bash
    set -euo pipefail
    case "{{type}}" in row|col-gen|col-run|col|all) ;; *) echo "usage: just [pair=NAME] e2e [row|col-gen|col-run|col|all]   (details: just help e2e)" >&2; exit 2 ;; esac
    [ -f "{{config}}" ] || { echo "config not found: {{config}} (pair='{{pair}}')" >&2; exit 2; }
    ext=""; cmprow=""; cmpcol=""
    set -- {{flags}}
    while [ "$#" -gt 0 ]; do
        case "$1" in
            --no-date-filter) cmpcol="$cmpcol --no-date-filter"; shift ;;
            --vintage)        ext="$ext --vintage $2"; case "$2" in month|quarter|year) cmpcol="$cmpcol --vintage $2" ;; esac; shift 2 ;;
            --preset)         ext="$ext --preset $2"; shift 2 ;;
            --stats-override) ext="$ext --stats-override $2"; shift 2 ;;
            --from-date)      ext="$ext --from-date $2"; cmprow="$cmprow --from-date $2"; cmpcol="$cmpcol --from-date $2"; shift 2 ;;
            --to-date)        ext="$ext --to-date $2"; cmprow="$cmprow --to-date $2"; cmpcol="$cmpcol --to-date $2"; shift 2 ;;
            *)                ext="$ext $1"; shift ;;
        esac
    done
    out="{{resultdir}}"; state="{{e2edir}}/.state{{suffix}}"
    logf="${DTRACK_LOG:-{{e2edir}}/output/run.log}"
    mkdir -p "$state" "$out" "$(dirname "$logf")"
    say() { echo ">>> $1"; printf '%s INFO [justfile.e2e] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$1" >> "$logf"; }
    is_done()   { [ -z "{{force}}" ] && [ -f "$state/$1.done" ]; }
    mark_done() { touch "$state/$1.done"; }
    do_discover() {
        if is_done discover; then say "discover columns: cached (skip-cache)"; return 0; fi
        say "discover columns (load-columns -> _column_meta + csv/{qname}_columns.csv)"
        dtrack load-columns {{db}} --config {{config}} --columns-dir "{{e2edir}}/csv"
        mark_done discover
    }
    do_row() {
        if is_done row.run; then say "[row] run-sql: cached (skip-cache)"; else
            say "[row] generate extract_row.<platform>.sql$ext (NO run)"
            dtrack generate {{db}} --config {{config}} --outdir {{e2edir}} --type row $ext
            say "[row] run-sql (execute the generated extract_row.<platform>.sql; mock-copies fixtures)"
            dtrack run-sql {{db}} --config {{config}} --outdir {{e2edir}} --type row
            mark_done row.run
        fi
        do_discover
        say "[row] load"
        dtrack load-row {{db}} {{e2edir}}/csv --config {{config}}
        say "[row] compare$cmprow"
        dtrack compare-row {{db}} --config {{config}} --html "$out/compare_row{{suffix}}.html" -y $cmprow
    }
    do_col_gen() {
        do_discover
        say "[col] match-columns (auto; col_map in config is kept; mapcsv: {{mapcsv}})"
        dtrack match-columns {{db}} --config {{config}} --map-csv "{{mapcsv}}" --yes
        say "[col] generate csv/extract_col.<platform>.sql$ext (scoped to matched cols + matching dates; NO run)"
        dtrack generate {{db}} --config {{config}} --outdir {{e2edir}} --type col $ext
        pa=""; [ -n "{{pair}}" ] && pa="pair={{pair}} "
        say "[col-gen] review {{e2edir}}/csv/extract_col.*.sql, then run: just ${pa}e2e col-run"
    }
    do_col_run() {
        if is_done col.run; then say "[col] run-sql: cached (skip-cache)"; else
            say "[col] run-sql (execute the reviewed extract_col.<platform>.sql; mock-copies fixtures)"
            dtrack run-sql {{db}} --config {{config}} --outdir {{e2edir}} --type col
            mark_done col.run
        fi
        say "[col] load"
        dtrack load-col {{db}} {{e2edir}}/csv --config {{config}}
        say "[col] compare$cmpcol"
        dtrack compare-col {{db}} --config {{config}} --html "$out/compare_col{{suffix}}.html" -y $cmpcol
    }
    case "{{type}}" in
        row)     do_row ;;
        col-gen) do_col_gen ;;
        col-run) do_col_run ;;
        col)     do_col_gen; do_col_run ;;
        all)     do_row; do_col_gen; do_col_run ;;
    esac

# clean e2e generated artifacts under {{e2edir}}. level: state | data | all
#   state (default) = skip-cache sentinels (.state) only; cheap steps re-run, data kept
#   data            = .state + csv + sas; forces re-extraction from the backend
#   all             = + output + project db; keeps pairs.json, mock/, generate_mock_data.py
e2e-clean level="state":
    #!/usr/bin/env bash
    set -euo pipefail
    case "{{level}}" in
        state) rm -rf "{{e2edir}}"/.state*
               echo "cleaned: .state* (skip-cache, all pair sets)" ;;
        data)  rm -rf "{{e2edir}}"/.state* "{{e2edir}}/csv" "{{e2edir}}/sas"
               echo "cleaned: .state* + csv + sas (forces re-extract)" ;;
        all)   rm -rf "{{e2edir}}"/.state* "{{e2edir}}/csv" "{{e2edir}}/sas" \
                      "{{e2edir}}/output" "{{db}}" "{{db}}-shm" "{{db}}-wal"
               echo "cleaned: all generated artifacts (kept pairs.json, mock/, generate_mock_data.py)" ;;
        *) echo "usage: just e2e-clean [state|data|all]" >&2; exit 2 ;;
    esac

# sample N rows by key from both sides -> value + backend hash16 to excel (debug hash_sum)
#   just key=cust_id sample-hash                 # all pairs, n=20
#   just key=cust_id pair=tableA sample-hash -n 50
sample-hash key="" *args:
    dtrack sample-hash {{db}} --config {{config}} --key {{key}} {{ if pair == "" { "" } else { "--pair " + pair } }} --out {{e2edir}}/output/sample_hash{{suffix}}.xlsx {{args}}

# run a SQL query against the database
query sql *args:
    dtrack query {{db}} "{{sql}}" {{args}}

# tail the unified pipeline log ({{e2edir}}/output/run.log; override with DTRACK_LOG)
logs:
    #!/usr/bin/env bash
    f="${DTRACK_LOG:-{{e2edir}}/output/run.log}"
    echo "tailing $f (ctrl+c to stop)"
    exec tail -n 120 -f "$f"

# run tests
test *args:
    pytest {{args}}

# run tests with coverage (pass --lf to re-run only last-failed, --ff for failed-first)
test-cov *args:
    pytest --cov=dtrack --cov-report=term-missing {{args}}

# (re)generate e2e mock fixtures into {{mockdir}} (pass --seed N, or --out to override)
gen-mock *args:
    python testing/generate_mock_data.py --out {{mockdir}} {{args}}

# lint check (if ruff is available)
lint:
    ruff check dtrack/

# format code (if ruff is available)
fmt:
    ruff format dtrack/

# clean generated files
clean:
    rm -rf ./sas/ ./sql/ ./csv/ __pycache__ .pytest_cache
    find . -name '__pycache__' -type d -exec rm -rf {} + 2>/dev/null || true
