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

# Key column for sample-hash / col-sample (just key=cust_id col-sample).
key := ""

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
        echo "    --clean[=reports|rebuild|all]               run e2e-clean first (default rebuild)"
        echo "    e.g.  just pair=month e2e col-gen --vintage month --preset detailed"
        echo "    e.g.  just e2e all --clean        # re-load+compare, no backend (rebuild)"
        echo "    e.g.  just e2e all --clean=all    # wipe extracts+caches, full re-extract"
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

# generate AWS/Athena extraction scripts
gen-aws outdir="./sql/" *args:
    dtrack gen-aws {{config}} --outdir {{outdir}} --db {{db}} {{args}}

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

# incremental col-stats compare: per vintage window, pull each side -> tmp CSV ->
# local DuckDB stats -> upsert _col_stats + compare (bounded memory). Self-contained:
# discovers dates via its own per-date COUNT(*) (reuses _row_counts if loaded) and
# compares col stats on COUNT-MATCH dates only. No prior row-compare needed.
#   just compare-csv --vintage month            # all pairs, monthly windows
#   just pair=tableA compare-csv --resume        # skip windows already loaded
#   just compare-csv --key acct,sub --sample 500     # 500 sampled keys/window
#   just compare-csv --key acct,sub --sample 0.1      # 10% of keys/window
compare-csv *args:
    dtrack compare-csv {{db}} --config {{config}} {{ if pair == "" { "" } else { "--pair " + pair } }} {{args}}

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
    ext=""; cmprow=""; cmpcol=""; clean_level=""
    set -- {{flags}}
    while [ "$#" -gt 0 ]; do
        case "$1" in
            --no-date-filter) cmpcol="$cmpcol --no-date-filter"; shift ;;
            --vintage)        ext="$ext --vintage $2"; case "$2" in month|quarter|year) cmpcol="$cmpcol --vintage $2" ;; esac; shift 2 ;;
            --preset)         ext="$ext --preset $2"; shift 2 ;;
            --stats-override) ext="$ext --stats-override $2"; shift 2 ;;
            --from-date)      ext="$ext --from-date $2"; cmprow="$cmprow --from-date $2"; cmpcol="$cmpcol --from-date $2"; shift 2 ;;
            --to-date)        ext="$ext --to-date $2"; cmprow="$cmprow --to-date $2"; cmpcol="$cmpcol --to-date $2"; shift 2 ;;
            --clean)          clean_level="rebuild"; shift ;;
            --clean=*)        clean_level="${1#*=}"; shift ;;
            *)                ext="$ext $1"; shift ;;
        esac
    done
    if [ -n "$clean_level" ]; then
        echo ">>> clean: e2e-clean $clean_level (requested via --clean)"
        just e2edir="{{e2edir}}" config="{{config}}" db="{{db}}" e2e-clean "$clean_level"
    fi
    out="{{resultdir}}"; state="{{e2edir}}/.state{{suffix}}"
    logf="${DTRACK_LOG:-{{e2edir}}/output/run.log}"
    mkdir -p "$state" "$out" "$(dirname "$logf")"
    say() { echo ">>> $1"; printf '%s INFO [justfile.e2e] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$1" >> "$logf"; }
    # announce a subcommand, run it, and log wall-clock elapsed to run.log
    step() {
        local label="$1"; shift
        say "$label"
        local t0 t1 dt rc=0
        t0=$(date +%s.%N)
        "$@" || rc=$?
        t1=$(date +%s.%N)
        dt=$(awk "BEGIN{printf \"%.1f\", $t1-$t0}")
        printf '%s INFO [justfile.e2e] %s: done in %ss\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$label" "$dt" >> "$logf"
        echo ">>> (${dt}s) $label"
        return $rc
    }
    is_done()   { [ -z "{{force}}" ] && [ -f "$state/$1.done" ]; }
    mark_done() { touch "$state/$1.done"; }
    do_discover() {
        if is_done discover; then say "discover columns: cached (skip-cache)"; return 0; fi
        step "discover columns (load-columns -> _column_meta + csv/{qname}_columns.csv)" \
            dtrack load-columns {{db}} --config {{config}} --columns-dir "{{e2edir}}/csv"
        mark_done discover
    }
    do_row() {
        if is_done row.run; then say "[row] run-sql: cached (skip-cache)"; else
            step "[row] generate extract_row.<platform>.sql (NO run)" \
                dtrack generate {{db}} --config {{config}} --outdir {{e2edir}} --type row $ext
            step "[row] run-sql (execute the generated extract_row.<platform>.sql)" \
                dtrack run-sql {{db}} --config {{config}} --outdir {{e2edir}} --type row
            mark_done row.run
        fi
        do_discover
        step "[row] load" \
            dtrack load-row {{db}} {{e2edir}}/csv --config {{config}}
        step "[row] compare" \
            dtrack compare-row {{db}} --config {{config}} --html "$out/compare_row{{suffix}}.html" -y $cmprow
    }
    do_col_gen() {
        do_discover
        step "[col] match-columns (auto; col_map in config is kept; mapcsv: {{mapcsv}})" \
            dtrack match-columns {{db}} --config {{config}} --map-csv "{{mapcsv}}" --yes
        step "[col] generate csv/extract_col.<platform>.sql (scoped to matched cols + matching dates; NO run)" \
            dtrack generate {{db}} --config {{config}} --outdir {{e2edir}} --type col $ext
        pa=""; [ -n "{{pair}}" ] && pa="pair={{pair}} "
        say "[col-gen] review {{e2edir}}/csv/extract_col.*.sql, then run: just ${pa}e2e col-run"
    }
    do_col_run() {
        if is_done col.run; then say "[col] run-sql: cached (skip-cache)"; else
            step "[col] run-sql (execute the reviewed extract_col.<platform>.sql)" \
                dtrack run-sql {{db}} --config {{config}} --outdir {{e2edir}} --type col
            mark_done col.run
        fi
        step "[col] load" \
            dtrack load-col {{db}} {{e2edir}}/csv --config {{config}}
        step "[col] compare" \
            dtrack compare-col {{db}} --config {{config}} --html "$out/compare_col{{suffix}}.html" -y $cmpcol
    }
    case "{{type}}" in
        row)     do_row ;;
        col-gen) do_col_gen ;;
        col-run) do_col_run ;;
        col)     do_col_gen; do_col_run ;;
        all)     do_row; do_col_gen; do_col_run ;;
    esac

# clean e2e artifacts under {{e2edir}}, by distance from the backend. Each tier
# is a superset of the one above. level: reports | rebuild | all (default rebuild)
#   reports = compare_*.{html,xlsx} only       -> re-compare (instant, reads project.db)
#   rebuild = + project.db* + .state/*.done     -> re-load+compare; KEEPS csv/ extracts
#             sentinels                            & .state caches (query db + csv.duckdb)
#                                                  -> NO backend hit
#   all     = + .state (cached query db + materialized csv.duckdb) + csv/ extracts
#                                               -> full re-extract (HITS backend)
#   kept always: pairs.json, output/run.log
e2e-clean level="rebuild":
    #!/usr/bin/env bash
    set -euo pipefail
    rm_reports() { rm -f "{{e2edir}}/output"/compare_*.html "{{e2edir}}/output"/compare_*.xlsx; }
    case "{{level}}" in
        reports) rm_reports
                 echo "cleaned: compare reports (re-compare to rebuild)" ;;
        rebuild) rm_reports
                 rm -f "{{e2edir}}"/.state*/*.done "{{db}}" "{{db}}-shm" "{{db}}-wal"
                 echo "cleaned: reports + project.db + .state/*.done sentinels (csv/ extracts & .state caches kept; no backend on rerun)" ;;
        all)     rm_reports
                 rm -rf "{{e2edir}}"/.state* "{{db}}" "{{db}}-shm" "{{db}}-wal" "{{e2edir}}/csv"
                 echo "cleaned: + .state caches + csv/ extracts (full re-extract; kept pairs.json, run.log)" ;;
        *) echo "usage: just e2e-clean [reports|rebuild|all]" >&2; exit 2 ;;
    esac

# sample N rows by key from both sides -> value + backend hash16 to excel (debug hash_sum)
#   just key=cust_id sample-hash                 # all pairs, n=20
#   just key=cust_id pair=tableA sample-hash -n 50
sample-hash *args:
    dtrack sample-hash {{db}} --config {{config}} --key {{key}} {{ if pair == "" { "" } else { "--pair " + pair } }} --out {{e2edir}}/output/sample_hash{{suffix}}.xlsx {{args}}

# col-sample: deterministic-sample rows by key (-> where_map) + cols, generate col SQL only
#   just key=cust_id col-sample                      # all pairs, n=20
#   just key=cust_id col-sample -n 50 --cols amt,ccy
#   just key=cust_id pair=tableA col-sample --sample-cols 5 --vintage month
# then execute the generated SQL:  just force=1 [pair=NAME] e2e col-run
col-sample *args:
    dtrack col-sample {{db}} --config {{config}} --key {{key}} {{ if pair == "" { "" } else { "--pair " + pair } }} --outdir {{e2edir}} {{args}}

# sample-key: unified composite-key sampler (supersedes sample-hash + col-sample)
#   just key=acct,sub sample-key                     # value+hash16 dump (default)
#   just key=acct,sub sample-key -n -1               # key anti-join report
#   just key=acct,sub sample-key --gen-col           # where_map + col SQL only
#   just key=acct,sub sample-key --gen-col --out {{e2edir}}/output/sk.xlsx   # both
sample-key *args:
    dtrack sample-key {{db}} --config {{config}} --key {{key}} {{ if pair == "" { "" } else { "--pair " + pair } }} --outdir {{e2edir}} {{args}}

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

# container-backed test suite (real Oracle/Trino/Hive/Spark via testcontainers)
test-containers *args:
    pytest tests/containers --run-containers {{args}}

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
