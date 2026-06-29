# dtrack — quick start
#   just pipx       editable install (recommended; .py live on next serve, js/html on refresh)
#   just pipx-prod  frozen install   ·   just pipx-reinstall  rebuild venv (deps changed)
#   just serve      web UI on :8080   ·   just serve-reload    auto-restart on .py edits
#   just init       initialize SQLite db   ·   just doctor      resolved env-file/macros/AWS vars
#
# Env (dtrack.conf | .env | dtrack.env): Oracle PCDS_USR + <macro>_pwd; Athena
# AWS_DEFAULT_REGION/_S3_WORK_GROUP/_S3_STAGING_DIR. Override path with --env=PATH or
# DTRACK_ENV_FILE. Extra Oracle macros: DTRACK_ORACLE_MACROS=pb40:svc_x,pb50:svc_y.

# Shared workdir for web UI + e2e/CLI: holds pairs.json, project.db, csv/ sas/ output/.
# Point web + cli at the same dir to share one project.
workdir := "testing"
db := workdir + "/project.db"
port := "8080"

# Base dir for the live e2e pipeline; outputs land in {{e2edir}}/{csv,sas,output}
e2edir := workdir

# Pair-subset run: drop {{configdir}}/NAME.json (only its `pairs`), select via
# `just pair=NAME e2e ...`. Sets share {{db}}; HTML + .state are namespaced by NAME
# so they don't clobber. No pair= => {{workdir}}/pairs.json (the web UI's config).
pair := ""
configdir := workdir
config := if pair == "" { workdir + "/pairs.json" } else { configdir + "/" + pair + ".json" }
suffix := if pair == "" { "" } else { "_" + pair }

# Where e2e reports (compare_{row,col}[_pair].{html,xlsx}) land. Override to keep
# separate from the web UI output: just resultdir=testing/e2e_out e2e all
resultdir := e2edir + "/output"

# Unified pipeline log (every CLI command + errors). See `just logs`.
export DTRACK_LOG := e2edir + "/output/run.log"

# Optional col-mapping for `e2e col` (CSV: pair,left,right); absent => auto + col_rules.
mapcsv := e2edir + "/csv/col_map.csv"

# force=1 ignores the e2e skip-cache and re-runs every expensive stage.
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
# discovers dates via its own per-date COUNT(*) on COUNT-MATCH dates; no prior row-compare.
#   just compare-csv --vintage month                  # all pairs, monthly windows
#   just force=1 compare-csv                           # re-pull (ignore skip-cache)
#   just compare-csv --key acct,sub --sample 500       # 500 fixed keys/window (raw IN-list)
#   just compare-csv --key acct,sub --sample 0.1       # 10% via hash32(norm key) pushdown
#   just compare-csv --verbose                         # print full SQL (default: concise)
# --data-dir (must exist) = transient per-window scratch; project.db + {pair}_compare_col.csv
# persist. Skip-cache is default; changed sample/key/col_filter/preset/vintage auto-re-pulls.
#
# incremental col-stats compare per vintage window (bounded memory; details above)
compare-csv *args:
    dtrack compare-csv {{db}} --config {{config}} {{ if pair == "" { "" } else { "--pair " + pair } }} {{ if force == "" { "" } else { "--force" } }} {{args}}

# --- live e2e pipeline: real backends -> HTML ---------------------------------
# Single entry point. type = row | col-gen | col-run | col | all (default all).
# Reports land in {{resultdir}}/compare_{row,col}[_<pair>].html. Subcommand +
# flag-forwarding details: `just help e2e`. Key rules:
#  - row BEFORE col: col SQL is scoped to the dates row-compare matched, so `col*`
#    alone needs a prior `just e2e row` (or `all`); `all` enforces it.
#  - col-gen writes csv/extract_col.<platform>.sql and STOPS; review it, then col-run
#    executes the SAME file via run-sql (mirrors the web flow, no regenerate).
#  - extraction flags (--vintage --preset --stats-override --from/--to-date) bake into
#    the SQL at col-gen; on col-run only --vintage/date flags still reach compare-col.
#  - discover-columns runs once per invocation (shared sentinel), in the row phase.
#  - skip-cache (default): row-extract/discover/col-extract drop a {{e2edir}}/.state
#    sentinel and skip while it exists; load/compare/match always re-run. Bypass with
#    `just force=1 e2e ...` or wipe via `just e2e-clean`.
#
# live e2e pipeline: row|col-gen|col-run|col|all -> HTML (run `just help e2e`)
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

# clean e2e artifacts under {{e2edir}}, by distance from the backend (each tier a
# superset). level: reports | rebuild | all (default rebuild). Always keeps pairs.json + run.log.
#   reports = compare_*.{html,xlsx}            -> re-compare (instant, reads project.db)
#   rebuild = + project.db* + .state/*.done    -> re-load+compare; KEEPS csv/ + .state caches (no backend)
#   all     = + .state caches + csv/ extracts  -> full re-extract (HITS backend)
#
# clean e2e artifacts: reports | rebuild | all (default rebuild)
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
#
# sample N rows by key, both sides -> value + hash16 to excel (debug hash_sum)
sample-hash *args:
    dtrack sample-hash {{db}} --config {{config}} --key {{key}} {{ if pair == "" { "" } else { "--pair " + pair } }} --out {{e2edir}}/output/sample_hash{{suffix}}.xlsx {{args}}

# col-sample: deterministic-sample rows by key (-> where_map) + cols, generate col SQL only
#   just key=cust_id col-sample                      # all pairs, n=20
#   just key=cust_id col-sample -n 50 --cols amt,ccy
#   just key=cust_id pair=tableA col-sample --sample-cols 5 --vintage month
# then execute the generated SQL:  just force=1 [pair=NAME] e2e col-run
#
# deterministic key+col sample -> where_map + col SQL only (generate, no run)
col-sample *args:
    dtrack col-sample {{db}} --config {{config}} --key {{key}} {{ if pair == "" { "" } else { "--pair " + pair } }} --outdir {{e2edir}} {{args}}

# sample-key: unified composite-key sampler (supersedes sample-hash + col-sample)
#   just key=acct,sub sample-key                     # value+hash16 dump (default)
#   just key=acct,sub sample-key -n -1               # key anti-join report
#   just key=acct,sub sample-key --gen-col           # where_map + col SQL only
#   just key=acct,sub sample-key --gen-col --out {{e2edir}}/output/sk.xlsx   # both
#
# unified composite-key sampler (supersedes sample-hash + col-sample)
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
