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

db := "testing/project.db"
config := "testing/config.json"
port := "8080"

# Where the e2e mock fixtures live. Override per machine:
#   just mockdir=example/testing/mock test-e2e
mockdir := "testing/mock"
export DTRACK_E2E_MOCK_DIR := mockdir

# Base dir for the live e2e pipeline; outputs land in {{e2edir}}/{csv,sas,output}
e2edir := "testing"

# Unified pipeline log (every CLI command + full stdout detail). See `just logs`.
export DTRACK_LOG := e2edir + "/output/dtrack.log"

# Set force=1 to ignore the e2e skip-cache and re-run every expensive stage:
#   just force=1 e2e all
force := ""

# list recipes
default:
    @just --list

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
# Single entry point. type = row | col | all. Outputs land in
# {{e2edir}}/output/compare_{row,col}.html. Override the backend with
# `just config=my_live.json e2e all`.
#
#   row : extract-row -> discover-columns -> load-row -> compare-row
#   col : discover-columns -> extract-col -> load-col -> match-columns -> compare-col
#   all : row, then col
#
# discover-columns (load-columns -> _column_meta) runs once per invocation
# (shared `discover` sentinel); it lands in the row phase so column/datatype
# metadata is downloaded as early as `just e2e row`.
#
# col SQL is filtered to the matching dates row-compare wrote, so the row
# phase MUST finish first. `all` enforces this; running `col` on its own
# REQUIRES a prior `just e2e row` (or `all`) to have populated _row_comparison.
#
# --skip-mapping skips column discovery + auto-match (load-columns and
# match-columns); it then relies on the col_map already in the config, with
# compare-col falling back to same-named columns. Use it only when the config
# already carries `columns` (and ideally `col_map`) for each side.
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
    case "{{type}}" in row|col|all) ;; *) echo "usage: just e2e [row|col|all] [--skip-mapping]" >&2; exit 2 ;; esac
    skip_mapping=0
    for f in {{flags}}; do case "$f" in --skip-mapping) skip_mapping=1 ;; esac; done
    out="{{e2edir}}/output"; state="{{e2edir}}/.state"; mkdir -p "$state" "$out"
    is_done()   { [ -z "{{force}}" ] && [ -f "$state/$1.done" ]; }
    mark_done() { touch "$state/$1.done"; }
    do_discover() {
        [ "$skip_mapping" -eq 0 ] || return 0
        if is_done discover; then echo ">>> discover columns: cached"; return 0; fi
        echo ">>> discover columns (load-columns -> _column_meta + csv/_columns.csv)"
        dtrack load-columns {{db}} --config {{config}} --columns-csv "{{e2edir}}/csv/_columns.csv"
        mark_done discover
    }
    if [ "{{type}}" = "row" ] || [ "{{type}}" = "all" ]; then
        if is_done row.extract; then echo ">>> [row] extract: cached"; else
            echo ">>> [row] extract"
            dtrack extract {{db}} --config {{config}} --outdir {{e2edir}} --type row
            mark_done row.extract
        fi
        do_discover
        echo ">>> [row] load"
        dtrack load-row {{db}} {{e2edir}}/csv --config {{config}}
        echo ">>> [row] compare"
        dtrack compare-row {{db}} --config {{config}} --html "$out/compare_row.html" -y
    fi
    if [ "{{type}}" = "col" ] || [ "{{type}}" = "all" ]; then
        do_discover
        if is_done col.extract; then echo ">>> [col] extract: cached"; else
            echo ">>> [col] extract (filtered to row-compare matching dates)"
            dtrack extract {{db}} --config {{config}} --outdir {{e2edir}} --type col
            mark_done col.extract
        fi
        echo ">>> [col] load"
        dtrack load-col {{db}} {{e2edir}}/csv --config {{config}}
        if [ "$skip_mapping" -eq 0 ]; then
            echo ">>> [col] match-columns (--skip-mapping to skip)"
            dtrack match-columns {{db}} --config {{config}} --yes
        fi
        echo ">>> [col] compare"
        dtrack compare-col {{db}} --config {{config}} --html "$out/compare_col.html" -y
    fi

# clean e2e generated artifacts under {{e2edir}}. level: state | data | all
#   state (default) = skip-cache sentinels (.state) only; cheap steps re-run, data kept
#   data            = .state + csv + sas; forces re-extraction from the backend
#   all             = + output + project db; keeps config.json, mock/, generate_mock_data.py
e2e-clean level="state":
    #!/usr/bin/env bash
    set -euo pipefail
    case "{{level}}" in
        state) rm -rf "{{e2edir}}/.state"
               echo "cleaned: .state (skip-cache)" ;;
        data)  rm -rf "{{e2edir}}/.state" "{{e2edir}}/csv" "{{e2edir}}/sas"
               echo "cleaned: .state + csv + sas (forces re-extract)" ;;
        all)   rm -rf "{{e2edir}}/.state" "{{e2edir}}/csv" "{{e2edir}}/sas" \
                      "{{e2edir}}/output" "{{db}}" "{{db}}-shm" "{{db}}-wal"
               echo "cleaned: all generated artifacts (kept config.json, mock/, generate_mock_data.py)" ;;
        *) echo "usage: just e2e-clean [state|data|all]" >&2; exit 2 ;;
    esac

# run a SQL query against the database
query sql *args:
    dtrack query {{db}} "{{sql}}" {{args}}

# tail the unified pipeline log ({{e2edir}}/output/dtrack.log; override with DTRACK_LOG)
logs:
    #!/usr/bin/env bash
    f="${DTRACK_LOG:-{{e2edir}}/output/dtrack.log}"
    echo "tailing $f"; tail -n 120 -f "$f"

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
