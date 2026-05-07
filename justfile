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

# refresh database schema (add missing columns)
refresh *args:
    dtrack refresh {{db}} {{args}}

# diagnose env-file + Oracle macros + AWS vars resolution
doctor *args:
    dtrack doctor {{args}}

# load pairs from config JSON
load-map *args:
    dtrack load-map {{db}} {{config}} --type row {{args}}

# list all pairs
pairs *args:
    dtrack list-pairs {{db}} -v {{args}}

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

# compare row counts for a pair
compare-row pair *args:
    dtrack compare-row {{db}} --pair {{pair}} {{args}}

# compare column stats for a pair
compare-col pair *args:
    dtrack compare-col {{db}} --pair {{pair}} {{args}}

# run a SQL query against the database
query sql *args:
    dtrack query {{db}} "{{sql}}" {{args}}

# show database status
status *args:
    dtrack status {{db}} {{args}}

# run tests
test *args:
    pytest {{args}}

# run tests with coverage
test-cov:
    pytest --cov=dtrack --cov-report=term-missing

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
