# dtrack justfile

db := "testing/project.db"
config := "testing/config.json"
port := "8080"

# list recipes
default:
    @just --list

# install dtrack with all optional deps
install:
    uv pip install -e ".[web,aws,dev,debug]"

# install core only (no optional deps)
install-core:
    uv pip install -e .

# start web UI
serve *args:
    dtrack serve {{db}} --config {{config}} --port {{port}} {{args}}

# init or refresh database (just init / just init --force)
init *args:
    dtrack init {{db}} {{args}}

# refresh database schema (add missing columns)
refresh *args:
    dtrack refresh {{db}} {{args}}

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
