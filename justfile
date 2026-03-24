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
serve:
    dtrack serve {{db}} --config {{config}} --port {{port}}

# init or refresh database
init:
    dtrack init {{db}}

# refresh database schema (add missing columns)
refresh:
    dtrack refresh {{db}}

# load pairs from config JSON
load-map:
    dtrack load-map {{db}} {{config}} --type row

# list all pairs
pairs:
    dtrack list-pairs {{db}} -v

# generate SAS extraction scripts
gen-sas outdir="./sas/":
    dtrack gen-sas {{config}} {{outdir}} --db {{db}}

# generate AWS/Athena extraction scripts
gen-aws outdir="./sql/":
    dtrack gen-aws {{config}} {{outdir}} --db {{db}}

# load row count CSVs
load-row folder="./csv/":
    dtrack load-row {{db}} {{folder}}

# load column stat CSVs
load-col folder="./csv/":
    dtrack load-col {{db}} {{folder}}

# compare row counts for a pair
compare-row pair:
    dtrack compare-row {{db}} --pair {{pair}}

# compare column stats for a pair
compare-col pair:
    dtrack compare-col {{db}} --pair {{pair}}

# run a SQL query against the database
query sql:
    dtrack query {{db}} "{{sql}}"

# show database status
status:
    dtrack status {{db}}

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
