set shell := ["bash", "-cu"]

install:
    uv sync

test:
    uv run pytest -q

test-left:
    uv run pytest -q tests/left

test-right:
    uv run pytest -q tests/right

test-e2e: s3-up
    AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 \
      DTRACK_S3_ENDPOINT_URL=http://127.0.0.1:4566 \
      uv run pytest -q --run-e2e tests/e2e

test-spark:
    uv run pytest -q --run-spark tests/spark

test-containers:
    uv run pytest -q --run-containers tests/containers

test-live:
    uv run pytest -q --run-live tests/live

test-debug *args:
    uv run pytest -x -s --pdb --pdbcls=IPython.terminal.debugger:TerminalPdb {{args}}

test-attach *args:
    uv run python -m debugpy --listen 5678 --wait-for-client -m pytest -x -s {{args}}

lint:
    uv run ruff check src tests

build:
    uv build

s3-up:
    docker compose -f docker/compose.s3.yaml up -d --wait

s3-down:
    docker compose -f docker/compose.s3.yaml down

test-s3: s3-up
    AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 \
      DTRACK_S3_ENDPOINT_URL=http://127.0.0.1:4566 \
      uv run pytest -q --run-integration tests/integration

e2e: s3-up
    ./examples/e2e/run_e2e.sh
