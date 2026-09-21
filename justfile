set shell := ["bash", "-cu"]

install:
    uv sync

test:
    uv run pytest -q

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
      uv run pytest -q -m integration tests/integration/test_s3_protocol.py
