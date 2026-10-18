set shell := ["bash", "-cu"]

default:
    @just --list --unsorted

sync:
    uv sync

test:
    uv run pytest -q --timeout=60 tests/unit

lint:
    uv run ruff check src tests/unit tests/live/sources
    uv run ruff format --check src tests/unit tests/live/sources

format:
    uv run ruff format src tests/unit tests/live/sources
    uv run ruff check --fix src tests/unit tests/live/sources

build:
    uv build

live:
    uv run pytest -v -s --run-live tests/live/sources/test_source_queries.py
