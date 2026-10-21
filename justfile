set shell := ["bash", "-c"]

default:
    @just --list --unsorted

sync *args:
    if [ "{{args}}" = "--debug" ]; then uv -v sync --extra debug; elif [ -n "{{args}}" ]; then echo "usage: just sync [--debug]" >&2; exit 2; else uv sync; fi

test *args:
    if [ "{{args}}" = "--debug attach" ]; then uv run --extra debug python -m debugpy --listen 5678 --wait-for-client -m pytest -x -s tests/unit; elif [ "{{args}}" = "--debug" ] || [ "{{args}}" = "--debug ipdb" ]; then uv run --extra debug pytest -q --timeout=60 --pdb --pdbcls=IPython.terminal.debugger:TerminalPdb tests/unit; elif [ -n "{{args}}" ]; then echo "usage: just test [--debug ipdb|attach]" >&2; exit 2; else uv run pytest -q --timeout=60 tests/unit; fi

lint *args:
    if [ "{{args}}" = "--debug" ]; then verbose="--verbose"; elif [ -n "{{args}}" ]; then echo "usage: just lint [--debug]" >&2; exit 2; else verbose=""; fi; uv run ruff check $verbose src tests/unit tests/live/sources tools
    if [ "{{args}}" = "--debug" ]; then verbose="--verbose"; else verbose=""; fi; uv run ruff format $verbose --check src tests/unit tests/live/sources tools

format *args:
    if [ "{{args}}" = "--debug" ]; then verbose="--verbose"; elif [ -n "{{args}}" ]; then echo "usage: just format [--debug]" >&2; exit 2; else verbose=""; fi; uv run ruff format $verbose src tests/unit tests/live/sources tools
    if [ "{{args}}" = "--debug" ]; then verbose="--verbose"; else verbose=""; fi; uv run ruff check $verbose --fix src tests/unit tests/live/sources tools

run *args:
    uv run --extra debug pair_recon {{args}}

dbx *args:
    uv run --extra debug pair_recon dbx {{args}}

gitlab *args:
    uv run --extra debug pair_recon gitlab {{args}}

live *args:
    if [ "{{args}}" = "--debug attach" ]; then uv run --extra debug python -m debugpy --listen 5678 --wait-for-client -m pytest -x -s --run-live tests/live/sources/test_source_queries.py; elif [ "{{args}}" = "--debug" ] || [ "{{args}}" = "--debug ipdb" ]; then uv run --extra debug pytest -v -s --pdb --pdbcls=IPython.terminal.debugger:TerminalPdb --run-live tests/live/sources/test_source_queries.py; elif [ -n "{{args}}" ]; then echo "usage: just live [--debug ipdb|attach]" >&2; exit 2; else uv run pytest -v -s --run-live tests/live/sources/test_source_queries.py; fi

sas *args:
    uv run --extra debug python -m pair_recon.core.sas export {{args}}
