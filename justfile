set shell := ["bash", "-cu"]

moto_env := "AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 DTRACK_S3_ENDPOINT_URL=http://127.0.0.1:4566"
live_py := "UV_PROJECT_ENVIRONMENT=.venv-live uv run --python 3.12 --group live"
compose := "docker compose -p dtrack -f docker/compose.s3.yaml"

# show this list
default:
    @just --list --unsorted

# sync | lint | fmt | build | hooks (git strips notebook outputs on add; local copies keep them)
[group('dev')]
dev action="sync":
    @case "{{action}}" in \
      sync)  uv sync ;; \
      hooks) git config filter.nbstrip.clean ".venv/bin/python examples/nbs/strip_outputs.py" && git config filter.nbstrip.smudge cat && echo "notebook output filter installed" ;; \
      lint)  uv run ruff check src tests ;; \
      fmt)   uv run ruff format src tests && uv run ruff check --fix src tests ;; \
      build) uv build ;; \
      *)     echo "usage: just dev sync|lint|fmt|build|hooks" >&2; exit 2 ;; \
    esac

# unit | cov | moto | spark | oracle | e2e | local | all | debug | attach  [pytest args]
[group('test')]
[positional-arguments]
test kind="unit" *args:
    @shift; case "{{kind}}" in \
      unit)   uv run pytest -q tests/unit "$@" ;; \
      cov)    uv run pytest -q tests/unit --cov --cov-report=term --cov-report=html:.coverage-html "$@" ;; \
      moto)   just s3 up && {{moto_env}} uv run pytest -q --run-moto tests/local/s3 "$@" ;; \
      spark)  uv run --group spark pytest -q --run-spark tests/local/spark "$@" ;; \
      oracle) uv run pytest -q --run-oracle tests/local/oracle "$@" ;; \
      e2e)    just s3 up && {{moto_env}} uv run pytest -q --run-e2e tests/local/e2e "$@" ;; \
      local)  just test moto && just test spark && just test oracle ;; \
      all)    just test unit && just test local ;; \
      debug)  uv run pytest -x -s --pdb --pdbcls=IPython.terminal.debugger:TerminalPdb "$@" ;; \
      attach) uv run python -m debugpy --listen 5678 --wait-for-client -m pytest -x -s "$@" ;; \
      *)      echo "usage: just test unit|cov|moto|spark|oracle|e2e|local|all|debug|attach [args]" >&2; exit 2 ;; \
    esac

# sync | setup | test | teardown  [args]   real S3 + Databricks (tests/live/README.md)
[group('live')]
[positional-arguments]
live action="test" *args:
    @case "{{action}}" in \
      sync)     UV_PROJECT_ENVIRONMENT=.venv-live uv sync --python 3.12 --group live ;; \
      setup)    {{live_py}} python tests/live/provision.py setup {{args}} ;; \
      teardown) {{live_py}} python tests/live/provision.py teardown {{args}} ;; \
      test)     shift; t=""; for a in "$@"; do [ -e "tests/live/${a%%::*}" ] && t="$t tests/live/$a"; done; \
                for a in "$@"; do shift; [ -e "tests/live/${a%%::*}" ] || set -- "$@" "$a"; done; \
                {{live_py}} pytest -v --run-live -rs ${t:-tests/live} "$@" ;; \
      *)        echo "usage: just live sync|setup|test|teardown [args]" >&2; exit 2 ;; \
    esac

# up | down | status   local Moto S3 on 127.0.0.1:4566
[group('services')]
s3 action="up":
    @case "{{action}}" in \
      up)     {{compose}} up -d --wait ;; \
      down)   {{compose}} down ;; \
      status) {{compose}} ps ;; \
      *)      echo "usage: just s3 up|down|status" >&2; exit 2 ;; \
    esac

# both processes against Moto -> examples/e2e/{execution.txt,logs,output}
[group('demo')]
e2e: (s3 "up")
    ./examples/e2e/run_e2e.sh

# list | whoami | catalog | create | run | edit | audit | delete  [--greeting X]   Databricks SDK tour vs the UI
[group('docs')]
[positional-arguments]
tour step="list" *args:
    @{{live_py}} python docs/learn/live-setup/databricks_tour.py "$@"

# serve docs/learn on 127.0.0.1:<port>; open it through VS Code's port forwarding or `ssh -L`
[group('docs')]
learn port="8765":
    @echo "http://localhost:{{port}}/lessons/0001-follow-one-file.html"
    python3 -c "import functools, http.server as h; S = type('S', (h.ThreadingHTTPServer,), {'handle_error': lambda *a: None}); S(('127.0.0.1', {{port}}), functools.partial(h.SimpleHTTPRequestHandler, directory='docs/learn/live-setup')).serve_forever()"
