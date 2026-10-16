#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
exec uv run --extra dbx python "$HERE/src/launch.py" "$@"
