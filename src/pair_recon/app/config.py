import json
import os
from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml
from dotenv import find_dotenv, load_dotenv

_PRIVATE = ("password", "secret", "token", "credential")
OUTPUT_DIR_ENV = "PAIR_RECON_OUTPUT_DIR"
DEFAULT_OUTPUT_DIR = "outputs"
PROJECT_ROOT = Path(__file__).resolve().parents[3]


def load_environment(root: str | Path | None = None) -> Path | None:
    """Load only .pair_recon.env without replacing process environment values."""
    found = find_dotenv(filename=".pair_recon.env", usecwd=True)
    source = Path(found) if found else Path(root or PROJECT_ROOT) / ".pair_recon.env"
    if not source.is_file():
        return None
    load_dotenv(source, override=False)
    return source


def env_value(*names: str, default: str | None = None) -> str | None:
    for name in names:
        value = os.getenv(name, "").strip()
        if value:
            return value
    return default


def local_output_root(defaults: dict[str, Any] | None = None) -> Path:
    configured = os.getenv(OUTPUT_DIR_ENV, "").strip()
    selected = configured or (defaults or {}).get("work_dir") or DEFAULT_OUTPUT_DIR
    target = Path(selected).expanduser()
    target.mkdir(parents=True, exist_ok=True)
    return target


def _public(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _public(item)
            for key, item in value.items()
            if not any(word in key.lower() for word in _PRIVATE)
        }
    if isinstance(value, list):
        return [_public(item) for item in value]
    return value


def load_defaults(path: str | Path) -> dict[str, Any]:
    source = Path(path)
    if not source.exists():
        return {}
    value = yaml.safe_load(source.read_text(encoding="utf-8")) or {}
    if not isinstance(value, dict):
        raise TypeError("defaults must be a YAML mapping")
    return value


# >>> load a json run config, unwrapping request snapshots <<< #
def load_config(path: str | Path) -> dict[str, Any]:
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(value, dict) and value.get("format") == "pair-recon-request-v1":
        value = value.get("settings")
    if not isinstance(value, dict):
        raise TypeError("config must be a JSON object")
    return value


def save_defaults(path: str | Path, value: dict[str, Any]) -> dict[str, Any]:
    target = Path(path)
    saved = _public(value)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(yaml.safe_dump(saved, allow_unicode=True, sort_keys=False), encoding="utf-8")
    return saved


def run_config(
    name: str, pair: dict[str, Any], answers: dict[str, Any], *, timezone: str
) -> dict[str, Any]:
    selected = deepcopy(pair)
    selected.pop("_run_defaults", None)
    selected.pop("run_mode", None)
    mode = answers.get("partition_mode", selected.get("partition_mode", "date"))
    selected["partition_mode"] = mode
    selected.update({key: answers[key] for key in ("sample", "strict", "audit") if key in answers})
    checks = list(answers.get("checks", ()))
    selected["checks"] = checks
    statistics = [item for item in checks if item != "row_count"]
    if statistics:
        selected["statistics"] = statistics
    else:
        selected.pop("statistics", None)
    if mode == "all":
        for key in ("fromDate", "toDate", "vintage"):
            selected.pop(key, None)
    else:
        for source, target in (
            ("from_date", "fromDate"),
            ("to_date", "toDate"),
            ("vintage", "vintage"),
        ):
            if source in answers:
                selected[target] = answers[source]
    return {"version": 2, "timezone": timezone, "pairs": {name: selected}}
