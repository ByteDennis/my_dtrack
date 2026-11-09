import json
import os
from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import yaml
from dotenv import find_dotenv, load_dotenv

from pair_recon.core.metadata.constant import (
    CATALOG_RESETTABLE_RUN_DEFAULT_FIELDS,
    CATALOG_RUN_DEFAULT_FIELDS,
)

_PRIVATE = ("password", "secret", "token", "credential")
OUTPUT_DIR_ENV = "PAIR_RECON_OUTPUT_DIR"
DEFAULT_OUTPUT_DIR = "outputs"
PROJECT_ROOT = Path(__file__).resolve().parents[3]


def column_list(value) -> list[str]:
    """Convert comma text or a sequence into stripped, unique names in input order."""
    items = value if isinstance(value, list | tuple) else str(value or "").split(",")
    return list(dict.fromkeys(str(item).strip() for item in items if str(item).strip()))


def _sample_rows(value) -> int:
    count = int(value)
    if count < 0:
        raise ValueError("sample_rows must be a non-negative integer")
    return count


@dataclass(frozen=True)
class RunOption:
    default: Any = None
    parse: Callable[[Any], Any] | None = None


# All entries are saved and passed directly to Pair configuration. A parser marks
# options read without a prompt; interactive options keep their CLI handling.
RUN_OPTIONS = {
    "sample": RunOption(),
    "use_modular": RunOption(),
    "normalization_rules": RunOption(),
    "audit": RunOption(),
    "sample_rows": RunOption(50, _sample_rows),
    "excluded_columns": RunOption(),
    "sample_columns": RunOption((), column_list),
    "sample_partitions": RunOption((), column_list),
}


def read_run_options(defaults: dict[str, Any]) -> dict[str, Any]:
    """Read automatic options after resolving source precedence, preserving explicit values."""
    return {
        name: option.parse(defaults.get(name, option.default))
        for name, option in RUN_OPTIONS.items()
        if option.parse is not None
    }


def select_run_options(values: dict[str, Any]) -> dict[str, Any]:
    """Select supplied direct options for both saved settings and Pair configuration."""
    return {name: deepcopy(values[name]) for name in RUN_OPTIONS if name in values}


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


def env_boolean(name: str, *, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    normalized = value.strip().casefold()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be true or false")


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


def resolve_run_defaults(
    metadata: dict[str, Any],
    saved_defaults: dict[str, Any],
    *,
    catalog_authoritative: bool = False,
) -> dict[str, Any]:
    """Merge run defaults once, retaining explicit None/False/empty values."""
    catalog_defaults = metadata.get("_run_defaults") or {}
    resolved = {**catalog_defaults, **saved_defaults}
    if catalog_authoritative:
        resolved.update(
            {name: metadata[name] for name in CATALOG_RUN_DEFAULT_FIELDS if name in metadata}
        )
        resolved.update(
            {name: metadata.get(name) for name in CATALOG_RESETTABLE_RUN_DEFAULT_FIELDS}
        )
        resolved.update(catalog_defaults)
    return resolved


def default_to_date(value: Any, timezone: str) -> str:
    """Use the run timezone's current day when no effective end date is supplied."""
    if value is not None and str(value).strip():
        return str(value).strip()
    return datetime.now(ZoneInfo(timezone)).date().isoformat()


def run_config(
    name: str, pair: dict[str, Any], answers: dict[str, Any], *, timezone: str
) -> dict[str, Any]:
    selected = deepcopy(pair)
    selected.pop("_run_defaults", None)
    selected.pop("run_mode", None)
    partition = answers.get("partition", selected.get("partition", "day"))
    selected["partition"] = partition
    selected.update(select_run_options(answers))
    checks = list(answers.get("checks", ()))
    selected["checks"] = checks
    statistics = [item for item in checks if item != "row_count"]
    if statistics:
        selected["statistics"] = statistics
    else:
        selected.pop("statistics", None)
    if partition == "all":
        for key in (
            "fromDate",
            "toDate",
            "vintage",
            "excluded_partitions",
            "vintage_sample_size",
            "vintage_values",
        ):
            selected.pop(key, None)
    else:
        for side in ("left", "right"):
            side_config = selected.setdefault(side, {})
            date_col = answers.get(f"{side}_date_col")
            if date_col:
                side_config["date_col"] = date_col
            elif f"{side}_date_col" in answers:
                side_config.pop("date_col", None)
            if f"{side}_date_type" in answers:
                side_config["date_type"] = answers[f"{side}_date_type"]
        for source, target in (
            ("from_date", "fromDate"),
            ("to_date", "toDate"),
            ("excluded_partitions", "excluded_partitions"),
            ("vintage", "vintage"),
        ):
            if source in answers:
                selected[target] = answers[source]
        for field in ("vintage_sample_size", "vintage_values"):
            if answers.get(field) not in (None, "", []):
                selected[field] = answers[field]
            elif field in answers:
                selected.pop(field, None)
    return {"timezone": timezone, "pairs": {name: selected}}
