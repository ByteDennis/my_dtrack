from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml

_PRIVATE = ("password", "secret", "token", "credential")


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
    mode = answers.get("partition_mode", selected.get("partition_mode", "date"))
    selected["partition_mode"] = mode
    selected.update({key: answers[key] for key in ("sample", "strict") if key in answers})
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
