from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from typing import Any

import yaml

from pair_recon_left.backends.sources import source_from_config
from pair_recon_left.common.sql.dates import quote_ident

SAS_SAMPLE_FORMAT = "pair-recon-sas-sample-v1"
REQUEST_ID_COLUMN = "__pair_recon_request_id"


def _artifact_paths(csv_path: str | Path) -> tuple[Path, Path, Path]:
    sample = Path(csv_path).expanduser()
    if not sample.is_absolute():
        sample = sample.resolve()
    return sample, sample.with_suffix(".sql"), sample.with_suffix(".request.yaml")


def _write_text_if_changed(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_file() and path.read_text(encoding="utf-8") == value:
        return
    temporary = path.with_name(path.name + ".part")
    try:
        temporary.write_text(value, encoding="utf-8")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def _request_column(dialect: str) -> str:
    if dialect == "oracle":
        return f'"{REQUEST_ID_COLUMN}"'
    return quote_ident(REQUEST_ID_COLUMN, dialect)


def _sample_request(pair_name: str, pair: dict[str, Any], manifest: dict[str, Any]) -> dict:
    left = {**manifest["left"], "partition": manifest.get("partition", "day")}
    source = source_from_config(left)
    if source.dialect not in {"oracle", "hadoop"}:
        raise ValueError(f"{pair_name}: sas_sample_csv requires an Oracle or Hadoop Left source")
    sampling = manifest.get("sample") or {"mode": "all"}
    if sampling.get("mode") == "fixed":
        raise ValueError(f"{pair_name}: sas_sample_csv supports all or fraction sampling")
    partitions = list(
        dict.fromkeys(
            partition
            for window in manifest.get("windows") or []
            for partition in window.get("partitions") or []
        )
    )
    if not partitions:
        raise ValueError(f"{pair_name}: SAS sample request has no selected vintage partitions")
    sql, columns = source.pull_window_sql(
        partitions,
        manifest["columns"],
        key_columns=manifest["key_cols"],
        sample_plan=sampling,
        key_families=manifest["key_families"],
        side="left",
    )
    contract = {
        "format": SAS_SAMPLE_FORMAT,
        "pair": pair_name,
        "plan_id": manifest.get("plan_id"),
        "sample": sampling,
        "partition": manifest.get("partition", "day"),
        "vintage": manifest.get("vintage"),
        "windows": [
            {"bucket": window.get("bucket"), "partitions": window.get("partitions") or []}
            for window in manifest.get("windows") or []
        ],
        "partitions": partitions,
        "expected_columns": columns,
        "query_sha256": hashlib.sha256(sql.encode()).hexdigest(),
    }
    request_id = hashlib.sha256(
        json.dumps(contract, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    literal = request_id.replace("'", "''")
    wrapped = (
        f"SELECT '{literal}' AS {_request_column(source.dialect)}, pair_recon_sample.*\n"
        f"FROM (\n{sql}\n) pair_recon_sample"
    )
    sample, sql_path, request_path = _artifact_paths(pair["left"]["sas_sample_csv"])
    return {
        **contract,
        "request_id": request_id,
        "sql_file": sql_path.name,
        "csv_file": sample.name,
        "request_file": request_path.name,
        "sql": wrapped,
        "sample_path": sample,
        "sql_path": sql_path,
        "request_path": request_path,
    }


def prepare_left_sample(
    pair_name: str, pair: dict[str, Any], manifest: dict[str, Any]
) -> dict[str, Any] | None:
    csv_path = (pair.get("left") or {}).get("sas_sample_csv")
    if not csv_path:
        return None
    request = _sample_request(pair_name, pair, manifest)
    sql_text = request.pop("sql") + "\n"
    sample = request.pop("sample_path")
    sql_path = request.pop("sql_path")
    request_path = request.pop("request_path")
    _write_text_if_changed(sql_path, sql_text)
    _write_text_if_changed(
        request_path,
        yaml.safe_dump(request, allow_unicode=True, sort_keys=False),
    )
    result = {
        "csv": str(sample),
        "sql": str(sql_path),
        "request": str(request_path),
        "request_id": request["request_id"],
        "windows": len(request["windows"]),
    }
    if not sample.is_file():
        return {**result, "state": "awaiting_sample", "reason": "CSV is missing"}
    try:
        with sample.open(encoding="utf-8-sig", newline="") as handle:
            reader = csv.reader(handle)
            header = next(reader, [])
            first = next(reader, None)
    except (OSError, UnicodeError, csv.Error) as exc:
        return {**result, "state": "awaiting_sample", "reason": f"CSV is unreadable: {exc}"}
    expected = [REQUEST_ID_COLUMN, *request["expected_columns"]]
    if header != expected:
        return {
            **result,
            "state": "awaiting_sample",
            "reason": f"CSV header does not match request {request['request_id'][:12]}",
        }
    if first is not None and (not first or first[0] != request["request_id"]):
        return {
            **result,
            "state": "awaiting_sample",
            "reason": f"CSV request ID does not match {request['request_id'][:12]}",
        }
    return {
        **result,
        "state": "ready",
        "source": {
            "source": "csv",
            "path": str(sample),
            "date_col": manifest["left"].get("date_col"),
            "date_type": manifest["left"].get("date_type"),
            "pre_sampled": True,
            "source_contract_id": request["request_id"],
        },
    }
