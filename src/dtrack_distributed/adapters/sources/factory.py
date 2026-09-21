from __future__ import annotations

from typing import Any

from .athena import AthenaSource
from .csv import CsvSource
from .hadoop import HadoopSource
from .oracle import OracleSource


def source_from_config(config: dict[str, Any]):
    source = str(config.get("source") or "").lower()
    if source == "oracle":
        return OracleSource(config)
    if source in {"athena", "aws"}:
        return AthenaSource(config)
    if source == "hadoop":
        return HadoopSource(config)
    if source == "csv":
        return CsvSource(config)
    raise ValueError(f"unsupported Left source: {source}")
