from __future__ import annotations

from typing import Any

from ..sources.csv import CsvSource


class CsvAnchorTarget(CsvSource):
    """Local Phase D Right-side correctness anchor."""

    def __init__(self, config: dict[str, Any]):
        super().__init__({**config, "source": "csv"})
