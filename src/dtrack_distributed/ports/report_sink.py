from __future__ import annotations

from pathlib import Path
from typing import Protocol


class ReportSink(Protocol):
    def publish(self, path: Path) -> str: ...
