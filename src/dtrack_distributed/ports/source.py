from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class Source(Protocol):
    """Data-side contract used by Stage 1 and Producer/Consumer runtimes."""

    def preflight(self) -> dict[str, Any]: ...

    def partition_counts(
        self, from_date: str | None = None, to_date: str | None = None
    ) -> dict[str, int]: ...

    def pull_window(
        self,
        partitions: list[str],
        columns: list[str],
        output: Path,
        *,
        key_columns: list[str] | None = None,
    ) -> Path: ...
