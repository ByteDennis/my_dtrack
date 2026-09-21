from __future__ import annotations

from typing import Any, Protocol


class CheckpointStore(Protocol):
    def completed(self) -> set[str]: ...
    def record(self, unit_id: str, result: dict[str, Any]) -> None: ...
