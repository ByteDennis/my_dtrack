from __future__ import annotations

from typing import Any, Protocol


class Kickstarter(Protocol):
    def start(self, *, run_id: str, s3_root: str) -> dict[str, Any]: ...
