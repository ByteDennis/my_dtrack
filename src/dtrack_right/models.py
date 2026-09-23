from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class DrainTask:
    kind: str
    pair: str
    window: str
    payload: dict[str, Any]
