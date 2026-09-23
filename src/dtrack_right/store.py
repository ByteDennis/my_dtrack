from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from .s3 import S3Store


# >>> same contract as S3Store over a directory: a Unity Catalog Volume path or a local folder <<< #
class VolumeStore:
    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root_uri = str(self.root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, relative: str) -> Path:
        return self.root / relative.lstrip("/")

    @staticmethod
    def json_bytes(value: Any) -> bytes:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()

    def put_bytes(self, relative: str, body: bytes, *, content_type: str = "") -> str:
        path = self._path(relative)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(path.name + ".part")
        temporary.write_bytes(body)
        temporary.replace(path)
        return relative

    def put_json(self, relative: str, value: Any) -> str:
        return self.put_bytes(relative, self.json_bytes(value))

    def get_bytes(self, relative: str) -> bytes:
        return self._path(relative).read_bytes()

    def get_json(self, relative: str) -> Any:
        return json.loads(self.get_bytes(relative).decode())

    def exists(self, relative: str) -> bool:
        return self._path(relative).is_file()

    def list_keys(self, prefix: str, *, start_after: str | None = None) -> list[str]:
        base = self._path(prefix)
        folder = base if base.is_dir() else base.parent
        if not folder.is_dir():
            return []
        keys = sorted(
            str(item.relative_to(self.root)).replace("\\", "/")
            for item in folder.rglob("*")
            if item.is_file() and not item.name.endswith(".part")
        )
        keys = [key for key in keys if key.startswith(prefix.lstrip("/"))]
        return [key for key in keys if start_after is None or key > start_after]

    def list_prefixes(self, prefix: str = "") -> list[str]:
        base = self._path(prefix) if prefix else self.root
        if not base.is_dir():
            return []
        return sorted(item.name for item in base.iterdir() if item.is_dir())

    def child(self, name: str) -> VolumeStore:
        return VolumeStore(self.root / name)

    def list_json(self, prefix: str, *, start_after: str | None = None) -> list[tuple[str, Any]]:
        return [
            (key, self.get_json(key)) for key in self.list_keys(prefix, start_after=start_after)
        ]

    def commit_unit(
        self, sequence: int, unit_id: str, payload: dict[str, Any], pointer: dict[str, Any]
    ):
        payload_key = f"units/{unit_id}.json"
        self.put_json(payload_key, payload)
        document = {
            **pointer,
            "unit_id": unit_id,
            "payload_key": payload_key,
            "sequence": int(sequence),
        }
        self.put_json(f"_INDEX/{sequence:08d}-{unit_id}.json", document)
        return document

    def heartbeat(self, now: float | None = None) -> str:
        return self.put_json("_HEARTBEAT", {"epoch_s": time.time() if now is None else now})

    def heartbeat_age(self, now: float | None = None) -> float | None:
        if not self.exists("_HEARTBEAT"):
            return None
        current = time.time() if now is None else now
        return max(0.0, current - float(self.get_json("_HEARTBEAT")["epoch_s"]))

    def close_manifests(self, pairs: list[str], expected_units: int) -> str:
        return self.put_json(
            "manifest/_CLOSED", {"pairs": sorted(pairs), "expected_units": expected_units}
        )

    def close_row_check(self, pairs: list[str]) -> str:
        return self.put_json("row_check/_CLOSED", {"pairs": sorted(pairs)})

    def put_result(self, unit_id: str, result: dict[str, Any]) -> str:
        return self.put_json(f"results/{unit_id}.json", result)

    def put_dead(self, unit_id: str, failure: dict[str, Any]) -> str:
        return self.put_json(f"dead/{unit_id}.json", failure)

    def ready_unclaimed(self) -> int:
        done = len(self.list_keys("results/")) + len(self.list_keys("dead/"))
        return max(0, len(self.list_keys("_INDEX/")) - done)


# >>> pick the store from a location string: s3://... or a Volume / local directory path <<< #
def open_store(location: str, *, kms_key_id: str | None = None):
    if location.startswith("s3://"):
        return S3Store(location, kms_key_id=kms_key_id)
    return VolumeStore(location)


# >>> a run is finished when its Right side wrote _STATUS.json{state: complete} <<< #
def run_is_complete(store) -> bool:
    return (
        store.exists("_STATUS.json") and store.get_json("_STATUS.json").get("state") == "complete"
    )


# >>> resolve the run folder: an explicit run_id, a location that already is a run, or the newest unfinished run under a runs root <<< #
def resolve_run(location: str, run_id: str | None = None, *, kms_key_id: str | None = None):
    root = open_store(location, kms_key_id=kms_key_id)
    if run_id:
        return root.child(run_id), run_id
    if root.list_keys("row_check/") or root.exists("_STATUS.json"):
        return root, location.rstrip("/").rsplit("/", 1)[-1]
    candidates = [name for name in root.list_prefixes() if not run_is_complete(root.child(name))]
    if not candidates:
        return None, None
    return root.child(candidates[-1]), candidates[-1]
