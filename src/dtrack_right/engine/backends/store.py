from __future__ import annotations

import json
import os
import posixpath
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from ..common.model.protocol import (
    COL_CHECK_CLOSED,
    HEARTBEAT,
    ROW_CHECK_CLOSED,
    failure_path,
    get_run_status,
    list_failures,
    list_left_documents,
    list_results,
    list_units,
    result_path,
    timestamp,
    unit_path,
)

_WINDOWS_REPLACE_ERRORS = frozenset({5, 32})
_WINDOWS_REPLACE_RETRY_DELAYS = (0.01, 0.02, 0.04, 0.08, 0.16, 0.32, 0.64)


def replace_local_file(temporary: Path, path: Path) -> None:
    """Atomically replace a local file, tolerating brief Windows sharing locks."""
    for delay in (*_WINDOWS_REPLACE_RETRY_DELAYS, None):
        try:
            temporary.replace(path)
            return
        except PermissionError as exc:
            if delay is None or getattr(exc, "winerror", None) not in _WINDOWS_REPLACE_ERRORS:
                raise
            time.sleep(delay)


def _unit_pair(store, unit_id: str, document: dict[str, Any]) -> str:
    if document.get("pair"):
        return str(document["pair"])
    for _key, unit in list_units(store):
        if unit.get("unit_id") == unit_id and unit.get("pair"):
            return str(unit["pair"])
    raise ValueError(f"{unit_id}: result or failure does not identify its pair")


class _RunStoreBehavior:
    """Protocol documents shared by storage backends; subclasses provide byte/key primitives."""

    @staticmethod
    def json_bytes(value: Any) -> bytes:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()

    def put_json(self, relative: str, value: Any) -> str:
        return self.put_bytes(relative, self.json_bytes(value), content_type="application/json")

    def get_json(self, relative: str) -> Any:
        return json.loads(self.get_bytes(relative).decode())

    def list_json(self, prefix: str, *, start_after: str | None = None) -> list[tuple[str, Any]]:
        return [
            (key, self.get_json(key)) for key in self.list_keys(prefix, start_after=start_after)
        ]

    def commit_unit(
        self,
        sequence: int,
        unit_id: str,
        payload: dict[str, Any],
        pointer: dict[str, Any],
    ) -> dict[str, Any]:
        document = {
            **pointer,
            "unit_id": unit_id,
            "sequence": int(sequence),
            "left_ready_at": timestamp(self),
            "left_statistics": payload,
        }
        self.put_json(unit_path(document["pair"], sequence, unit_id), document)
        return document

    def heartbeat(self, now: float | None = None) -> str:
        return self.put_json(
            HEARTBEAT,
            {"epoch_s": time.time() if now is None else now, "updated_at": timestamp(self)},
        )

    def heartbeat_age(self, now: float | None = None) -> float | None:
        if not self.exists(HEARTBEAT):
            return None
        current = time.time() if now is None else now
        return max(0.0, current - float(self.get_json(HEARTBEAT)["epoch_s"]))

    def close_manifests(
        self,
        pairs: list[str],
        expected_units: int,
        plan_ids: dict[str, str | None] | None = None,
    ) -> str:
        return self.put_json(
            COL_CHECK_CLOSED,
            {
                "pairs": sorted(pairs),
                "expected_units": expected_units,
                "plan_ids": plan_ids or {},
                "closed_at": timestamp(self),
            },
        )

    def close_row_check(self, pairs: list[str]) -> str:
        return self.put_json(
            ROW_CHECK_CLOSED, {"pairs": sorted(pairs), "closed_at": timestamp(self)}
        )

    def put_result(self, unit_id: str, result: dict[str, Any]) -> str:
        pair = _unit_pair(self, unit_id, result)
        return self.put_json(
            result_path(pair, unit_id),
            {**result, "unit_id": unit_id, "pair": pair, "completed_at": timestamp(self)},
        )

    def put_dead(self, unit_id: str, failure: dict[str, Any]) -> str:
        pair = _unit_pair(self, unit_id, failure)
        return self.put_json(
            failure_path(pair, unit_id),
            {**failure, "unit_id": unit_id, "pair": pair, "failed_at": timestamp(self)},
        )

    def ready_unclaimed(self) -> int:
        ready = len(list_units(self))
        done = len(list_results(self)) + len(list_failures(self))
        return max(0, ready - done)


def validate_run_id(value: object) -> str:
    run_id = str(value or "")
    if not run_id or run_id in {".", ".."} or "/" in run_id or "\\" in run_id or "\0" in run_id:
        raise ValueError("run_id must be one non-empty folder name below the runs root")
    return run_id


@dataclass(frozen=True)
class S3Location:
    bucket: str
    prefix: str = ""

    @classmethod
    def parse(cls, uri: str) -> S3Location:
        parsed = urlparse(uri)
        if parsed.scheme != "s3" or not parsed.netloc:
            raise ValueError(f"expected s3://bucket/prefix, got {uri!r}")
        return cls(parsed.netloc, parsed.path.strip("/"))


class S3Store(_RunStoreBehavior):
    def __init__(self, root_uri: str, *, client=None):
        self.root_uri = root_uri
        self.location = S3Location.parse(root_uri)
        if client is None:
            import boto3
            from botocore.config import Config

            endpoint = os.getenv("DTRACK_S3_ENDPOINT_URL") or None
            kwargs: dict[str, Any] = {}
            if endpoint:
                kwargs.update(
                    endpoint_url=endpoint,
                    config=Config(s3={"addressing_style": "path"}),
                )
            client = boto3.client("s3", **kwargs)
        self.client = client

    @property
    def bucket(self) -> str:
        return self.location.bucket

    def _key(self, relative: str) -> str:
        relative = relative.lstrip("/")
        return posixpath.join(self.location.prefix, relative) if self.location.prefix else relative

    def _relative(self, key: str) -> str:
        prefix = f"{self.location.prefix}/" if self.location.prefix else ""
        return key.removeprefix(prefix)

    def put_bytes(self, relative: str, body: bytes, *, content_type: str) -> str:
        kwargs = {
            "Bucket": self.bucket,
            "Key": self._key(relative),
            "Body": body,
            "ContentType": content_type,
        }
        self.client.put_object(**kwargs)
        return kwargs["Key"]

    def get_bytes(self, relative: str) -> bytes:
        response = self.client.get_object(Bucket=self.bucket, Key=self._key(relative))
        return response["Body"].read()

    def exists(self, relative: str) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket, Key=self._key(relative))
            return True
        except Exception as exc:
            code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))
            if code in {"404", "NoSuchKey", "NotFound"} or "not found" in str(exc).lower():
                return False
            raise

    def list_keys(self, prefix: str, *, start_after: str | None = None) -> list[str]:
        kwargs: dict[str, Any] = {"Bucket": self.bucket, "Prefix": self._key(prefix)}
        if start_after:
            kwargs["StartAfter"] = self._key(start_after)
        result = []
        while True:
            response = self.client.list_objects_v2(**kwargs)
            result.extend(
                self._relative(row["Key"])
                for row in response.get("Contents") or []
                if not row["Key"].endswith("/")
            )
            if not response.get("IsTruncated"):
                return result
            kwargs.pop("StartAfter", None)
            kwargs["ContinuationToken"] = response["NextContinuationToken"]

    def list_prefixes(self, prefix: str = "") -> list[str]:
        base = self._key(prefix)
        base = base + "/" if base and not base.endswith("/") else base
        kwargs: dict[str, Any] = {"Bucket": self.bucket, "Prefix": base, "Delimiter": "/"}
        result = []
        while True:
            response = self.client.list_objects_v2(**kwargs)
            for row in response.get("CommonPrefixes") or []:
                result.append(row["Prefix"].removeprefix(base).strip("/"))
            if not response.get("IsTruncated"):
                return sorted(result)
            kwargs["ContinuationToken"] = response["NextContinuationToken"]

    def child(self, name: str) -> S3Store:
        return S3Store(self.root_uri.rstrip("/") + "/" + name, client=self.client)

    def delete_keys(self, keys: list[str]) -> int:
        deleted = 0
        for offset in range(0, len(keys), 1000):
            batch = keys[offset : offset + 1000]
            if not batch:
                continue
            response = self.client.delete_objects(
                Bucket=self.bucket,
                Delete={"Objects": [{"Key": self._key(key)} for key in batch], "Quiet": True},
            )
            errors = response.get("Errors") or []
            if errors:
                raise RuntimeError(f"failed to delete audit objects: {errors}")
            deleted += len(batch)
        return deleted


# >>> same contract as S3Store over a directory: a Unity Catalog Volume path or a local folder <<< #
class VolumeStore(_RunStoreBehavior):
    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root_uri = str(self.root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, relative: str) -> Path:
        return self.root / relative.lstrip("/")

    def put_bytes(self, relative: str, body: bytes, *, content_type: str = "") -> str:
        path = self._path(relative)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.part")
        try:
            temporary.write_bytes(body)
            replace_local_file(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)
        return relative

    def get_bytes(self, relative: str) -> bytes:
        return self._path(relative).read_bytes()

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

    def delete_keys(self, keys: list[str]) -> int:
        deleted = 0
        parents = set()
        for key in keys:
            path = self._path(key)
            if path.is_file():
                path.unlink()
                parents.add(path.parent)
                deleted += 1
        for parent in sorted(parents, key=lambda item: len(item.parts), reverse=True):
            if parent != self.root:
                try:
                    parent.rmdir()
                except OSError:
                    pass
        return deleted


# >>> pick the store from a location string: s3://... or a Volume / local directory path <<< #
def open_store(location: str):
    if location.startswith("s3://"):
        return S3Store(location)
    return VolumeStore(location)


# >>> a run is finished when its Right side wrote _status/run.json{state: complete} <<< #
def run_is_complete(store) -> bool:
    return get_run_status(store).get("state") == "complete"


# >>> resolve an explicit run_id or the newest unfinished run below the stable runs root <<< #
def resolve_run(location: str, run_id: str | None = None):
    root = open_store(location)
    if run_id:
        run_id = validate_run_id(run_id)
        return root.child(run_id), run_id
    candidates = []
    for name in root.list_prefixes():
        child = root.child(name)
        if list_left_documents(child) and not run_is_complete(child):
            candidates.append(name)
    if not candidates:
        return None, None
    return root.child(candidates[-1]), candidates[-1]
