import io
import json
import logging
import posixpath
import threading
import time
from copy import deepcopy
from typing import Any

import requests
from databricks.sdk.errors import NotFound
from urllib3.exceptions import HTTPError as Urllib3Error

log = logging.getLogger("pair_recon.store")
RETRY_ATTEMPTS = 5
RETRY_BACKOFF_S = 2.0
TRANSIENT_ERRORS = (
    requests.exceptions.ConnectionError,
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.Timeout,
    Urllib3Error,
    ConnectionError,
)


def _is_not_found(exc: Exception) -> bool:
    return (
        isinstance(exc, (FileNotFoundError, NotFound))
        or getattr(exc, "status_code", None) == 404
        or getattr(exc, "error_code", None) == "RESOURCE_DOES_NOT_EXIST"
    )


_FAILURE_STREAK = [0]


# >>> consecutive failed Files API attempts since the last success; the stage watchdog reads it <<< #
def volume_failure_streak() -> int:
    return _FAILURE_STREAK[0]


# >>> repeat a Files API call after a broken connection or timeout, backing off attempt * RETRY_BACKOFF_S <<< #
def retry_transient(operation: str, call, *, attempts: int = RETRY_ATTEMPTS):
    for attempt in range(1, attempts + 1):
        try:
            value = call()
            _FAILURE_STREAK[0] = 0
            return value
        except TRANSIENT_ERRORS as exc:
            _FAILURE_STREAK[0] += 1
            if attempt >= attempts:
                raise
            delay = RETRY_BACKOFF_S * attempt
            log.warning(
                "volume %s failed (%s: %s); retry %d/%d in %.0fs",
                operation,
                type(exc).__name__,
                exc,
                attempt,
                attempts - 1,
                delay,
            )
            time.sleep(delay)
    return None


class ApiVolumeStore:
    _IMMUTABLE_JSON = (
        "_protocol/units/",
        "_protocol/outcomes/matches/",
        "_protocol/outcomes/differences/",
        "_protocol/outcomes/failures/",
        "_protocol/windows/",
        "_protocol/row/left.json",
        "config/manifest.json",
        "config/sampled_keys.json",
    )

    def __init__(self, root: str, *, files=None, _json_cache=None):
        if files is None:
            from databricks.sdk import WorkspaceClient

            files = WorkspaceClient().files
        self.root = root.rstrip("/")
        self.root_uri = self.root
        self.files = files
        self._state_lock = threading.RLock()
        self._json_cache = {} if _json_cache is None else _json_cache

    def _path(self, relative: str) -> str:
        value = posixpath.normpath(relative.replace("\\", "/")).lstrip("/")
        if value in {"", ".", ".."} or value.startswith("../"):
            raise ValueError("path must stay below the store root")
        return f"{self.root}/{value}"

    def child(self, name: str):
        return type(self)(self._path(name), files=self.files, _json_cache=self._json_cache)

    @classmethod
    def _cacheable_json(cls, relative: str) -> bool:
        value = relative.replace("\\", "/").lstrip("/")
        return any(value == prefix or value.startswith(prefix) for prefix in cls._IMMUTABLE_JSON)

    def put_bytes(self, relative: str, value: bytes, *, content_type: str = "") -> str:
        path = self._path(relative)

        def upload():
            self.files.create_directory(posixpath.dirname(path))
            self.files.upload(path, io.BytesIO(value), overwrite=True)

        retry_transient(f"upload {relative}", upload)
        return relative

    def put_json(self, relative: str, value: Any) -> str:
        body = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
        result = self.put_bytes(relative, body, content_type="application/json")
        path = self._path(relative)
        with self._state_lock:
            if self._cacheable_json(relative):
                self._json_cache[path] = deepcopy(value)
            else:
                self._json_cache.pop(path, None)
        return result

    def get_bytes(self, relative: str) -> bytes:
        path = self._path(relative)
        return retry_transient(
            f"download {relative}", lambda: self.files.download(path).contents.read()
        )

    def get_json(self, relative: str) -> Any:
        path = self._path(relative)
        if self._cacheable_json(relative):
            with self._state_lock:
                if path in self._json_cache:
                    return deepcopy(self._json_cache[path])
        value = json.loads(self.get_bytes(relative).decode("utf-8"))
        if self._cacheable_json(relative):
            with self._state_lock:
                self._json_cache[path] = deepcopy(value)
        return deepcopy(value) if self._cacheable_json(relative) else value

    def _list_directory(self, path: str) -> list:
        return retry_transient(
            f"list {path}", lambda: list(self.files.list_directory_contents(path))
        )

    def _delete(self, path: str) -> None:
        retry_transient(f"delete {path}", lambda: self.files.delete(path))

    def exists(self, relative: str) -> bool:
        path = self._path(relative)
        try:
            retry_transient(f"metadata {relative}", lambda: self.files.get_metadata(path))
            return True
        except Exception as exc:
            if _is_not_found(exc):
                return False
            raise

    def list_keys(self, prefix: str, *, start_after: str | None = None) -> list[str]:
        base = self.root if not prefix else self._path(prefix)
        pending = [base]
        keys = []
        while pending:
            current = pending.pop()
            try:
                rows = self._list_directory(current)
            except Exception as exc:
                if _is_not_found(exc):
                    continue
                raise
            for row in rows:
                if row.is_directory:
                    pending.append(row.path)
                else:
                    keys.append(row.path.removeprefix(self.root + "/"))
        return sorted(key for key in keys if start_after is None or key > start_after)

    def list_prefixes(self, prefix: str = "") -> list[str]:
        base = self.root if not prefix else self._path(prefix)
        try:
            rows = self._list_directory(base)
            return sorted(
                posixpath.basename(row.path.rstrip("/")) for row in rows if row.is_directory
            )
        except Exception as exc:
            if _is_not_found(exc):
                return []
            raise

    def delete_keys(self, keys: list[str]) -> int:
        deleted = 0
        for key in keys:
            path = self._path(key)
            try:
                self._delete(path)
            except Exception as exc:
                if _is_not_found(exc):
                    with self._state_lock:
                        self._json_cache.pop(path, None)
                    continue
                raise
            with self._state_lock:
                self._json_cache.pop(path, None)
            deleted += 1
        return deleted

    def clear(self) -> int:
        keys = self.list_keys("")
        self.delete_keys(keys)
        return len(keys)
