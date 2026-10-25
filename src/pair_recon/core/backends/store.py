import io
import json
import posixpath
import threading
from copy import deepcopy
from typing import Any

from databricks.sdk.errors import NotFound


def _is_not_found(exc: Exception) -> bool:
    return (
        isinstance(exc, (FileNotFoundError, NotFound))
        or getattr(exc, "status_code", None) == 404
        or getattr(exc, "error_code", None) == "RESOURCE_DOES_NOT_EXIST"
    )


class ApiVolumeStore:
    _IMMUTABLE_JSON = (
        "units/",
        "col_check/result_batches/",
        "col_check/results/",
        "col_check/failures/",
        "col_check/right_ready/",
        "row_check/left.json",
        "plan.json",
        "fixed_keys.json",
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
        self.files.create_directory(posixpath.dirname(path))
        self.files.upload(path, io.BytesIO(value), overwrite=True)
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
        return self.files.download(self._path(relative)).contents.read()

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

    def exists(self, relative: str) -> bool:
        try:
            self.files.get_metadata(self._path(relative))
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
                rows = list(self.files.list_directory_contents(current))
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
            rows = self.files.list_directory_contents(base)
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
                self.files.delete(path)
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
