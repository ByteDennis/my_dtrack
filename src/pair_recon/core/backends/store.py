import io
import json
import posixpath
from typing import Any


class ApiVolumeStore:
    def __init__(self, root: str, *, files=None):
        if files is None:
            from databricks.sdk import WorkspaceClient

            files = WorkspaceClient().files
        self.root = root.rstrip("/")
        self.root_uri = self.root
        self.files = files

    def _path(self, relative: str) -> str:
        value = posixpath.normpath(relative.replace("\\", "/")).lstrip("/")
        if value in {"", ".", ".."} or value.startswith("../"):
            raise ValueError("path must stay below the store root")
        return f"{self.root}/{value}"

    def child(self, name: str):
        return type(self)(self._path(name), files=self.files)

    def put_bytes(self, relative: str, value: bytes, *, content_type: str = "") -> str:
        path = self._path(relative)
        self.files.create_directory(posixpath.dirname(path))
        self.files.upload(path, io.BytesIO(value), overwrite=True)
        return relative

    def put_json(self, relative: str, value: Any) -> str:
        body = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
        return self.put_bytes(relative, body, content_type="application/json")

    def get_bytes(self, relative: str) -> bytes:
        return self.files.download(self._path(relative)).contents.read()

    def get_json(self, relative: str) -> Any:
        return json.loads(self.get_bytes(relative).decode("utf-8"))

    def exists(self, relative: str) -> bool:
        try:
            self.files.get_metadata(self._path(relative))
            return True
        except Exception as exc:
            if isinstance(exc, FileNotFoundError) or getattr(exc, "status_code", None) == 404:
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
                if isinstance(exc, FileNotFoundError) or getattr(exc, "status_code", None) == 404:
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
            if isinstance(exc, FileNotFoundError) or getattr(exc, "status_code", None) == 404:
                return []
            raise

    def delete_keys(self, keys: list[str]) -> int:
        for key in keys:
            self.files.delete(self._path(key))
        return len(keys)

    def clear(self) -> int:
        keys = self.list_keys("")
        self.delete_keys(keys)
        return len(keys)
