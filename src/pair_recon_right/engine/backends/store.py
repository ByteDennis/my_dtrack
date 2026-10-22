import json
import os
import time
import uuid
from pathlib import Path

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
    unit_batch_document,
    unit_batch_path,
)


def validate_run_id(value):
    run_id = str(value or "")
    if not run_id or run_id in {".", ".."} or any(char in run_id for char in "/\\\0"):
        raise ValueError("run_id must be one non-empty folder name")
    return run_id


class VolumeStore:
    def __init__(self, root):
        self.root = Path(root)
        self.root_uri = str(self.root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, relative):
        value = Path(relative)
        if value.is_absolute() or ".." in value.parts:
            raise ValueError("path must stay below the store root")
        return self.root / value

    def child(self, name):
        return type(self)(self._path(name))

    def put_bytes(self, relative, body, *, content_type=""):
        path = self._path(relative)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".tmp-{uuid.uuid4().hex}.part")
        try:
            temporary.write_bytes(body)
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)
        return relative

    def put_json(self, relative, value):
        body = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
        return self.put_bytes(relative, body, content_type="application/json")

    def get_bytes(self, relative):
        return self._path(relative).read_bytes()

    def get_json(self, relative):
        return json.loads(self.get_bytes(relative).decode())

    def exists(self, relative):
        return self._path(relative).is_file()

    def list_keys(self, prefix, *, start_after=None):
        base = self._path(prefix) if prefix else self.root
        folder = base if base.is_dir() else base.parent
        if not folder.is_dir():
            return []
        keys = sorted(
            path.relative_to(self.root).as_posix()
            for path in folder.rglob("*")
            if path.is_file() and not path.name.endswith(".part")
        )
        return [
            key
            for key in keys
            if key.startswith(prefix) and (start_after is None or key > start_after)
        ]

    def list_json(self, prefix, *, start_after=None):
        return [
            (key, self.get_json(key)) for key in self.list_keys(prefix, start_after=start_after)
        ]

    def list_prefixes(self, prefix=""):
        base = self._path(prefix) if prefix else self.root
        return (
            sorted(path.name for path in base.iterdir() if path.is_dir()) if base.is_dir() else []
        )

    def delete_keys(self, keys):
        deleted = 0
        for key in keys:
            path = self._path(key)
            if path.is_file():
                path.unlink()
                deleted += 1
                parent = path.parent
                while parent != self.root and parent.is_dir() and not any(parent.iterdir()):
                    parent.rmdir()
                    parent = parent.parent
        return deleted

    def commit_units(self, sequence, units, *, timezone_name=None):
        document = unit_batch_document(
            sequence,
            units,
            left_ready_at=timestamp(timezone_name=timezone_name),
        )
        self.put_json(unit_batch_path(sequence, len(units)), document)
        return document["units"]

    def heartbeat(self, now=None):
        return self.put_json(
            HEARTBEAT,
            {"epoch_s": time.time() if now is None else now, "updated_at": timestamp(self)},
        )

    def heartbeat_age(self, now=None):
        if not self.exists(HEARTBEAT):
            return None
        current = time.time() if now is None else now
        return max(0.0, current - float(self.get_json(HEARTBEAT)["epoch_s"]))

    def close_manifests(self, pairs, expected_units, plan_ids=None):
        return self.put_json(
            COL_CHECK_CLOSED,
            {
                "pairs": sorted(pairs),
                "expected_units": expected_units,
                "plan_ids": plan_ids or {},
                "closed_at": timestamp(self),
            },
        )

    def close_row_check(self, pairs):
        return self.put_json(
            ROW_CHECK_CLOSED, {"pairs": sorted(pairs), "closed_at": timestamp(self)}
        )

    def put_result(self, unit_id, result):
        pair = result["pair"]
        return self.put_json(
            result_path(pair, unit_id),
            {**result, "unit_id": unit_id, "completed_at": timestamp(self)},
        )

    def put_dead(self, unit_id, failure):
        pair = failure["pair"]
        return self.put_json(
            failure_path(pair, unit_id),
            {**failure, "unit_id": unit_id, "failed_at": timestamp(self)},
        )

    def ready_unclaimed(self):
        return max(0, len(list_units(self)) - len(list_results(self)) - len(list_failures(self)))


def open_store(location):
    return VolumeStore(location)


def resolve_run(location, run_id=None):
    root = open_store(location)
    if run_id:
        run_id = validate_run_id(run_id)
        return root.child(run_id), run_id
    candidates = [
        name
        for name in root.list_prefixes()
        if list_left_documents(root.child(name))
        and get_run_status(root.child(name)).get("state") != "complete"
    ]
    return (root.child(candidates[-1]), candidates[-1]) if candidates else (None, None)
