import os
import time
from pathlib import Path

from pair_recon.core.backends.store import ApiVolumeStore

from ..common.model.protocol import (
    COL_CHECK_CLOSED,
    HEARTBEAT,
    ROW_CHECK_CLOSED,
    failure_path,
    list_failures,
    list_results,
    list_units,
    result_path,
    timestamp,
    unit_batch_document,
    unit_batch_path,
)

_WINDOWS_ERRORS = {5, 32}
_RETRY_DELAYS = (0.01, 0.02, 0.04, 0.08, 0.16, 0.32, 0.64)


def replace_local_file(temporary: Path, path: Path) -> None:
    for delay in (*_RETRY_DELAYS, None):
        try:
            os.replace(temporary, path)
            return
        except PermissionError as exc:
            if delay is None or getattr(exc, "winerror", None) not in _WINDOWS_ERRORS:
                raise
            time.sleep(delay)


def validate_run_id(value: object) -> str:
    run_id = str(value or "")
    if not run_id or run_id in {".", ".."} or any(char in run_id for char in "/\\\0"):
        raise ValueError("run_id must be one non-empty folder name")
    return run_id


class VolumeStore(ApiVolumeStore):
    def list_json(self, prefix: str, *, start_after: str | None = None):
        return [
            (key, self.get_json(key)) for key in self.list_keys(prefix, start_after=start_after)
        ]

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
