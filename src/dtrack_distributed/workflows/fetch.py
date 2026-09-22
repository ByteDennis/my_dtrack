from __future__ import annotations

import logging
from pathlib import Path

log = logging.getLogger("dtrack.fetch")


# >>> download one run's report set from the report store; latest/pointer.json picks the run when none is given <<< #
def fetch_report(report_store, out_dir: str | Path, *, run_id: str | None = None) -> dict:
    if run_id is None:
        if not report_store.exists("latest/pointer.json"):
            raise FileNotFoundError("report store has no latest/pointer.json yet")
        pointer = report_store.get_json("latest/pointer.json")
        run_id = pointer["run_id"]
        log.info(
            "latest/pointer.json -> run %s (%s, %s findings)",
            run_id,
            pointer.get("status"),
            pointer.get("findings"),
        )
    base = f"history/{run_id}/"
    keys = report_store.list_keys(base)
    if not keys:
        raise FileNotFoundError(f"no report under {base}")
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    files = {}
    for key in keys:
        name = key.removeprefix(base)
        target = out / name
        target.write_bytes(report_store.get_bytes(key))
        files[name] = target
        log.info("fetched %s -> %s (%d bytes)", key, target, target.stat().st_size)
    return {"run_id": run_id, "files": files}
