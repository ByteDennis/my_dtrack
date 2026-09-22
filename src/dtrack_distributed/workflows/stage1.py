from __future__ import annotations

import hashlib
import json
import logging
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from typing import Any

from ..adapters.sources.factory import source_from_config

log = logging.getLogger("dtrack.stage1")
SECRET_FIELDS = {"user", "password", "access_token", "dsn", "server_hostname", "http_path"}


def make_run_id(config: dict[str, Any], *, now: datetime | None = None) -> str:
    now = now or datetime.now(UTC)
    digest = hashlib.sha256(
        json.dumps(config, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()[:6]
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{digest}"


# >>> pair config as the Right side needs it: no credentials, no connection endpoints <<< #
def public_pair_config(pair: dict[str, Any]) -> dict[str, Any]:
    result = {key: value for key, value in pair.items() if key not in ("left", "right")}
    for side in ("left", "right"):
        result[side] = {
            k: v for k, v in pair[side].items() if k not in SECRET_FIELDS and not k.endswith("_env")
        }
    return result


# >>> Stage 1, left half: preflight + exact per-Partition counts -> stage1/<pair>.left.json <<< #
def run_stage1_left(
    config: dict[str, Any],
    store,
    *,
    run_id: str | None = None,
    pair_names: list[str] | None = None,
    source_factory: Callable[[dict[str, Any]], Any] = source_from_config,
    pair_workers: int = 4,
) -> tuple[str, list[dict[str, Any]]]:
    run_id = run_id or make_run_id(config)
    pairs = {
        name: pair
        for name, pair in config["pairs"].items()
        if not pair.get("skip") and (not pair_names or name in pair_names)
    }
    if not pairs:
        raise ValueError("no active pairs selected")
    log.info("run_id=%s work=%s pairs=%s", run_id, store.root_uri, list(pairs))
    store.put_json("_STATUS.json", {"state": "left_counting", "run_id": run_id, "role": "left"})
    started = time.monotonic()
    sources = {name: source_factory(pair["left"]) for name, pair in pairs.items()}
    health = {name: source.preflight() for name, source in sources.items()}
    for name, row in health.items():
        log.info(
            "left preflight %s: %s", name, "OK" if row.get("ok") else f"FAIL {row.get('error')}"
        )
    failed = [name for name, row in health.items() if not row.get("ok")]
    if failed:
        raise RuntimeError(f"left preflight failed: {', '.join(failed)}")

    def count(name: str):
        pair = pairs[name]
        begun = time.monotonic()
        values = sources[name].partition_counts(pair.get("fromDate"), pair.get("toDate"))
        return values, round(time.monotonic() - begun, 6)

    with ThreadPoolExecutor(max_workers=max(1, pair_workers)) as pool:
        counted = {name: pool.submit(count, name) for name in pairs}
        counts = {name: future.result() for name, future in counted.items()}
    documents = []
    for name, pair in pairs.items():
        values, took = counts[name]
        log.info(
            "left count %s: %d partitions, %d rows (%.3fs)",
            name,
            len(values),
            sum(values.values()),
            took,
        )
        public = public_pair_config(pair)
        document = {
            "run_id": run_id,
            "pair": name,
            "left": public["left"],
            "right": public["right"],
            "bounds": {"from": pair.get("fromDate"), "to": pair.get("toDate")},
            "date_synonyms": dict(pair.get("date_synonyms") or {}),
            "counts": values,
            "pair_config": public,
            "preflight": health[name],
            "timing": {"left_count_s": took},
        }
        store.put_json(f"stage1/{name}.left.json", document)
        log.info("-> stage1/%s.left.json", name)
        documents.append(document)
    store.put_json("stage1/_LEFT_CLOSED", {"pairs": sorted(pairs), "run_id": run_id})
    store.heartbeat()
    store.put_json("_STATUS.json", {"state": "left_closed", "run_id": run_id, "role": "left"})
    log.info(
        "left side closed: %d pair(s) in %.3fs; waiting on the Right side for verdicts",
        len(pairs),
        time.monotonic() - started,
    )
    return run_id, documents
