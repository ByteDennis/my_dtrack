from __future__ import annotations

import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from ...backends.sources import source_from_config
from ..model.audit import SqlAudit, attach_sql_audit
from ..model.manifest import build_manifest, key_family_specs, mapping_csv_bytes, sample_plan
from ..model.protocol import (
    LEFT_ROW_CHECK_CLOSED,
    artifact_location,
    left_row_path,
    mapping_path,
    plan_path,
    timestamp,
    write_run_status,
)
from ..runtime.run_logging import get_left_logger
from ..sql.dates import resolve_date_type

log = get_left_logger("row_check")
SECRET_FIELDS = {"user", "password", "access_token", "dsn", "server_hostname", "http_path"}
STAGE_ONE_FIELDS = (
    "left",
    "right",
    "partition_mode",
    "fromDate",
    "toDate",
    "date_synonyms",
    "sample",
    "pre_sampled",
    "right_pre_sampled",
    "audit",
    "key",
    "key_families",
)


def public_pair_config(pair: dict[str, Any]) -> dict[str, Any]:
    result = {key: value for key, value in pair.items() if key not in ("left", "right")}
    for side in ("left", "right"):
        result[side] = {
            k: v for k, v in pair[side].items() if k not in SECRET_FIELDS and not k.endswith("_env")
        }
        if "date_type" in result[side]:
            result[side]["date_type"] = resolve_date_type(result[side]["date_type"]).spec(
                portable=True
            )
    return result


def active_pairs(
    config: dict[str, Any], pair_names: list[str] | None = None
) -> dict[str, dict[str, Any]]:
    pairs = {
        name: pair
        for name, pair in config["pairs"].items()
        if not pair.get("skip") and (not pair_names or name in pair_names)
    }
    if not pairs:
        raise ValueError("no active pairs selected")
    if len(pairs) != 1:
        raise ValueError("exactly one active pair is required per run")
    return pairs


def reuse_row_check(
    config: dict[str, Any],
    store,
    *,
    run_id: str,
    pair_names: list[str] | None = None,
) -> tuple[str, list[dict[str, Any]]] | None:
    pairs = active_pairs(config, pair_names)
    cached = {}
    for name, pair in pairs.items():
        path = left_row_path(name)
        if not store.exists(path):
            return None
        document = store.get_json(path)
        if document.get("run_id") != run_id or "counts" not in document:
            return None
        current = public_pair_config(pair)
        counted = document.get("pair_config") or {}
        for side in ("left", "right"):
            stored = (counted.get(side) or {}).get("date_type")
            configured = (current.get(side) or {}).get("date_type")
            if stored is None or configured is None:
                continue
            requested = resolve_date_type(configured)
            concrete = resolve_date_type(stored)
            if requested.requires_probe and requested.name == concrete.name:
                pair[side]["date_type"] = stored
                current[side]["date_type"] = stored
        if any(current.get(field) != counted.get(field) for field in STAGE_ONE_FIELDS):
            return None
        cached[name] = {
            "counts": document["counts"],
            "preflight": document.get("preflight") or {"ok": True, "reused": True},
            "timing": document.get("timing") or {"left_count_s": None},
        }
    log.info("reuse Left row check for run_id=%s pairs=%s", run_id, list(pairs))
    return run_id, publish_row_check_results(store, run_id, pairs, cached)


def publish_row_check_results(
    store,
    run_id: str,
    pairs: dict[str, dict[str, Any]],
    results: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    missing = sorted(set(pairs) - set(results))
    if missing:
        raise ValueError(f"row-check results missing pair(s): {', '.join(missing)}")
    documents = []
    for name, pair in pairs.items():
        result = results[name]
        values = result["counts"]
        took = result.get("timing", {}).get("left_count_s")
        public = public_pair_config(pair)
        document = {
            "run_id": run_id,
            "pair": name,
            "left": public["left"],
            "right": public["right"],
            "bounds": {"from": pair.get("fromDate"), "to": pair.get("toDate")},
            "date_synonyms": dict(pair.get("date_synonyms") or {}),
            "partition_mode": public.get("partition_mode", "date"),
            "counts": values,
            "pair_config": public,
            "preflight": result["preflight"],
            "timing": {"left_count_s": took},
        }
        store.put_json(left_row_path(name), document)
        log.info("-> %s", left_row_path(name))
        if (
            int(public.get("version") or 1) == 2
            and not public.get("snapshot")
            and any(check != "row_count" for check in public.get("checks") or ())
        ):
            baseline = {
                "gate_decision": {"passed": True},
                "partitions": {
                    partition: {"left": count, "right": None, "result": "left_only"}
                    for partition, count in values.items()
                },
            }
            plan = build_manifest(run_id, name, public, baseline)
            store.put_bytes(
                mapping_path(name),
                mapping_csv_bytes(public),
                content_type="text/csv; charset=utf-8",
            )
            store.put_json(plan_path(name), plan)
            log.info(
                "plan released: %s (%d windows, %d columns, %d units)",
                artifact_location(store, plan_path(name)),
                len(plan["windows"]),
                len(plan["columns"]),
                plan["expected_units"],
            )
        documents.append(document)
    store.put_json(
        LEFT_ROW_CHECK_CLOSED,
        {"pairs": sorted(pairs), "run_id": run_id, "closed_at": timestamp(store)},
    )
    store.heartbeat()
    write_run_status(store, {"state": "left_row_check_closed", "run_id": run_id, "role": "left"})
    return documents


def run_row_check(
    config: dict[str, Any],
    store,
    *,
    run_id: str,
    pair_names: list[str] | None = None,
    source_factory: Callable[[dict[str, Any]], Any] = source_from_config,
    pair_workers: int = 4,
    sources: dict[str, Any] | None = None,
    preflight_health: dict[str, dict[str, Any]] | None = None,
) -> tuple[str, list[dict[str, Any]]]:
    pairs = active_pairs(config, pair_names)
    log.info("run_id=%s work=%s pairs=%s", run_id, store.root_uri, list(pairs))
    write_run_status(
        store,
        {"state": "left_row_check", "run_id": run_id, "role": "left"},
        timezone_name=config.get("timezone"),
    )
    started = time.monotonic()
    if sources is None:
        sources = {
            name: source_factory(
                {
                    **pair["left"],
                    "partition_mode": "all"
                    if pair.get("snapshot")
                    else pair.get("partition_mode", "date"),
                }
            )
            for name, pair in pairs.items()
        }
    missing_sources = sorted(set(pairs) - set(sources))
    if missing_sources:
        raise ValueError(f"preflight sources missing pair(s): {', '.join(missing_sources)}")
    sources = {
        name: attach_sql_audit(
            sources[name],
            SqlAudit(store, "left", pair=name, enabled=bool(pairs[name].get("audit"))),
        )
        for name in pairs
    }
    if preflight_health is None:
        health = {name: source.preflight() for name, source in sources.items()}
    else:
        missing_health = sorted(set(pairs) - set(preflight_health))
        if missing_health:
            raise ValueError(f"preflight health missing pair(s): {', '.join(missing_health)}")
        health = {name: dict(preflight_health[name]) for name in pairs}
    for name, row in health.items():
        log.info(
            "left preflight %s: %s", name, "OK" if row.get("ok") else f"FAIL {row.get('error')}"
        )
    failed = [name for name, row in health.items() if not row.get("ok")]
    if failed:
        raise RuntimeError(f"left preflight failed: {', '.join(failed)}")
    for name, source in sources.items():
        detected = getattr(source, "config", {}).get("date_type")
        if detected is not None:
            pairs[name]["left"]["date_type"] = detected

    def count(name: str):
        pair = pairs[name]
        begun = time.monotonic()
        options = {}
        if pair.get("right_pre_sampled") and not pair.get("pre_sampled"):
            keys = [
                value.strip() for value in str(pair.get("key") or "").split(",") if value.strip()
            ]
            options = {
                "sample": sample_plan(pair.get("sample")),
                "key_columns": keys,
                "key_families": key_family_specs(pair, keys),
            }
        values = sources[name].partition_counts(pair.get("fromDate"), pair.get("toDate"), **options)
        return values, round(time.monotonic() - begun, 6)

    with ThreadPoolExecutor(max_workers=max(1, pair_workers)) as pool:
        counted = {name: pool.submit(count, name) for name in pairs}
        counts = {name: future.result() for name, future in counted.items()}
    results = {}
    for name in pairs:
        values, took = counts[name]
        log.info(
            "left count %s: %d partitions, %d rows (%.3fs)",
            name,
            len(values),
            sum(values.values()),
            took,
        )
        results[name] = {
            "counts": values,
            "preflight": health[name],
            "timing": {"left_count_s": took},
        }
    documents = publish_row_check_results(store, run_id, pairs, results)
    if all(int(pair.get("version") or 1) == 2 for pair in pairs.values()):
        log.info(
            "left row check closed: %d pair(s) in %.3fs; version 2 plans are ready for col-check",
            len(pairs),
            time.monotonic() - started,
        )
    else:
        log.info(
            "left side closed: %d pair(s) in %.3fs; waiting on the Right side for row-check verdicts",
            len(pairs),
            time.monotonic() - started,
        )
    SqlAudit(
        store, "left", enabled=any(bool(pair.get("audit")) for pair in pairs.values())
    ).refresh()
    return run_id, documents
