from __future__ import annotations

import json
import time
from pathlib import Path, PurePosixPath
from typing import Any

from ...backends.store import S3Store, replace_local_file
from ..model.manifest import build_manifest
from ..model.protocol import (
    COL_CHECK_CLOSED,
    HEARTBEAT,
    LEFT_ROW_CHECK_CLOSED,
    ROW_CHECK_CLOSED,
    RUN_STATUS,
    UNITS_STATUS,
    fixed_keys_path,
    left_row_path,
    list_failures,
    list_results,
    list_units,
    pair_root,
    pair_segment,
    plan_path,
    row_result_path,
    timestamp,
)
from ..runtime.run_logging import get_left_logger
from ..sql.dates import resolve_date_type
from .row_check import STAGE_ONE_FIELDS, public_pair_config, run_row_check

log = get_left_logger("rollback")
STAGES = ("dead", "col", "row")
COMMIT_MARKERS = (
    ROW_CHECK_CLOSED,
    COL_CHECK_CLOSED,
    LEFT_ROW_CHECK_CLOSED,
    RUN_STATUS,
)
PAIR_PREFIXES = ("pairs/",)


# >>> a local run folder (dtrack_local, or a downloaded S3 run) with the few store calls a rollback needs <<< #
class LocalFolder:
    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root_uri = str(self.root)

    def exists(self, relative: str) -> bool:
        return (self.root / relative).is_file()

    def get_json(self, relative: str) -> Any:
        return json.loads((self.root / relative).read_text(encoding="utf-8"))

    def get_bytes(self, relative: str) -> bytes:
        return (self.root / relative).read_bytes()

    def put_bytes(self, relative: str, body: bytes, *, content_type: str = "") -> str:
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(path.name + ".part")
        try:
            temporary.write_bytes(body)
            replace_local_file(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)
        return relative

    def put_json(self, relative: str, value: Any) -> str:
        return self.put_bytes(
            relative,
            json.dumps(value, ensure_ascii=False, sort_keys=True).encode("utf-8"),
            content_type="application/json",
        )

    def list_keys(self, prefix: str) -> list[str]:
        folder = self.root / prefix
        if not folder.is_dir():
            return []
        return sorted(
            path.relative_to(self.root).as_posix() for path in folder.rglob("*") if path.is_file()
        )

    def list_json(self, prefix: str) -> list[tuple[str, Any]]:
        keys = [key for key in self.list_keys(prefix) if key.endswith(".json")]
        return [(key, self.get_json(key)) for key in keys]

    def heartbeat(self, now: float | None = None) -> str:
        return self.put_json(
            HEARTBEAT,
            {"epoch_s": time.time() if now is None else now, "updated_at": timestamp(self)},
        )


# >>> s3://... opens the S3 run folder, anything else a local run folder <<< #
def open_location(location: str):
    return S3Store(location) if location.startswith("s3://") else LocalFolder(location)


# >>> one prefix snapshot supports pair discovery and batch planning without per-pair rescans <<< #
def _documents(store, prefixes=PAIR_PREFIXES) -> dict[str, list[tuple[str, Any]]]:
    return {
        prefix: [
            (key, store.get_json(key)) for key in store.list_keys(prefix) if key.endswith(".json")
        ]
        for prefix in prefixes
    }


# >>> discover pair names from durable run documents, never from an unchecked CLI spelling <<< #
def discover_pairs(store, documents=None) -> list[str]:
    documents = documents or _documents(store)
    pairs = {
        document["pair"]
        for rows in documents.values()
        for _key, document in rows
        if isinstance(document, dict) and isinstance(document.get("pair"), str) and document["pair"]
    }
    return sorted(pairs)


def resolve_pairs(store, requested: list[str] | tuple[str, ...], documents=None) -> list[str]:
    names = [item.strip() for value in requested for item in value.split(",") if item.strip()]
    if not names:
        raise ValueError("at least one --pair is required")
    if "all" in names:
        if names != ["all"]:
            raise ValueError("--pair all cannot be combined with named pairs")
        available = discover_pairs(store, documents)
        if not available:
            raise ValueError("no pairs were found in this run")
        return available
    unique = list(dict.fromkeys(names))
    available = discover_pairs(store, documents)
    unknown = [name for name in unique if name not in available]
    if unknown:
        suffix = f"; available: {', '.join(available)}" if available else ""
        raise ValueError(f"unknown pair(s): {', '.join(unknown)}{suffix}")
    return unique


# >>> scan unit documents once, then build an exact plan for every selected pair <<< #
def plan_many(store, pairs: list[str], stage: str, documents=None) -> dict[str, list[str]]:
    if stage not in STAGES:
        raise ValueError(f"stage must be one of {', '.join(STAGES)}, got {stage!r}")
    selected = set(pairs)
    planned: dict[str, list[str]] = {pair: [] for pair in pairs}
    groups = [list_failures(store)]
    if stage != "dead":
        groups = [list_units(store), list_results(store), *groups]
    for rows in groups:
        for key, document in rows:
            pair = document.get("pair")
            if pair not in selected:
                continue
            planned[pair].append(key)
    if stage != "dead":
        report_keys = store.list_keys("report/")
        for pair in pairs:
            for key in (
                COL_CHECK_CLOSED,
                fixed_keys_path(pair),
                RUN_STATUS,
                UNITS_STATUS,
            ):
                if store.exists(key):
                    planned[pair].append(key)
            planned[pair].extend(store.list_keys(f"{pair_root(pair)}/col_check/right_ready/"))
            planned[pair].extend(report_keys)
    if stage == "row":
        for pair in pairs:
            for key in (
                ROW_CHECK_CLOSED,
                LEFT_ROW_CHECK_CLOSED,
                row_result_path(pair),
                left_row_path(pair),
                plan_path(pair),
                f"report/pairs/{pair_segment(pair)}/row_check.csv",
            ):
                if store.exists(key):
                    planned[pair].append(key)
    return {
        pair: sorted(
            set(keys),
            key=lambda key: (key not in COMMIT_MARKERS, key),
        )
        for pair, keys in planned.items()
    }


# >>> the objects to delete so `stage` of `pair` is produced again; commit markers first <<< #
def plan(store, pair: str, stage: str) -> list[str]:
    return plan_many(store, [pair], stage)[pair]


def reset_for_rerun(store, pairs: list[str], stage: str) -> list[str]:
    planned = plan_many(store, pairs, stage)
    keys = sorted({key for rows in planned.values() for key in rows})
    if not keys:
        return []
    store.delete_keys(keys)
    remaining = [key for key in keys if store.exists(key)]
    if remaining:
        raise RuntimeError(f"force reset left {len(remaining)} object(s): {remaining[:3]}")
    log.info("force reset %s stage for %s: %d object(s)", stage, pairs, len(keys))
    return keys


# >>> resolve a logical store key to the exact local path or S3 object that rollback will delete <<< #
def locate(store, key: str) -> dict[str, str]:
    logical = PurePosixPath(key)
    if not logical.parts or logical.is_absolute() or ".." in logical.parts or "\\" in key:
        raise ValueError(f"rollback target is outside the run folder: {key!r}")
    if hasattr(store, "client"):
        object_key = store._key(key)
        return {
            "key": key,
            "location": f"s3://{store.bucket}/{object_key}",
            "object_key": object_key,
        }
    root = Path(store.root).resolve()
    path = (root / Path(*logical.parts)).resolve()
    try:
        path.relative_to(root)
    except ValueError:
        raise ValueError(f"rollback target is outside the run folder: {key!r}") from None
    return {"key": key, "location": str(path)}


def _deletion(target: dict[str, str], status: str, error: str | None = None) -> dict[str, str]:
    row = {**target, "status": status}
    if error:
        row["error"] = error
    return row


def _target_exists(store, target: dict[str, str]) -> bool:
    if "object_key" in target:
        return store.exists(target["key"])
    return Path(target["location"]).is_file()


# >>> delete the frozen physical targets, then verify each one is absent instead of trusting a count <<< #
def delete(store, targets: list[dict[str, str]]) -> dict[str, Any]:
    before = {target["key"]: _target_exists(store, target) for target in targets}
    errors: dict[str, str] = {}
    if hasattr(store, "client"):
        existing = [target for target in targets if before[target["key"]]]
        for start in range(0, len(existing), 1000):
            batch = existing[start : start + 1000]
            response = store.client.delete_objects(
                Bucket=store.bucket,
                Delete={"Objects": [{"Key": target["object_key"]} for target in batch]},
            )
            for error in response.get("Errors") or []:
                errors[error["Key"]] = f"{error.get('Code', 'Error')}: {error.get('Message', '')}"
    else:
        for target in targets:
            if not before[target["key"]]:
                continue
            try:
                Path(target["location"]).unlink()
            except OSError as exc:
                errors[target["location"]] = str(exc)
    rows = []
    for target in targets:
        error = errors.get(target.get("object_key", target["location"]))
        if not before[target["key"]]:
            rows.append(_deletion(target, "missing"))
        elif error:
            rows.append(_deletion(target, "failed", error))
        elif _target_exists(store, target):
            rows.append(_deletion(target, "failed", "target still exists after delete"))
        else:
            rows.append(_deletion(target, "deleted"))
    counts = {
        status: sum(row["status"] == status for row in rows)
        for status in ("deleted", "missing", "failed")
    }
    log.info(
        "rollback at %s: deleted=%d missing=%d failed=%d",
        store.root_uri,
        counts["deleted"],
        counts["missing"],
        counts["failed"],
    )
    return {**counts, "deletions": rows}


# >>> the pair's new public config, refused when a Stage 1 setting differs from what was counted <<< #
def stage_two_config(store, config: dict[str, Any], pair: str) -> tuple[dict, dict]:
    left_doc = store.get_json(left_row_path(pair))
    public = public_pair_config(config["pairs"][pair])
    counted = left_doc["pair_config"]
    for side in ("left", "right"):
        stored = counted[side].get("date_type")
        if stored is None:
            continue
        configured = resolve_date_type(public[side].get("date_type"))
        concrete = resolve_date_type(stored)
        if configured.requires_probe and configured.name == concrete.name:
            public[side]["date_type"] = stored
    changed = [n for n in STAGE_ONE_FIELDS if public.get(n) != counted.get(n)]
    if changed:
        raise ValueError(
            f"{pair}: Stage 1 settings changed ({', '.join(changed)}); roll back with stage 'row'"
        )
    return left_doc, public


# >>> validate a row restart without opening a source; keep the original run identity <<< #
def prepare_row_check(store, config: dict[str, Any], pairs: list[str]) -> str:
    for pair in pairs:
        public_pair_config(config["pairs"][pair])
    run_ids = {store.get_json(left_row_path(pair)).get("run_id") for pair in pairs}
    if None in run_ids or len(run_ids) != 1:
        raise ValueError("selected pairs do not share one preserved run_id")
    return run_ids.pop()


# >>> build and validate a replacement entirely in memory before rollback deletes anything <<< #
def prepare_manifest(store, config: dict[str, Any], pair: str) -> dict[str, Any]:
    left_doc, public = stage_two_config(store, config, pair)
    verdict = store.get_json(row_result_path(pair))
    manifest = build_manifest(left_doc["run_id"], pair, public, verdict)
    return {
        "manifest": manifest,
        "left_doc": {**left_doc, "pair_config": public},
    }


# >>> publish the replacement and its matching preserved Stage 1 document after deletion succeeds <<< #
def publish_manifest(store, pair: str, prepared: dict[str, Any]) -> dict[str, Any]:
    manifest = prepared["manifest"]
    store.put_json(plan_path(pair), manifest)
    store.put_json(left_row_path(pair), prepared["left_doc"])
    log.info("%s rebuilt: %d units", plan_path(pair), manifest["expected_units"])
    return manifest


# >>> what to run after a rollback, per stage, for the distributed flow and for dtrack_local <<< #
def next_steps_many(
    stage: str, pairs: list[str], location: str, *, row_check_restarted: bool = False
) -> list[str]:
    pair_args = " ".join(f"--pair {pair}" for pair in pairs)
    local_pairs = repr(pairs)
    target = location.rstrip("/")
    run_root, run_id = target.rsplit("/", 1) if "/" in target else (".", target)
    run_args = f"--run-s3 {run_root} --run-id {run_id}"
    col = [
        (
            "distributed: restart the Databricks job with the same run_id, then "
            f"dtrack col-check {run_args} {pair_args}"
        ),
        f"dtrack_local: notebook 02 with PAIRS = {local_pairs} (or pipeline.col_check), then 03",
    ]
    if stage == "dead":
        return [
            "distributed: restart the Databricks job with the same run_id (dead units are retried)",
            "dtrack_local: notebook 02 (the Right side retries them), then 03",
        ]
    if stage == "col":
        return col
    if row_check_restarted:
        return col
    return [
        f"distributed: dtrack row-check --config c.json {run_args} {pair_args}",
        *col,
        f"dtrack_local: notebook 01 with PAIRS = {local_pairs} first",
    ]


# >>> freeze one deduplicated batch plan; delete only after every pair/config check succeeds <<< #
def rollback_many(
    store,
    pairs: list[str] | tuple[str, ...],
    stage: str,
    *,
    config: dict[str, Any] | None = None,
    apply: bool = False,
) -> dict[str, Any]:
    documents = _documents(store)
    pairs = resolve_pairs(store, pairs, documents)
    if config is not None and stage == "dead":
        raise ValueError("a config can only be used with stage 'col' or 'row'")
    if config is not None:
        missing = [pair for pair in pairs if pair not in config.get("pairs", {})]
        if missing:
            raise ValueError(f"config is missing pair(s): {', '.join(missing)}")
        inactive = [pair for pair in pairs if config["pairs"][pair].get("skip")]
        if inactive:
            raise ValueError(f"config skips selected pair(s): {', '.join(inactive)}")
        prepared = (
            {pair: prepare_manifest(store, config, pair) for pair in pairs}
            if stage == "col"
            else {}
        )
        row_run_id = prepare_row_check(store, config, pairs) if stage == "row" else None
    else:
        prepared = {}
        row_run_id = None
    by_pair = plan_many(store, pairs, stage, documents)
    keys = sorted(
        {key for pair_keys in by_pair.values() for key in pair_keys},
        key=lambda key: (key not in COMMIT_MARKERS, key),
    )
    targets = [locate(store, key) for key in keys]
    outcome: dict[str, Any] = {
        "pairs": pairs,
        "stage": stage,
        "planned": len(keys),
        "keys": keys,
        "targets": targets,
        "by_pair": by_pair,
    }
    if apply:
        outcome.update(delete(store, targets))
        if config is not None and not outcome["failed"]:
            if stage == "col":
                outcome["expected_units"] = {
                    pair: publish_manifest(store, pair, prepared[pair])["expected_units"]
                    for pair in pairs
                }
            else:
                run_id, documents = run_row_check(
                    config,
                    store,
                    run_id=row_run_id,
                    pair_names=pairs,
                )
                outcome["row_check"] = {"run_id": run_id, "pairs": len(documents)}
    outcome["next"] = next_steps_many(
        stage,
        pairs,
        store.root_uri,
        row_check_restarted=(
            config is not None and stage == "row" and (not apply or "row_check" in outcome)
        ),
    )
    return outcome


# >>> single-pair API kept for callers; the CLI uses rollback_many for repeated --pair/all <<< #
def rollback(
    store,
    pair: str,
    stage: str,
    *,
    config: dict[str, Any] | None = None,
    apply: bool = False,
) -> dict[str, Any]:
    outcome = rollback_many(store, [pair], stage, config=config, apply=apply)
    outcome["pair"] = pair
    if isinstance(outcome.get("expected_units"), dict):
        outcome["expected_units"] = outcome["expected_units"][pair]
    return outcome
