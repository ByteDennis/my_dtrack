from __future__ import annotations

import json
import logging
from pathlib import Path, PurePosixPath
from typing import Any

from ..backends.store import S3Store
from ..common.manifest import build_manifest
from .row_check import public_pair_config

log = logging.getLogger("dtrack.left.rollback")
STAGES = ("dead", "col", "row")
STAGE_ONE_FIELDS = ("left", "right", "fromDate", "toDate", "date_synonyms")
COMMIT_MARKERS = ("row_check/_CLOSED", "manifest/_CLOSED", "row_check/_LEFT_CLOSED", "_STATUS.json")


# >>> a local run folder (dtrack_local, or a downloaded S3 run) with the few store calls a rollback needs <<< #
class LocalFolder:
    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root_uri = str(self.root)

    def exists(self, relative: str) -> bool:
        return (self.root / relative).is_file()

    def get_json(self, relative: str) -> Any:
        return json.loads((self.root / relative).read_text(encoding="utf-8"))

    def put_json(self, relative: str, value: Any) -> str:
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True), encoding="utf-8")
        return relative

    def list_json(self, prefix: str) -> list[tuple[str, Any]]:
        folder = self.root / prefix
        if not folder.is_dir():
            return []
        keys = sorted(p.relative_to(self.root).as_posix() for p in folder.rglob("*.json"))
        return [(key, self.get_json(key)) for key in keys]


# >>> s3://... opens the S3 run folder, anything else a local run folder <<< #
def open_location(location: str):
    return S3Store(location) if location.startswith("s3://") else LocalFolder(location)


# >>> every Stage 2 object that belongs to one pair: index pointers, their payloads, results, dead units <<< #
def _pair_units(store, pair: str) -> list[str]:
    keys = []
    for prefix in ("_INDEX/", "results/", "dead/"):
        for key, document in store.list_json(prefix):
            if document.get("pair") == pair:
                keys.append(key)
                if prefix == "_INDEX/" and document.get("payload_key"):
                    keys.append(document["payload_key"])
    return keys


# >>> the objects to delete so `stage` of `pair` is produced again; commit markers first <<< #
def plan(store, pair: str, stage: str) -> list[str]:
    if stage not in STAGES:
        raise ValueError(f"stage must be one of {', '.join(STAGES)}, got {stage!r}")
    if stage == "dead":
        keys = [key for key, document in store.list_json("dead/") if document.get("pair") == pair]
    else:
        keys = [*_pair_units(store, pair), "manifest/_CLOSED", f"manifest/{pair}.keys.json"]
        keys.append("_STATUS.json")
    if stage == "row":
        keys += [
            "row_check/_CLOSED",
            "row_check/_LEFT_CLOSED",
            f"row_check/{pair}.json",
            f"row_check/{pair}.left.json",
            f"reports/{pair}_compare_row.csv",
            f"manifest/{pair}.json",
        ]
    present = {key for key in keys if store.exists(key)}
    return sorted(present, key=lambda key: (key not in COMMIT_MARKERS, key))


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
    left_doc = store.get_json(f"row_check/{pair}.left.json")
    public = public_pair_config(config["pairs"][pair])
    changed = [n for n in STAGE_ONE_FIELDS if public.get(n) != left_doc["pair_config"].get(n)]
    if changed:
        raise ValueError(
            f"{pair}: Stage 1 settings changed ({', '.join(changed)}); roll back with stage 'row'"
        )
    return left_doc, public


# >>> Stage 2 settings changed (col_map, sample, key, vintage, ...): rebuild the manifest from the kept Stage 1 verdict <<< #
def rebuild_manifest(store, config: dict[str, Any], pair: str) -> dict[str, Any]:
    left_doc, public = stage_two_config(store, config, pair)
    verdict = store.get_json(f"row_check/{pair}.json")
    manifest = build_manifest(left_doc["run_id"], pair, public, verdict)
    store.put_json(f"manifest/{pair}.json", manifest)
    store.put_json(f"row_check/{pair}.left.json", {**left_doc, "pair_config": public})
    log.info("manifest/%s.json rebuilt: %d units", pair, manifest["expected_units"])
    return manifest


# >>> what to run after a rollback, per stage, for the distributed flow and for dtrack_local <<< #
def next_steps(stage: str, pair: str, location: str) -> list[str]:
    col = [
        (
            "distributed: restart the Databricks job with the same run_id, then "
            f"dtrack col-check --s3 {location} --pair {pair}"
        ),
        f"dtrack_local: notebook 02 with PAIRS = [{pair!r}] (or pipeline.col_check), then 03",
    ]
    if stage == "dead":
        return [
            "distributed: restart the Databricks job with the same run_id (dead units are retried)",
            "dtrack_local: notebook 02 (the Right side retries them), then 03",
        ]
    if stage == "col":
        return col
    return [
        f"distributed: dtrack row-check --config c.json --s3 {location} --pair {pair}",
        *col,
        f"dtrack_local: notebook 01 with PAIRS = [{pair!r}] first",
    ]


# >>> dry run unless apply: the plan, and with apply the deletion plus an optional manifest rebuild <<< #
def rollback(
    store,
    pair: str,
    stage: str,
    *,
    config: dict[str, Any] | None = None,
    apply: bool = False,
) -> dict[str, Any]:
    if config is not None and stage != "col":
        raise ValueError("a config only rebuilds the manifest of stage 'col'")
    if config is not None:
        stage_two_config(store, config, pair)
    keys = plan(store, pair, stage)
    targets = [locate(store, key) for key in keys]
    outcome: dict[str, Any] = {
        "pair": pair,
        "stage": stage,
        "planned": len(keys),
        "keys": keys,
        "targets": targets,
    }
    if apply:
        outcome.update(delete(store, targets))
        if config is not None and not outcome["failed"]:
            outcome["expected_units"] = rebuild_manifest(store, config, pair)["expected_units"]
    outcome["next"] = next_steps(stage, pair, store.root_uri)
    return outcome
