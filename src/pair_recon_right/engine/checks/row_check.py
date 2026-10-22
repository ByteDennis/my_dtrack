from __future__ import annotations

import csv
import io
import logging
import time
from collections import Counter
from typing import Any

from ..common.model.audit import SqlAudit, attach_sql_audit
from ..common.model.manifest import build_manifest, key_family_specs, mapping_csv_bytes, sample_plan
from ..common.model.protocol import (
    ROW_CHECK_CLOSED,
    artifact_location,
    left_row_check_close_document,
    list_left_documents,
    list_plans,
    list_row_results,
    mapping_path,
    plan_path,
    row_check_is_closed,
    row_result_path,
    write_right_state,
)
from ..common.sql.dates import apply_synonyms


def reconcile_partitions(
    left_counts: dict[str, int],
    right_counts: dict[str, int],
    date_synonyms: dict[str, str] | None = None,
    partition_mode: str = "date",
) -> dict[str, dict[str, Any]]:
    if partition_mode == "all":
        left, right = dict(left_counts), dict(right_counts)
    else:
        left = apply_synonyms(left_counts, date_synonyms)
        right = apply_synonyms(right_counts, date_synonyms)
    result: dict[str, dict[str, Any]] = {}
    for partition in sorted(set(left) | set(right)):
        left_value = left.get(partition)
        right_value = right.get(partition)
        if left_value is None:
            partition_result = "right_only"
        elif right_value is None:
            partition_result = "left_only"
        elif int(left_value) == int(right_value):
            partition_result = "match"
        else:
            partition_result = "count_diff"
        row: dict[str, Any] = {
            "left": left_value,
            "right": right_value,
            "result": partition_result,
        }
        if partition_result == "count_diff":
            row["delta"] = int(right_value) - int(left_value)
        result[partition] = row
    return result


def summarize_partitions(partitions: dict[str, dict[str, Any]]) -> dict[str, int]:
    counts = Counter(row["result"] for row in partitions.values())
    return {name: counts[name] for name in ("match", "count_diff", "left_only", "right_only")}


def reconcile_snapshot(
    left_counts: dict[str, int], right_counts: dict[str, int]
) -> dict[str, dict[str, Any]]:
    left = sum(int(value) for value in left_counts.values())
    if not right_counts:
        return {"snapshot": {"left": left, "right": None, "result": "left_only"}}
    matches = [key for key, value in right_counts.items() if int(value) == left]
    partition = max(matches or right_counts)
    right = int(right_counts[partition])
    row = {
        "left": left,
        "right": right,
        "result": "match" if left == right else "count_diff",
    }
    if left != right:
        row["delta"] = right - left
    return {partition: row}


def evaluate_gate(partitions: dict[str, dict[str, Any]], policy: str = "auto") -> dict[str, Any]:
    policy = (policy or "auto").strip().lower()
    total = len(partitions)
    matched = sum(row.get("result") == "match" for row in partitions.values())
    excluded_ratio = 0.0 if total == 0 else (total - matched) / total
    result = {
        "policy": policy,
        "excluded_ratio": excluded_ratio,
        "matched_partitions": matched,
        "total_partitions": total,
    }
    if policy == "manual":
        return {**result, "passed": False, "reason": "manual_review"}
    if policy == "auto":
        return {**result, "passed": matched > 0, "reason": "auto"}
    if policy.startswith("ratio:"):
        threshold = float(policy.split(":", 1)[1])
        if not 0 <= threshold <= 1:
            raise ValueError("ratio gate must be between 0 and 1")
        return {
            **result,
            "threshold": threshold,
            "passed": matched > 0 and excluded_ratio <= threshold,
            "reason": "ratio",
        }
    raise ValueError("gate must be auto, manual, or ratio:<0..1>")


log = logging.getLogger("pair_recon.right.row_check")
FINDINGS_SHOWN = 10


def row_check_csv_bytes(document: dict[str, Any]) -> bytes:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(
        output, fieldnames=["pair", "partition", "left", "right", "delta", "result"]
    )
    writer.writeheader()
    for partition, row in sorted(document["partitions"].items()):
        writer.writerow(
            {
                "pair": document["pair"],
                "partition": partition,
                "left": row.get("left"),
                "right": row.get("right"),
                "delta": row.get("delta", ""),
                "result": row["result"],
            }
        )
    return output.getvalue().encode()


def pending_left_documents(store) -> list[dict[str, Any]]:
    pending = []
    for _key, document in list_left_documents(store):
        if store.exists(row_result_path(document["pair"])):
            continue
        pending.append(document)
    return pending


def reconcile_pair(
    store,
    left_doc: dict[str, Any],
    target,
    *,
    gate: str = "auto",
    preflight_health: dict[str, Any] | None = None,
    preflight_s: float | None = None,
) -> dict[str, Any]:
    pair = left_doc["pair"]
    attach_sql_audit(
        target,
        SqlAudit(
            store,
            "right",
            pair=pair,
            enabled=bool(left_doc["pair_config"].get("audit")),
        ),
    )
    if preflight_health is None:
        started = time.monotonic()
        health = target.preflight()
        preflight_s = round(time.monotonic() - started, 6)
    else:
        health = dict(preflight_health)
        preflight_s = float(preflight_s or 0.0)
    if not health.get("ok"):
        log.error("preflight %s failed: %s", pair, health)
        raise RuntimeError(
            f"Right-side preflight failed for {pair} "
            f"(relation {health.get('relation')}): {health.get('error')}"
        )
    log.info(
        "preflight %s ok: relation=%s %d columns (%.3fs)",
        pair,
        health.get("relation"),
        len(health.get("columns") or ()),
        preflight_s,
    )
    right_date_type = getattr(target, "config", {}).get("date_type")
    if right_date_type is not None:
        left_doc["right"]["date_type"] = right_date_type
        left_doc["pair_config"]["right"]["date_type"] = right_date_type
    started = time.monotonic()
    pair_config = left_doc["pair_config"]
    options = {}
    if pair_config.get("pre_sampled") and not pair_config.get("right_pre_sampled"):
        keys = [
            value.strip() for value in str(pair_config.get("key") or "").split(",") if value.strip()
        ]
        if not keys:
            raise ValueError("pre-sampled CSV requires a pair key")
        options = {
            "sample": sample_plan(pair_config.get("sample")),
            "key_columns": [pair_config["col_map"].get(key, key) for key in keys],
            "key_families": key_family_specs(pair_config, keys),
        }
    right_counts = target.partition_counts(
        left_doc["bounds"].get("from"), left_doc["bounds"].get("to"), **options
    )
    right_s = round(time.monotonic() - started, 6)
    log.info(
        "right count %s: %d partitions, %d rows (%.3fs); first %s",
        pair,
        len(right_counts),
        sum(right_counts.values()),
        right_s,
        next(iter(sorted(right_counts.items())), None),
    )
    snapshot = bool(left_doc["pair_config"].get("snapshot"))
    partitions = (
        reconcile_snapshot(left_doc["counts"], right_counts)
        if snapshot
        else reconcile_partitions(
            left_doc["counts"],
            right_counts,
            left_doc.get("date_synonyms"),
            left_doc.get("partition_mode", "date"),
        )
    )
    document = {
        "run_id": left_doc["run_id"],
        "pair": pair,
        "left": left_doc["left"],
        "right": left_doc["right"],
        "bounds": left_doc["bounds"],
        "date_synonyms": dict(left_doc.get("date_synonyms") or {}),
        "partition_mode": left_doc.get("partition_mode", "date"),
        "partitions": partitions,
        "summary": summarize_partitions(partitions),
        "gate_decision": evaluate_gate(partitions, gate),
        "preflight": health,
        "timing": {
            "preflight_s": preflight_s,
            "left_count_s": left_doc.get("timing", {}).get("left_count_s"),
            "right_count_s": right_s,
        },
    }
    log.info("reconcile %s: %s", pair, document["summary"])
    findings = [(key, row) for key, row in sorted(partitions.items()) if row["result"] != "match"]
    for partition, row in findings[:FINDINGS_SHOWN]:
        log.info(
            "  finding %s %s %s left=%s right=%s",
            pair,
            partition,
            row["result"],
            row["left"],
            row["right"],
        )
    if len(findings) > FINDINGS_SHOWN:
        log.info("  ... %d more finding(s) in report/row_check.csv", len(findings) - FINDINGS_SHOWN)
    decision = document["gate_decision"]
    log.info(
        "gate %s: policy=%s passed=%s excluded_ratio=%.4f",
        pair,
        decision["policy"],
        decision["passed"],
        decision["excluded_ratio"],
    )
    store.put_bytes(
        "report/row_check.csv",
        row_check_csv_bytes(document),
        content_type="text/csv",
    )
    version = int(left_doc["pair_config"].get("version") or 1)
    column_checks = any(
        check != "row_count" for check in left_doc["pair_config"].get("checks") or ()
    )
    if (version == 1 and decision["passed"]) or (
        version == 2 and column_checks and not store.exists(plan_path(pair))
    ):
        manifest = build_manifest(left_doc["run_id"], pair, left_doc["pair_config"], document)
        store.put_bytes(
            mapping_path(pair),
            mapping_csv_bytes(left_doc["pair_config"]),
            content_type="text/csv; charset=utf-8",
        )
        store.put_json(plan_path(pair), manifest)
        log.info(
            "plan released: %s (%d windows, %d columns, %d units)",
            artifact_location(store, plan_path(pair)),
            len(manifest["windows"]),
            len(manifest["columns"]),
            manifest["expected_units"],
        )
    store.put_json(row_result_path(pair), document)
    return document


def unresolved_version2_partitions(store) -> dict[str, list[str]]:

    plans = {plan["pair"]: plan for _key, plan in list_plans(store)}
    results = {result["pair"]: result for _key, result in list_row_results(store)}
    unresolved = {}
    for pair, plan in plans.items():
        if int(plan.get("version") or 1) != 2 or pair not in results:
            continue
        synonyms = results[pair].get("date_synonyms") or {}
        excluded = {synonyms.get(value, value) for value in plan.get("excluded_partitions") or []}
        mismatches = sorted(
            partition
            for partition, row in results[pair]["partitions"].items()
            if row.get("result") != "match" and partition not in excluded
        )
        if mismatches:
            unresolved[pair] = mismatches
    return unresolved


def require_version2_exclusions(store) -> None:
    unresolved = unresolved_version2_partitions(store)
    if not unresolved:
        return
    plans = {plan["pair"]: plan for _key, plan in list_plans(store)}
    all_mode = sorted(
        pair for pair in unresolved if (plans.get(pair) or {}).get("partition_mode") == "all"
    )
    if all_mode:
        raise RuntimeError(
            "partition_mode all row counts differ; align the complete Left and Right relations "
            f"before continuing ({', '.join(all_mode)})"
        )
    details = "; ".join(f"{pair}: {', '.join(values)}" for pair, values in unresolved.items())
    raise RuntimeError(
        "version 2 row-check found unexcluded mismatched partitions; "
        f"add them to excluded_partitions, rerun the Left job with the same run-id, "
        f"then restart Right ({details})"
    )


def run_row_check(
    store,
    target_factory,
    *,
    gate: str = "auto",
    targets: dict[str, Any] | None = None,
    preflight_health: dict[str, dict[str, Any]] | None = None,
    preflight_seconds: dict[str, float] | None = None,
) -> int:
    store.delete_keys([ROW_CHECK_CLOSED])
    write_right_state(
        store,
        {"phase": "row_check", "row_check_closed_at": None},
    )
    processed = 0
    pending = pending_left_documents(store)
    audit_enabled = any(bool(document["pair_config"].get("audit")) for document in pending)
    pending_pairs = {document["pair"] for document in pending}
    if targets is not None:
        missing_targets = sorted(pending_pairs - set(targets))
        if missing_targets:
            raise ValueError(f"preflight targets missing pair(s): {', '.join(missing_targets)}")
    if preflight_health is not None:
        missing_health = sorted(pending_pairs - set(preflight_health))
        if missing_health:
            raise ValueError(f"preflight health missing pair(s): {', '.join(missing_health)}")
    for left_doc in pending:
        pair = left_doc["pair"]
        target = (
            targets[pair]
            if targets is not None
            else target_factory(
                {
                    **left_doc["right"],
                    "partition_mode": left_doc.get("partition_mode", "date"),
                }
            )
        )
        reconcile_pair(
            store,
            left_doc,
            target,
            gate=gate,
            preflight_health=preflight_health[pair] if preflight_health is not None else None,
            preflight_s=(preflight_seconds or {}).get(pair),
        )
        processed += 1
    closed = left_row_check_close_document(store)
    if closed and not row_check_is_closed(store):
        pairs = closed["pairs"]
        if all(store.exists(row_result_path(pair)) for pair in pairs):
            store.close_row_check(pairs)
            released = [pair for pair in pairs if store.exists(plan_path(pair))]
            log.info("row_check closed: %d/%d pairs released", len(released), len(pairs))
    if processed:
        SqlAudit(store, "right", enabled=audit_enabled).refresh()
    require_version2_exclusions(store)
    return processed
