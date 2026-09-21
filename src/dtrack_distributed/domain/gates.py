from __future__ import annotations

from collections import Counter
from typing import Any

from ..engine.dates import apply_synonyms


def reconcile_partitions(
    left_counts: dict[str, int],
    right_counts: dict[str, int],
    date_synonyms: dict[str, str] | None = None,
) -> dict[str, dict[str, Any]]:
    left = apply_synonyms(left_counts, date_synonyms)
    right = apply_synonyms(right_counts, date_synonyms)
    result: dict[str, dict[str, Any]] = {}
    for partition in sorted(set(left) | set(right)):
        left_value = left.get(partition)
        right_value = right.get(partition)
        if left_value is None:
            verdict = "right_only"
        elif right_value is None:
            verdict = "left_only"
        elif int(left_value) == int(right_value):
            verdict = "match"
        else:
            verdict = "count_diff"
        row: dict[str, Any] = {
            "left": left_value,
            "right": right_value,
            "verdict": verdict,
        }
        if verdict == "count_diff":
            row["delta"] = int(right_value) - int(left_value)
        result[partition] = row
    return result


def summarize_partitions(partitions: dict[str, dict[str, Any]]) -> dict[str, int]:
    counts = Counter(row["verdict"] for row in partitions.values())
    return {name: counts[name] for name in ("match", "count_diff", "left_only", "right_only")}


def evaluate_gate(partitions: dict[str, dict[str, Any]], policy: str = "auto") -> dict[str, Any]:
    policy = (policy or "auto").strip().lower()
    total = len(partitions)
    matched = sum(row.get("verdict") == "match" for row in partitions.values())
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
