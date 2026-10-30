from __future__ import annotations

import copy
import os
from typing import Any

from pair_recon_left.backends.sources import source_from_config
from pair_recon_left.cli.config import validate_config
from pair_recon_left.common.checks.row_check import public_pair_config
from pair_recon_left.common.model.manifest import build_manifest, key_family_specs

DEFAULT_TIMING_SECONDS = 120.0
MIN_TIMING_BATCHES = 3


def observation_seconds(value: object) -> float:
    text = str(value or "").strip()
    if not text:
        raise ValueError("PAIR_RECON_TIMING must be non-empty")
    if text.casefold() in {"true", "yes", "on"}:
        return DEFAULT_TIMING_SECONDS
    try:
        seconds = float(text)
    except ValueError:
        return DEFAULT_TIMING_SECONDS
    if seconds <= 0:
        raise ValueError("PAIR_RECON_TIMING seconds must be positive")
    return seconds


def _estimated_rows(raw_rows: int, sample: dict[str, Any]) -> int | None:
    if sample["mode"] == "all":
        return raw_rows
    if sample["mode"] == "fraction":
        return min(raw_rows, max(1, round(raw_rows * float(sample["rate"]))))
    return None


def _duration(seconds: float | None) -> str:
    if seconds is None:
        return "unknown"
    seconds = max(0, round(seconds))
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes}m"
    if minutes:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"


def _quantity(value: int) -> str:
    if value >= 1_000_000:
        return f"{value / 1_000_000:.2f}".rstrip("0").rstrip(".") + "M"
    if value >= 1_000:
        return f"{value / 1_000:.1f}".rstrip("0").rstrip(".") + "K"
    return str(value)


def _percentage(value: float) -> str:
    return f"{value * 100:.4f}".rstrip("0").rstrip(".") + "%"


def _project_seconds(rows: int, rate: float, measured: dict[str, Any]) -> float | None:
    if rate <= 0:
        return None
    startup = float(measured["execute_seconds"]) + float(measured["first_batch_seconds"])
    remaining = max(0, rows - int(measured["first_batch_rows"]))
    return startup + (remaining / rate)


def _candidate_values(current: float, *, minimum: float = 0.000001) -> list[float]:
    return list(dict.fromkeys(max(minimum, current / divisor) for divisor in (1, 2, 4)))


def _scenario_rows(
    total_raw: int,
    current_vintages: int,
    configured_rate: float,
    rate: float,
    vintages: int,
) -> int:
    if vintages == current_vintages and rate == configured_rate:
        return max(1, round(total_raw * rate))
    average = total_raw / max(1, current_vintages)
    return max(1, round(average * vintages * rate))


def left_pull_timing(
    settings: dict,
    config: dict,
    *,
    timing_value: object | None = None,
    source_factory=source_from_config,
    printer=print,
) -> dict[str, Any]:
    seconds = observation_seconds(
        os.getenv("PAIR_RECON_TIMING") if timing_value is None else timing_value
    )
    validated = validate_config(copy.deepcopy(config))
    pair = next(iter(validated["pairs"]))
    pair_config = validated["pairs"][pair]
    partition = "all" if pair_config.get("snapshot") else pair_config.get("partition", "day")
    source = source_factory({**pair_config["left"], "partition": partition})
    health = source.health_check()
    if not health.get("ok"):
        raise RuntimeError(f"Left health check failed: {health.get('error')}")
    detected = getattr(source, "config", {}).get("date_type")
    if detected is not None:
        pair_config["left"]["date_type"] = detected
    keys = [
        value.strip() for value in str(pair_config.get("key") or "").split(",") if value.strip()
    ]
    families = key_family_specs(pair_config, keys)
    counts = source.partition_counts(
        pair_config.get("fromDate"),
        pair_config.get("toDate"),
    )
    baseline = {
        "gate_decision": {"passed": True},
        "partitions": {
            value: {"left": count, "right": None, "result": "left_only"}
            for value, count in counts.items()
        },
    }
    manifest = build_manifest(settings["run_id"], pair, public_pair_config(pair_config), baseline)
    windows = manifest.get("windows") or []
    if not windows:
        raise RuntimeError(f"timing has no selected vintage for {pair}")
    configured_sample = manifest["sample"]
    if configured_sample["mode"] == "fixed":
        raise ValueError("PAIR_RECON_TIMING supports percentage or all sampling, not fixed-N")
    measured = max(windows, key=lambda value: int(value.get("n_left") or 0))
    sql, _columns = source.pull_window_sql(
        measured["partitions"],
        manifest["columns"],
        key_columns=manifest["key_cols"],
        sample_plan=configured_sample,
        key_families=families,
        side="left",
    )
    total_raw = sum(int(window.get("n_left") or 0) for window in windows)
    estimated_total = sum(
        _estimated_rows(int(window.get("n_left") or 0), configured_sample) or 0
        for window in windows
    )
    measured_estimate = _estimated_rows(int(measured.get("n_left") or 0), configured_sample)

    def progress(rows, elapsed, rate, batches):
        remaining = None if measured_estimate is None else max(0, measured_estimate - rows)
        eta = None if not rate or remaining is None else remaining / rate
        printer(
            "\r"
            f"Timing pull: {rate:,.1f} records/s | batch={batches} | "
            f"vintage={measured['bucket']} | rows={rows:,}/{measured_estimate or '?'} | "
            f"ETA={_duration(eta)}",
            end="",
            flush=True,
        )

    measured_rate = source.measure_query_rate(
        sql,
        seconds=seconds,
        min_batches=MIN_TIMING_BATCHES,
        reporter=progress,
    )
    printer()
    rate = float(measured_rate["records_per_second"] or 0)
    configured_rate = (
        float(configured_sample["rate"]) if configured_sample["mode"] == "fraction" else None
    )
    effective_rate = configured_rate if configured_rate is not None else 1.0
    total_eta = _project_seconds(estimated_total, rate, measured_rate)
    vintage_counts = sorted({1, max(1, len(windows) // 2), len(windows)})
    sample_rates = _candidate_values(effective_rate)
    scenarios = []
    for vintage_count in vintage_counts:
        for sample_rate in sample_rates:
            rows = _scenario_rows(
                total_raw,
                len(windows),
                effective_rate,
                sample_rate,
                vintage_count,
            )
            scenario_seconds = _project_seconds(rows, rate, measured_rate)
            scenarios.append(
                {
                    "vintages": vintage_count,
                    "sample_rate": sample_rate,
                    "estimated_rows": rows,
                    "estimated_seconds": None
                    if scenario_seconds is None
                    else round(scenario_seconds),
                    "estimated_time": _duration(scenario_seconds),
                }
            )
    report = {
        "pair": pair,
        "measured_vintage": measured["bucket"],
        "vintage_sample_size": len(windows),
        "configured_sample_rate": configured_rate,
        "source_rows": total_raw,
        "estimated_pull_rows": estimated_total,
        "records_per_second": round(rate, 1),
        "observed_rows": measured_rate["rows"],
        "observed_batches": measured_rate["batches"],
        "observed_seconds": round(float(measured_rate["observed_seconds"]), 1),
        "execute_seconds": round(float(measured_rate["execute_seconds"]), 1),
        "first_batch_seconds": round(float(measured_rate["first_batch_seconds"]), 1),
        "estimated_total_seconds": None if total_eta is None else round(total_eta),
        "estimated_total": _duration(total_eta),
        "completed_during_probe": bool(measured_rate["exhausted"]),
        "scenarios": scenarios,
    }
    printer(
        f"Timing | {report['records_per_second']:,.1f} rec/s | "
        f"exec {report['execute_seconds']:.1f}s | first {report['first_batch_seconds']:.1f}s | "
        f"{report['vintage_sample_size']} vintages | sample {_percentage(effective_rate)} | "
        f"pull ~{_quantity(estimated_total)} | ETA ~{report['estimated_total']}"
    )
    grouped = []
    for vintage_count in vintage_counts:
        items = [
            f"{_percentage(row['sample_rate'])}={_quantity(row['estimated_rows'])}/"
            f"{row['estimated_time']}"
            for row in scenarios
            if row["vintages"] == vintage_count
        ]
        grouped.append(f"{vintage_count}v: " + ", ".join(items))
    printer("Options | " + "; ".join(grouped) + "; approximate")
    return report
