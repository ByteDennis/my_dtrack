"""Live correctness and step-timing contracts for Oracle, Athena, and Hadoop.

Create an ignored ``tests/live/source_queries.json`` (or point
``DTRACK_LIVE_SOURCE_QUERIES`` at one) with one entry per source::

    {
      "oracle": {"config": {"source": "oracle", "...": "..."}, "query": "SELECT ..."},
      "athena": {"config": {"source": "athena", "...": "..."}, "query": "SELECT ..."},
      "hadoop": {"config": {"source": "hadoop", "dsn": "name"}, "query": "SELECT ..."}
    }

Credentials stay in environment variables, driver configuration, or the ignored file.

Run ``just live profile`` for step timings. Reports use UTF-8 and are written under
``.dtrack-work/live-timings`` unless ``DTRACK_LIVE_TIMINGS_DIR`` overrides it. Set
``DTRACK_LIVE_PROFILE_RUNS`` to repeat each source and ``DTRACK_LIVE_PROFILE_FETCH_SIZE``
to tune fetch batches.
"""

from __future__ import annotations

import hashlib
import json
import locale
import os
import platform
import sys
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter_ns
from typing import Any

import pandas as pd
import pytest

from dtrack_left.backends.sources import source_from_config

SOURCE_NAMES = ("oracle", "athena", "hadoop")
DEFAULT_CONFIG = Path(__file__).with_name("source_queries.json")
DEFAULT_TIMINGS_DIR = Path(".dtrack-work") / "live-timings"


def _case(name: str) -> tuple[dict, str]:
    path = Path(os.environ.get("DTRACK_LIVE_SOURCE_QUERIES", DEFAULT_CONFIG))
    if not path.is_file():
        pytest.skip(f"set DTRACK_LIVE_SOURCE_QUERIES or create {DEFAULT_CONFIG}")
    document = json.loads(path.read_text(encoding="utf-8-sig"))
    case = document.get(name)
    if case is None:
        pytest.skip(f"add {name!r} to {path}")
    config = case.get("config")
    query = case.get("query")
    if not isinstance(config, dict) or config.get("source") != name:
        raise ValueError(f"{path}: {name}.config.source must be {name!r}")
    if not isinstance(query, str) or not query.strip():
        raise ValueError(f"{path}: {name}.query must be non-empty SQL")
    return config, query


def _text_frame(frame: pd.DataFrame) -> pd.DataFrame:
    return frame.astype("string").fillna("")


def _positive_int(name: str, default: int) -> int:
    raw = os.environ.get(name, str(default))
    try:
        value = int(raw)
    except ValueError:
        raise ValueError(f"{name} must be a positive integer, got {raw!r}") from None
    if value < 1:
        raise ValueError(f"{name} must be a positive integer, got {raw!r}")
    return value


@contextmanager
def _measure(steps: list[dict[str, Any]], name: str):
    started = perf_counter_ns()
    status = "ok"
    try:
        yield
    except BaseException:
        status = "failed"
        raise
    finally:
        steps.append(
            {
                "step": name,
                "elapsed_ms": round((perf_counter_ns() - started) / 1_000_000, 3),
                "status": status,
            }
        )


def _timing_path(source_name: str) -> Path:
    root = Path(os.environ.get("DTRACK_LIVE_TIMINGS_DIR", DEFAULT_TIMINGS_DIR)).expanduser()
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S.%fZ")
    return root / f"{source_name}-{stamp}-{os.getpid()}.json"


def _write_timing_report(path: Path, report: dict[str, Any]) -> Path:
    steps = report["steps"]
    measured = sum(step["elapsed_ms"] for step in steps)
    for step in steps:
        step["share_percent"] = round(100 * step["elapsed_ms"] / measured, 2) if measured else 0
    report["measured_total_ms"] = round(measured, 3)
    report["slowest_steps"] = [
        {"step": step["step"], "elapsed_ms": step["elapsed_ms"]}
        for step in sorted(steps, key=lambda item: item["elapsed_ms"], reverse=True)[:5]
    ]

    path.parent.mkdir(parents=True, exist_ok=True)
    part = path.with_name(f"{path.name}.part")
    payload = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    part.write_text(payload, encoding="utf-8", newline="\n")
    os.replace(part, path)
    return path


def _profile_once(source_name: str, config: dict, sql: str, output: Path, run_index: int) -> Path:
    steps: list[dict[str, Any]] = []
    report: dict[str, Any] = {
        "source": source_name,
        "run": run_index,
        "status": "running",
        "started_at": datetime.now(UTC).isoformat(),
        "query_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
        "environment": {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
            "python": platform.python_version(),
            "filesystem_encoding": sys.getfilesystemencoding(),
            "locale_encoding": locale.getpreferredencoding(False),
            "report_encoding": "utf-8",
        },
        "steps": steps,
    }
    report_path = _timing_path(source_name)
    started = perf_counter_ns()
    connection = None
    cursor = None
    try:
        with _measure(steps, "source_from_config"):
            source = source_from_config(config)
        with _measure(steps, "connect"):
            connection = source.connect()
        with _measure(steps, "cursor"):
            cursor = connection.cursor()
        with _measure(steps, "execute"):
            cursor.execute(sql)
        with _measure(steps, "read_description"):
            columns = [str(item[0]) for item in (cursor.description or [])]

        batch_size = _positive_int("DTRACK_LIVE_PROFILE_FETCH_SIZE", 10_000)
        with _measure(steps, "fetch_first_batch"):
            first_batch = cursor.fetchmany(batch_size) if columns else []
        rows = list(first_batch)
        with _measure(steps, "fetch_remaining_batches"):
            while batch := cursor.fetchmany(batch_size):
                rows.extend(batch)
        with _measure(steps, "dataframe_from_records"):
            frame = pd.DataFrame.from_records(rows, columns=columns)
        with _measure(steps, "cursor_close"):
            cursor.close()
        cursor = None
        with _measure(steps, "connection_close"):
            connection.close()
        connection = None

        assert columns
        assert not frame.empty
        with _measure(steps, "query_to_csv"):
            csv_path = source.query_to_csv(sql, output, columns)
        with _measure(steps, "pandas_read_csv"):
            exported = pd.read_csv(
                csv_path, dtype="string", keep_default_na=False, encoding="utf-8"
            )
        with _measure(steps, "frame_compare"):
            pd.testing.assert_frame_equal(_text_frame(frame), exported, check_dtype=False)
        report.update(
            status="passed",
            rows=len(frame),
            columns=len(columns),
            fetch_size=batch_size,
        )
    except BaseException as exc:
        report.update(status="failed", exception_type=type(exc).__name__)
        raise
    finally:
        for name, resource in (
            ("cursor_close_after_error", cursor),
            ("connection_close_after_error", connection),
        ):
            if resource is None:
                continue
            try:
                with _measure(steps, name):
                    resource.close()
            except Exception as exc:  # noqa: BLE001 - preserve original query failure
                report.setdefault("cleanup_errors", []).append(type(exc).__name__)
        report["elapsed_ms"] = round((perf_counter_ns() - started) / 1_000_000, 3)
        report["finished_at"] = datetime.now(UTC).isoformat()
        _write_timing_report(report_path, report)
        ranking = ", ".join(
            f"{item['step']}={item['elapsed_ms']:.3f}ms" for item in report["slowest_steps"]
        )
        print(f"timing report: {report_path}\nslowest steps: {ranking}", flush=True)
    return report_path


@pytest.mark.live
@pytest.mark.parametrize("source_name", SOURCE_NAMES)
def test_source_query_and_csv_return_the_same_frame(source_name, tmp_path):
    """Run read-only probe SQL from an ignored local JSON file against each configured source."""
    config, sql = _case(source_name)
    source = source_from_config(config)

    columns, rows = source.query(sql)
    frame = pd.DataFrame.from_records(rows, columns=columns)

    assert columns
    assert not frame.empty
    output = source.query_to_csv(sql, tmp_path / f"{source_name}.csv", columns)
    exported = pd.read_csv(output, dtype="string", keep_default_na=False, encoding="utf-8")
    pd.testing.assert_frame_equal(_text_frame(frame), exported, check_dtype=False)


@pytest.mark.live
@pytest.mark.live_performance
@pytest.mark.parametrize("source_name", SOURCE_NAMES)
def test_source_query_step_timings(source_name, tmp_path, request):
    """Measure every client-side query/export step without enforcing unstable time limits."""
    config, sql = _case(source_name)
    for run_index in range(1, _positive_int("DTRACK_LIVE_PROFILE_RUNS", 1) + 1):
        path = _profile_once(
            source_name,
            config,
            sql,
            tmp_path / f"{source_name}-{run_index}.csv",
            run_index,
        )
        request.node.user_properties.append(("timing_report", str(path)))


@pytest.mark.live
@pytest.mark.live_performance
def test_timing_report_is_utf8_lf_and_windows_safe(tmp_path, monkeypatch):
    monkeypatch.setenv("DTRACK_LIVE_TIMINGS_DIR", str(tmp_path))
    path = _timing_path("hadoop")
    _write_timing_report(
        path,
        {
            "note": "UTF-8 编码检查",
            "steps": [{"step": "probe", "elapsed_ms": 1.0, "status": "ok"}],
        },
    )

    raw = path.read_bytes()
    assert "UTF-8 编码检查" in raw.decode("utf-8")
    assert b"\r\n" not in raw
    assert not set('<>:"/\\|?*') & set(path.name)
