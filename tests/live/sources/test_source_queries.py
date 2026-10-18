import json
import os
import platform
import sys
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from time import perf_counter_ns
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import pytest
from tzlocal import get_localzone_name

from pair_recon_left.backends.sources import source_from_config

CONFIG = Path(os.environ.get("PAIR_RECON_LIVE_SOURCE_QUERIES", "tests/live/source_queries.json"))
TIMINGS = Path(os.environ.get("PAIR_RECON_LIVE_TIMINGS_DIR", ".pair_recon-work/live-timings"))
TIMEZONE = os.environ.get("PAIR_RECON_TIMEZONE", get_localzone_name())
SOURCES = ("oracle", "hadoop", "csv")


def case(source_name):
    if not CONFIG.is_file():
        pytest.skip(f"create {CONFIG}")
    configured = json.loads(CONFIG.read_text(encoding="utf-8-sig"))
    if source_name not in configured:
        pytest.skip(f"add {source_name} to {CONFIG}")
    value = configured[source_name]
    return value["config"], value["query"]


@contextmanager
def measure(rows: list[dict[str, Any]], name: str):
    started = perf_counter_ns()
    try:
        yield
    finally:
        rows.append({"step": name, "elapsed_ms": (perf_counter_ns() - started) / 1_000_000})


@pytest.mark.live
@pytest.mark.parametrize(
    ("source_name", "mode"),
    [(source_name, mode) for source_name in SOURCES for mode in ("records", "csv")],
)
def test_query(source_name, mode, tmp_path):
    config, sql = case(source_name)
    source = source_from_config(config)
    columns, rows = source.query(sql)
    frame = pd.DataFrame.from_records(rows, columns=columns)
    assert columns and not frame.empty
    if mode == "csv":
        exported = pd.read_csv(
            source.query_to_csv(sql, tmp_path / f"{source_name}.csv", columns),
            dtype="string",
            keep_default_na=False,
            encoding="utf-8",
        )
        pd.testing.assert_frame_equal(
            frame.astype("string").fillna(""), exported, check_dtype=False
        )


@pytest.mark.live
@pytest.mark.live_performance
@pytest.mark.parametrize("source_name", SOURCES)
def test_timing(source_name, tmp_path):
    config, sql = case(source_name)
    steps = []
    with measure(steps, "source"):
        source = source_from_config(config)
    with measure(steps, "query"):
        columns, rows = source.query(sql)
    with measure(steps, "dataframe"):
        frame = pd.DataFrame.from_records(rows, columns=columns)
    with measure(steps, "csv"):
        source.query_to_csv(sql, tmp_path / f"{source_name}.csv", columns)
    TIMINGS.mkdir(parents=True, exist_ok=True)
    path = TIMINGS / f"{source_name}-{datetime.now(ZoneInfo(TIMEZONE)):%Y%m%dT%H%M%S%f}.json"
    report = {
        "rows": len(frame),
        "columns": len(columns),
        "platform": platform.platform(),
        "python": sys.version,
        "encoding": "utf-8",
        "timezone": TIMEZONE,
        "steps": steps,
    }
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    assert path.is_file()
