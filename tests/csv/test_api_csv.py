"""HTTP-layer tests for the CSV path via FastAPI's TestClient.

Covers the two CSV surfaces that need no backend:
  * the standalone CSV-compare utility (/api/csv_compare/inspect|run), and
  * the gen-then-run pipeline for a csv↔csv pair
    (/api/generate -> /api/extract/run-sql with platform=csv).
"""

import io
import json
import os

import pytest

# Starlette's TestClient drives the ASGI app over httpx; without it these
# HTTP-layer tests can't run. Skip (don't error) so the rest of the csv suite
# still runs on a host that hasn't installed the test extra.
pytest.importorskip("httpx", reason="pip install httpx to run the CSV API tests")

from helpers import write_data_csv, unified_config_csv_pair


# ---------------------------------------------------------------------------
# /api/csv_compare/* — pure pandas, no config needed
# ---------------------------------------------------------------------------

_LEFT = b"id,name,amt\n1,a,10\n2,b,20\n3,c,30\n"
_RIGHT = b"id,name,amt\n1,a,10\n2,b,99\n3,c,30\n"


def _files():
    return {
        "left": ("left.csv", io.BytesIO(_LEFT), "text/csv"),
        "right": ("right.csv", io.BytesIO(_RIGHT), "text/csv"),
    }


def test_csv_compare_inspect(app_client):
    client, _ = app_client
    resp = client.post("/api/csv_compare/inspect", files=_files())
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["left"]["columns"] == ["id", "name", "amt"]
    assert body["left"]["n_rows"] == 3


def test_csv_compare_run_flags_mismatch(app_client):
    client, _ = app_client
    resp = client.post(
        "/api/csv_compare/run",
        files=_files(),
        data={"pk_cols": "id", "compare_cols": "name,amt", "n_examples": "5"},
    )
    assert resp.status_code == 200, resp.text
    assert "logs" in resp.json()


def test_csv_compare_run_requires_pk(app_client):
    client, _ = app_client
    resp = client.post(
        "/api/csv_compare/run",
        files=_files(),
        data={"pk_cols": "", "compare_cols": "name", "n_examples": "5"},
    )
    # Rejected either by the handler's explicit check (400) or FastAPI form
    # validation (422) — both mean "you can't compare without a PK".
    assert resp.status_code in (400, 422), resp.text


# ---------------------------------------------------------------------------
# /api/generate + /api/extract/run-sql for a csv↔csv pair
# ---------------------------------------------------------------------------

def _seed_csv_pair(ctx):
    """Write raw data CSVs + a csv-pair config into the sandbox; return paths."""
    d = ctx["dir"]
    left = write_data_csv(d / "left.csv", ["trade_date", "ccy", "amt"],
                          [["2026-01-01", "USD", 10], ["2026-01-01", "EUR", 20],
                           ["2026-01-02", "USD", 30]])
    right = write_data_csv(d / "right.csv", ["trade_date", "ccy", "amt"],
                           [["2026-01-01", "USD", 10], ["2026-01-02", "USD", 30]])
    cfg = unified_config_csv_pair(
        left, right, sas_outdir=ctx["sas_outdir"], aws_outdir=ctx["aws_outdir"])
    with open(ctx["config"], "w") as f:
        json.dump(cfg, f)
    return left, right


def test_generate_writes_csv_extract_sql(app_client):
    client, ctx = app_client
    _seed_csv_pair(ctx)
    resp = client.post("/api/generate", json={
        "type": "row",
        "sas_outdir": ctx["sas_outdir"],
        "aws_outdir": ctx["aws_outdir"],
    })
    assert resp.status_code == 200, resp.text
    # left csv -> sas_outdir, right csv -> aws_outdir
    left_sql = os.path.join(ctx["sas_outdir"], "extract_row.sql")
    right_sql = os.path.join(ctx["aws_outdir"], "extract_row.sql")
    assert os.path.exists(left_sql), resp.text
    assert os.path.exists(right_sql)
    body = open(left_sql).read().lower()
    assert "row_count" in body and "select" in body


def test_generate_then_run_csv_produces_row_csv(app_client):
    client, ctx = app_client
    _seed_csv_pair(ctx)
    gen = client.post("/api/generate", json={
        "type": "row",
        "sas_outdir": ctx["sas_outdir"],
        "aws_outdir": ctx["aws_outdir"],
    })
    assert gen.status_code == 200, gen.text

    # Execute the left side's extract_row.sql via the streaming run endpoint.
    run = client.post("/api/extract/run-sql", json={
        "platform": "csv",
        "type": "row",
        "outdir": ctx["sas_outdir"],
    })
    assert run.status_code == 200, run.text
    stream = run.text  # forces the SSE stream (and the worker thread) to finish
    assert "event: done" in stream

    # qualified_name for source=csv, name=left -> "csv_left"
    out_csv = os.path.join(ctx["sas_outdir"], "csv_left_row.csv")
    assert os.path.exists(out_csv), f"no output csv; stream tail:\n{stream[-500:]}"
    body = open(out_csv).read()
    assert "2026-01-01,2" in body and "2026-01-02,1" in body
