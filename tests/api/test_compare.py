"""Tests for the comparison endpoints.

    POST /api/compare/row
    POST /api/compare/col
    GET  /api/report/{report_type}

The compare routes shell out to dtrack.cli.cmd_compare_{row,col}. On an
empty DB with no loaded data, these complete (output may be empty); the
test only asserts the route returns 200 or 500 with a captured stdout
buffer (the route's contract is "return {ok, output} or {ok=false, error,
output}").
"""


def test_compare_row_returns_output_shape(client):
    resp = client.post("/api/compare/row", json={
        "from_date": None, "to_date": None,
    })
    assert resp.status_code in (200, 500)
    body = resp.json()
    # Both branches include `output` (captured stdout) — the only stable shape.
    assert "output" in body
    if resp.status_code == 200:
        assert body["ok"] is True


def test_compare_col_returns_output_shape(client):
    resp = client.post("/api/compare/col", json={
        "from_date": None, "to_date": None,
    })
    assert resp.status_code in (200, 500)
    assert "output" in resp.json()


def test_report_missing_returns_404(client):
    resp = client.get("/api/report/row")
    # No compare run has produced output/compare_row.html yet.
    assert resp.status_code == 404
    assert "not found" in resp.json()["error"].lower()


def test_report_returns_html_when_present(client, api_workdir):
    # Drop a stub compare_row.html into the output dir and assert
    # /api/report/row serves it as text/html.
    import os
    path = os.path.join(api_workdir["output_dir"], "compare_row.html")
    with open(path, "w") as f:
        f.write("<html><body>stub</body></html>")
    resp = client.get("/api/report/row")
    assert resp.status_code == 200
    assert "text/html" in resp.headers.get("content-type", "")
    assert "stub" in resp.text
