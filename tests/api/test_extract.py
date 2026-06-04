"""Tests for the extraction-side endpoints:
    POST /api/extract
    POST /api/extract/dry-run
    POST /api/extract/run-sql
    POST /api/extract/cancel/{run_id}

The happy path of these endpoints requires live Oracle / Athena / Hadoop /
SAS / CSV connections (set up via .env on the Windows dev box). Here we
focus on:
  * 404 / 400 paths that don't need a backend
  * /api/extract/cancel error path (unknown run_id)
  * dry-run wiring (the conftest's write_extract_sql writes a parseable
    block; on Windows with .env loaded the call should reach the platform
    runner and the assertion is just status==200 OR 500 with a useful
    error -- we don't pin the response since it depends on whether the
    backend rejects the dummy SQL).
"""


def test_extract_aws_routing_shape(client, api_workdir):
    # Routing-only check for a known platform: platform='aws' reaches
    # extract_aws. We accept 200 (conn set up on a dev box) or 500 (no
    # backend / sample tables missing); we DON'T expect 404 / 422.
    resp = client.post("/api/extract", json={
        "platform": "aws", "type": "row",
        "outdir": api_workdir["aws_outdir"],
        "from_date": "2024-01-01", "to_date": "2024-01-02",
    })
    assert resp.status_code in (200, 500), resp.text


def test_extract_unknown_platform_rejected(client, api_workdir):
    # There is no silent default: an unrecognised platform is a 400. It used to
    # fall through to gen_sas, which is wrong now that source=hadoop runs
    # directly via hadoop_connect rather than through the SAS generator.
    resp = client.post("/api/extract", json={
        "platform": "totally-made-up", "type": "row",
        "outdir": api_workdir["aws_outdir"],
    })
    assert resp.status_code == 400, resp.text
    assert "unknown platform" in resp.json()["error"].lower()


def test_dry_run_404_when_sql_file_missing(client, api_workdir):
    resp = client.post("/api/extract/dry-run", json={
        "platform": "oracle", "type": "col",
        "outdir": api_workdir["sas_outdir"],  # empty dir, no extract_col.sql
    })
    assert resp.status_code == 404
    assert "not found" in resp.json()["error"].lower()


def test_dry_run_400_on_empty_sql_file(client, api_workdir, write_extract_sql):
    # Write an empty-ish file (no blocks) -- the parser returns []
    # and the endpoint returns 400.
    path = write_extract_sql(side="left", type="col", name="ignored",
                              conn="pb23", sql="-- nothing here")
    # Overwrite with truly empty content so parse_extract_sql_file yields [].
    open(path, "w").write("-- dtrack header only\n")
    resp = client.post("/api/extract/dry-run", json={
        "platform": "oracle", "type": "col",
        "outdir": api_workdir["sas_outdir"],
    })
    assert resp.status_code == 400


def test_dry_run_400_on_unknown_platform(client, api_workdir, write_extract_sql):
    write_extract_sql(side="left", type="col")
    resp = client.post("/api/extract/dry-run", json={
        "platform": "bogus_platform", "type": "col",
        "outdir": api_workdir["sas_outdir"],
    })
    assert resp.status_code == 400


def test_dry_run_oracle_smoke(client, api_workdir, write_extract_sql):
    # On the Windows dev box with .env loaded, this should reach the
    # oracle driver and either succeed (200 + rows) or fail with a 500
    # carrying a backend error. On any other host, the parent conftest
    # skips this whole module before we get here.
    write_extract_sql(
        side="left", type="col", name="t_pair__col",
        conn="pb23", sql="SELECT 1 AS one FROM dual",
    )
    resp = client.post("/api/extract/dry-run", json={
        "platform": "oracle", "type": "col",
        "outdir": api_workdir["sas_outdir"],
        "max_rows": 5,
    })
    assert resp.status_code in (200, 500), resp.text
    body = resp.json()
    if resp.status_code == 200:
        assert body["ok"] is True
        assert body["name"] == "t_pair__col"
        assert body["platform"] == "oracle"
        assert "columns" in body and "rows" in body


def test_extract_cancel_unknown_run_id_returns_404(client):
    resp = client.post("/api/extract/cancel/does-not-exist")
    assert resp.status_code == 404
    assert "not found" in resp.json()["error"].lower()


def test_run_sql_404_when_file_missing(client, api_workdir):
    resp = client.post("/api/extract/run-sql", json={
        "type": "col", "platform": "oracle",
        "outdir": api_workdir["sas_outdir"],
    })
    assert resp.status_code == 404
    assert "not found" in resp.json()["error"].lower()


def test_run_sql_overwrite_cache_accepted(client, api_workdir, write_extract_sql):
    # We don't drain the SSE stream here -- we just confirm the endpoint
    # accepts the overwrite_cache flag without rejecting the request.
    write_extract_sql(side="left", type="col", name="t_pair__col",
                       conn="pb23", sql="SELECT 1 FROM dual")
    resp = client.post("/api/extract/run-sql", json={
        "type": "col", "platform": "oracle",
        "outdir": api_workdir["sas_outdir"],
        "overwrite_cache": True,
        "max_workers": 1,
    })
    # SSE stream -- TestClient buffers everything, so 200 with text/event-stream
    # is the expected status. We don't assert on body events here.
    assert resp.status_code == 200
    assert resp.headers.get("content-type", "").startswith("text/event-stream")
