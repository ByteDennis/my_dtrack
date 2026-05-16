"""Smoke tests for the HTML page routes -- each one should render Jinja
without raising. We don't assert on the exact markup (templates change
all the time); we just make sure the route + template wiring is intact
so a stray import error gets caught.
"""

import pytest


PAGES = [
    "/",
    "/pairs",
    "/load_row",
    "/row_compare",
    "/col_mapping",
    "/col_gen",
    "/load_col",
    "/col_compare",
    "/playground",
    "/csv_compare",
    "/sync",
]


@pytest.mark.parametrize("path", PAGES)
def test_html_page_renders(client, path):
    resp = client.get(path)
    assert resp.status_code == 200, f"{path} -> {resp.status_code}: {resp.text[:200]}"
    ctype = resp.headers.get("content-type", "")
    assert "text/html" in ctype, f"{path} returned non-HTML content-type: {ctype}"


def test_col_gen_rough_redirects_to_col_gen(client):
    # /col_gen_rough is a back-compat redirect to /col_gen?preset=quick.
    resp = client.get("/col_gen_rough", follow_redirects=False)
    # FastAPI redirects use 307/302; accept any 3xx.
    assert resp.status_code in (301, 302, 303, 307, 308)
    location = resp.headers.get("location", "")
    assert "/col_gen" in location
    assert "preset=quick" in location
