import json
import os
import time
from pathlib import Path

import pytest
import requests
from dotenv import dotenv_values

from dtrack_left import confluence
from dtrack_left.cli import main

pytestmark = [pytest.mark.confluence, pytest.mark.docker]
ENV_FILE = Path(__file__).with_name("confluence.env")


# >>> the instance `just confluence up` provisioned: URL, admin PAT, admin password for the REST probes <<< #
@pytest.fixture(scope="module")
def dc():
    values = {**dotenv_values(ENV_FILE), **os.environ} if ENV_FILE.exists() else dict(os.environ)
    base = values.get("DTRACK_CONFLUENCE_URL")
    if not base or not values.get("CONFLUENCE_TOKEN"):
        pytest.skip(f"run `just confluence up` first ({ENV_FILE} missing)")
    try:
        requests.get(f"{base}/status", timeout=5).raise_for_status()
    except requests.RequestException as exc:
        pytest.skip(f"Confluence at {base} is not reachable: {exc}")
    admin = requests.Session()
    admin.auth = (values["DTRACK_CONFLUENCE_ADMIN"], values["DTRACK_CONFLUENCE_PASSWORD"])
    admin.headers["X-Atlassian-Token"] = "no-check"
    client = confluence.make_confluence_client(url=base, token=values["CONFLUENCE_TOKEN"])
    return {"base": base, "token": values["CONFLUENCE_TOKEN"], "admin": admin, "client": client}


@pytest.fixture
def page(dc):
    title = f"dtrack it {time.time_ns()}"
    created = dc["client"].create_page("DT", title, "<p>v1</p>", representation="storage")
    yield {"id": created["id"], "title": title}
    confluence.delete_page(dc["client"], created["id"], purge=True)


def _edit(dc, page, times):
    for n in range(times):
        dc["client"].update_page(page["id"], page["title"], f"<p>edit {n}</p>")


def _viewer_status(dc, page_id):
    response = requests.get(
        f"{dc['base']}/rest/api/content/{page_id}", auth=("viewer", "viewer"), timeout=30
    )
    return response.status_code


def test_9x_keeps_gaps_after_a_delete_and_keep_versions_removes_the_oldest(dc, page):
    _edit(dc, page, 5)
    assert confluence.page_versions(dc["client"], page["id"]) == [1, 2, 3, 4, 5, 6]
    assert confluence.delete_versions(dc["client"], page["id"], [2, 3]) == [2, 3]
    assert confluence.page_versions(dc["client"], page["id"]) == [1, 4, 5, 6]
    outcome = confluence.keep_versions(dc["client"], page["id"], 2)
    assert outcome == {"deleted": 2, "remaining": [5, 6]}
    with pytest.raises(ValueError, match="current version"):
        confluence.delete_versions(dc["client"], page["id"], [6])


def test_read_restriction_by_group_hides_the_page_from_non_members(dc, page):
    client, admin = dc["client"], dc["admin"]
    assert _viewer_status(dc, page["id"]) == 200
    shown = confluence.set_restrictions(
        client, page["id"], read=["group:dtrack-readers"], edit=[], me="admin"
    )
    assert shown["read"] == {"users": ["admin"], "groups": ["dtrack-readers"]}
    assert _viewer_status(dc, page["id"]) == 200
    admin.delete(f"{dc['base']}/rest/api/user/viewer/group/dtrack-readers", timeout=30)
    try:
        assert _viewer_status(dc, page["id"]) == 404
    finally:
        admin.put(f"{dc['base']}/rest/api/user/viewer/group/dtrack-readers", timeout=30)
    confluence.set_restrictions(client, page["id"], read=[], edit=[], me="admin")
    assert confluence.get_restrictions(client, page["id"])["read"] == {"users": [], "groups": []}


def test_edit_restriction_blocks_updates_but_not_reads(dc, page):
    confluence.set_restrictions(dc["client"], page["id"], edit=["admin"], me="admin")
    body = {
        "type": "page",
        "title": page["title"],
        "version": {"number": 2},
        "body": {"storage": {"value": "<p>viewer</p>", "representation": "storage"}},
    }
    response = requests.put(
        f"{dc['base']}/rest/api/content/{page['id']}",
        json=body,
        auth=("viewer", "viewer"),
        timeout=30,
    )
    assert response.status_code == 403 and _viewer_status(dc, page["id"]) == 200


def test_attachment_upload_versions_then_delete(dc, page, tmp_path):
    path = tmp_path / "report.csv"
    path.write_text("a\n1\n")
    first = confluence.upload_attachment(dc["client"], page["id"], path)
    path.write_text("a\n1\n2\n")
    second = confluence.upload_attachment(dc["client"], page["id"], path, comment="rerun")
    assert (first["version"], second["version"]) == (1, 2) and first["id"] == second["id"]
    confluence.delete_attachment(dc["client"], page["id"], "report.csv")
    listed = dc["admin"].get(
        f"{dc['base']}/rest/api/content/{page['id']}/child/attachment", timeout=30
    )
    assert listed.json()["results"] == []


def _report(tmp_path):
    report = tmp_path / "report"
    report.mkdir()
    (report / "summary.json").write_text(
        json.dumps(
            {
                "run_id": "it-1",
                "status": "DONE",
                "generated_at": "2026-10-07T12:00:00+00:00",
                "pairs": 0,
                "released_pairs": [],
                "units_expected": 0,
                "units_completed": 0,
                "findings": {},
                "errors": [],
            }
        )
    )
    (report / "report.xlsx").write_bytes(b"xlsx")
    return report


# >>> the CLI end to end: publish, republish with options, then blog-delete --purge <<< #
def test_blog_cli_publishes_prunes_restricts_and_deletes(dc, tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("CONFLUENCE_URL", dc["base"])
    monkeypatch.setenv("CONFLUENCE_TOKEN", dc["token"])
    report, title = _report(tmp_path), f"dtrack cli it {time.time_ns()}"
    argv = ["tools", "blog", "--report", str(report), "--title", title, "--space", "DT"]
    argv += ["--out", str(tmp_path / "p.xhtml"), "--publish"]
    assert main([*argv, "--attach", str(report / "report.xlsx")]) == 0
    page_id = json.loads(capsys.readouterr().out)["page_id"]
    try:
        for n in range(3):
            (report / "note.txt").write_text(f"run {n}")
            assert main([*argv, "--attach", str(report / "note.txt")]) == 0
        capsys.readouterr()
        assert main([*argv, "--keep-versions", "2", "--restrict-read", "viewer"]) == 0
        outcome = json.loads(capsys.readouterr().out)
        assert len(outcome["versions"]["remaining"]) == 2
        assert set(outcome["restrictions"]["read"]["users"]) == {"admin", "viewer"}
        assert main([*argv, "--detach", "note.txt", "--clear-restrictions"]) == 0
        outcome = json.loads(capsys.readouterr().out)
        assert outcome["detached"] == ["note.txt"]
        assert outcome["restrictions"]["read"]["users"] == []
    finally:
        assert main(["tools", "blog-delete", "--page-id", page_id, "--purge", "--yes"]) == 0
    gone = dc["admin"].get(
        f"{dc['base']}/rest/api/content/{page_id}", params={"status": "any"}, timeout=30
    )
    assert gone.status_code == 404
