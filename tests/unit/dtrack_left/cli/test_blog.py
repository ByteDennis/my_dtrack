import csv
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from dtrack_left.cli import blog as blog_cli
from dtrack_left.cli import main
from dtrack_left.confluence import build_tracking_page, make_confluence_client


def _report(tmp_path: Path) -> Path:
    report = tmp_path / "report"
    report.mkdir()
    (report / "summary.json").write_text(
        json.dumps(
            {
                "run_id": "r-1",
                "status": "DONE",
                "generated_at": "2026-10-07T12:00:00+00:00",
                "pairs": 2,
                "released_pairs": ["customers", "orders"],
                "units_expected": 4,
                "units_completed": 4,
                "findings": {
                    "row_count_diff": 1,
                    "partition_missing": 0,
                    "col_stat_diff": 1,
                },
                "errors": [],
            }
        )
    )
    for pair, row_verdicts, col_verdicts in (
        ("customers", ["match"], ["match"]),
        ("orders", ["match", "count_diff"], ["match", "diff"]),
    ):
        for kind, verdicts in (("row", row_verdicts), ("col", col_verdicts)):
            with (report / f"{pair}_compare_{kind}.csv").open("w", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=["pair", "verdict"])
                writer.writeheader()
                writer.writerows({"pair": pair, "verdict": verdict} for verdict in verdicts)
    (report / "report.xlsx").write_bytes(b"xlsx")
    return report


def test_tracking_page_summarizes_each_pair_and_escapes_links(tmp_path):
    page = build_tracking_page(_report(tmp_path), run_url="https://reports.test/r-1?a=1&b=2")
    assert page["title"] == "2026-10-07 — r-1"
    assert [(row["pair"], row["row_status"], row["col_status"]) for row in page["pairs"]] == [
        ("customers", "PASS", "PASS"),
        ("orders", "FAIL", "FAIL"),
    ]
    assert "View run HTML" in page["body"] and "a=1&amp;b=2" in page["body"]
    assert "count_diff: 1" in page["body"] and "diff: 1" in page["body"]


def test_blog_defaults_to_a_local_preview_without_credentials(tmp_path, monkeypatch, capsys):
    report = _report(tmp_path)
    monkeypatch.delenv("CONFLUENCE_URL", raising=False)
    monkeypatch.delenv("CONFLUENCE_TOKEN", raising=False)
    output = tmp_path / "preview.xhtml"
    assert main(["tools", "blog", "--report", str(report), "--out", str(output)]) == 0
    outcome = json.loads(capsys.readouterr().out)
    assert outcome["mode"] == "preview" and outcome["confluence_api"] == "v1"
    assert outcome["pairs"] == 2
    assert output.is_file() and "DTrack Reconciliation" in output.read_text()


def test_blog_publish_uses_environment_auth_and_attaches_requested_files(
    tmp_path, monkeypatch, capsys
):
    report = _report(tmp_path)
    calls = []

    class Client:
        def get_current_user(self):
            calls.append(("identity",))
            return {"username": "dtrack"}

        def update_or_create(self, parent_id, title, body, representation):
            calls.append(("publish", parent_id, title, representation, "Pair Results" in body))
            return {"id": "42"}

        def attach_file(self, filename, page_id):
            calls.append(("attach", Path(filename).name, page_id))

    monkeypatch.setenv("CONFLUENCE_URL", "https://confluence.test")
    monkeypatch.setenv("CONFLUENCE_TOKEN", "secret")
    monkeypatch.setattr(
        blog_cli,
        "make_confluence_client",
        lambda **kwargs: calls.append(("auth", kwargs)) or Client(),
    )
    argv = [
        "tools",
        "blog",
        "--report",
        str(report),
        "--space",
        "DTRACK",
        "--parent-id",
        "10",
        "--attach",
        str(report / "report.xlsx"),
        "--publish",
    ]
    assert main(argv) == 0
    outcome = json.loads(capsys.readouterr().out)
    assert outcome["mode"] == "published" and outcome["page_id"] == "42"
    assert calls[0][0] == "auth" and calls[0][1]["token"] == "secret"
    assert ("publish", "10", "2026-10-07 — r-1", "storage", True) in calls
    assert ("attach", "report.xlsx", "42") in calls


def test_confluence_auth_distinguishes_server_pat_from_cloud_api_token(monkeypatch):
    calls = []
    monkeypatch.setitem(
        sys.modules,
        "atlassian",
        SimpleNamespace(ConfluenceServer=lambda **kwargs: calls.append(kwargs) or kwargs),
    )
    assert make_confluence_client(url="https://server", token="pat") == {
        "url": "https://server",
        "token": "pat",
        "api_root": "rest/api",
    }
    assert make_confluence_client(
        url="https://cloud.atlassian.net", token="api", username="me@example.com", cloud=True
    ) == {
        "url": "https://cloud.atlassian.net/wiki",
        "username": "me@example.com",
        "password": "api",
        "cloud": True,
        "api_root": "rest/api",
    }
    with pytest.raises(ValueError, match="CONFLUENCE_USERNAME"):
        make_confluence_client(url="https://cloud.atlassian.net", token="api", cloud=True)
