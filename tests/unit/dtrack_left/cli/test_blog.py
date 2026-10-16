import csv
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from dtrack_left import confluence
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


def test_tracking_page(tmp_path):
    page = build_tracking_page(_report(tmp_path), run_url="https://reports.test/r-1?a=1&b=2")
    assert page["title"] == "2026-10-07 — r-1"
    assert [(row["pair"], row["row_status"], row["col_status"]) for row in page["pairs"]] == [
        ("customers", "PASS", "PASS"),
        ("orders", "FAIL", "FAIL"),
    ]
    assert "View run HTML" in page["body"] and "a=1&amp;b=2" in page["body"]
    assert "count_diff: 1" in page["body"] and "diff: 1" in page["body"]


def test_blog_defaults(tmp_path, monkeypatch, capsys):
    report = _report(tmp_path)
    monkeypatch.delenv("CONFLUENCE_URL", raising=False)
    monkeypatch.delenv("CONFLUENCE_TOKEN", raising=False)
    output = tmp_path / "preview.xhtml"
    assert main(["tools", "blog", "preview", "--report", str(report), "--out", str(output)]) == 0
    outcome = json.loads(capsys.readouterr().out)
    assert outcome["mode"] == "preview" and outcome["confluence_api"] == "v1"
    assert outcome["pairs"] == 2
    assert output.is_file() and "DTrack Reconciliation" in output.read_text()


def test_blog_publish(tmp_path, monkeypatch, capsys):
    report = _report(tmp_path)
    calls = []

    class Client:
        def get_current_user(self):
            calls.append(("identity",))
            return {"username": "dtrack"}

        def update_or_create(self, parent_id, title, body, representation):
            calls.append(("publish", parent_id, title, representation, "Pair Results" in body))
            return {"id": "42"}

    monkeypatch.setenv("CONFLUENCE_URL", "https://confluence.test")
    monkeypatch.setenv("CONFLUENCE_TOKEN", "secret")
    monkeypatch.setattr(
        confluence,
        "upload_attachment",
        lambda client, page_id, path: (
            calls.append(("attach", Path(path).name, page_id)) or {"name": Path(path).name}
        ),
    )
    monkeypatch.setattr(
        confluence,
        "set_restrictions",
        lambda client, page_id, read, edit, me: (
            calls.append(("restrict", page_id, read, edit, me)) or {"read": {}, "update": {}}
        ),
    )
    monkeypatch.setattr(
        blog_cli,
        "make_confluence_client",
        lambda **kwargs: calls.append(("auth", kwargs)) or Client(),
    )
    argv = [
        "tools",
        "blog",
        "publish",
        "--report",
        str(report),
        "--space",
        "DTRACK",
        "--parent-id",
        "10",
        "--attach",
        str(report / "report.xlsx"),
    ]
    assert main(argv) == 0
    outcome = json.loads(capsys.readouterr().out)
    assert outcome["mode"] == "published" and outcome["page_id"] == "42"
    assert calls[0][0] == "auth" and calls[0][1]["token"] == "secret"
    assert ("publish", "10", "2026-10-07 — r-1", "storage", True) in calls
    assert ("attach", "report.xlsx", "42") in calls
    assert ("restrict", "42", ["dtrack"], ["dtrack"], "dtrack") in calls


def test_confluence_auth(monkeypatch):
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


def test_blog_rejects(tmp_path, capsys):
    report = _report(tmp_path)
    argv = ["tools", "blog", "publish", "--report", str(report), "--space", "DT"]
    assert main([*argv, "--clear-restrictions", "--restrict-read", "alice"]) == 2
    assert "cannot be combined" in capsys.readouterr().err


def test_blog_passes(tmp_path, monkeypatch, capsys):
    report = _report(tmp_path)
    seen = {}
    monkeypatch.setenv("CONFLUENCE_URL", "https://confluence.test")
    monkeypatch.setenv("CONFLUENCE_TOKEN", "secret")
    monkeypatch.setattr(blog_cli, "make_confluence_client", lambda **kwargs: object())
    monkeypatch.setattr(
        blog_cli,
        "publish_tracking_page",
        lambda client, page, **kwargs: seen.update(kwargs) or {"page_id": "7"},
    )
    argv = ["tools", "blog", "publish", "--report", str(report), "--space", "DT"]
    extra = ["--keep-versions", "3", "--delete-version", "2", "--delete-version", "4"]
    extra += ["--restrict-read", "group:readers", "--detach", "old.csv"]
    assert main([*argv, *extra]) == 0
    assert seen["keep"] == 3 and seen["delete_version_numbers"] == [2, 4]
    assert seen["restrict_read"] == ["group:readers"] and seen["restrict_edit"] is None
    assert seen["private"] is True
    assert seen["detach"] == ["old.csv"]
    assert main([*argv, "--clear-restrictions"]) == 0
    assert seen["restrict_read"] == [] and seen["restrict_edit"] == []
    assert seen["private"] is False


def test_blog_template(tmp_path, capsys):
    template = tmp_path / "page.xhtml"
    template.write_text("<h1>{{title}}</h1>{{#tables}}<p>{{table}}</p>{{/tables}}")
    data = tmp_path / "data.json"
    data.write_text(json.dumps({"title": "Validation", "tables": [{"table": "a.b"}]}))
    output = tmp_path / "preview.xhtml"
    assert (
        main(
            [
                "tools",
                "blog",
                "preview",
                "--template",
                str(template),
                "--data",
                str(data),
                "--out",
                str(output),
            ]
        )
        == 0
    )
    assert output.read_text() == "<h1>Validation</h1><p>a.b</p>"
    assert json.loads(capsys.readouterr().out)["title"] == "Validation"


def test_blog_update(tmp_path, monkeypatch, capsys):
    stored = {
        "version": 1,
        "template_name": "page.xhtml",
        "template_xhtml": (
            "<h1>{{title}}</h1>{{#tables}}<p>{{name}} {{{tech_pvt.artifacts_xhtml}}} "
            "{{{biz_pvt.status_xhtml}}}</p>{{/tables}}"
        ),
        "data": {
            "title": "Validation",
            "tables": [
                {
                    "name": "Orders",
                    "table": "main.orders",
                    "tech_pvt": {"artifacts": []},
                    "biz_pvt": {},
                }
            ],
        },
    }
    appended = tmp_path / "table.json"
    appended.write_text(json.dumps({"name": "Customers", "table": "main.customers"}))
    evidence = tmp_path / "evidence.json"
    evidence.write_text(
        json.dumps(
            {
                "table": "Orders",
                "phase": "tech_pvt",
                "label": "Evidence",
                "attachment": "orders.xlsx",
            }
        )
    )
    attachment = tmp_path / "orders.xlsx"
    attachment.write_bytes(b"xlsx")
    seen = {}
    monkeypatch.setenv("CONFLUENCE_URL", "https://confluence.test")
    monkeypatch.setenv("CONFLUENCE_TOKEN", "secret")
    monkeypatch.setattr(blog_cli, "make_confluence_client", lambda **kwargs: object())
    monkeypatch.setattr(
        blog_cli, "get_template_state", lambda client, page_id: {"value": stored, "version": 1}
    )
    monkeypatch.setattr(blog_cli, "attachment_names", lambda client, page_id: ["old.csv"])
    monkeypatch.setattr(
        blog_cli,
        "update_template_page",
        lambda client, page_id, page, attachments: (
            seen.update(page_id=page_id, page=page, attachments=attachments)
            or {"page_id": page_id, "version": 2, "template_state_version": 2}
        ),
    )
    output = tmp_path / "merged.xhtml"
    assert (
        main(
            [
                "tools",
                "blog",
                "update",
                "--page-id",
                "42",
                "--append-table",
                str(appended),
                "--add-evidence",
                str(evidence),
                "--set-status",
                "Orders.business_pvt=PASS",
                "--attach",
                str(attachment),
                "--out",
                str(output),
            ]
        )
        == 0
    )
    outcome = json.loads(capsys.readouterr().out)
    assert outcome["mode"] == "updated" and outcome["tables"] == 2
    assert seen["page_id"] == "42" and seen["attachments"] == [str(attachment)]
    tables = seen["page"]["template_state"]["data"]["tables"]
    assert tables[0]["biz_pvt"]["status"] == "PASS"
    assert tables[0]["tech_pvt"]["artifacts"][0]["attachment"] == "orders.xlsx"
    assert tables[1]["name"] == "Customers"
    assert output.is_file() and "Orders" in output.read_text()


def test_blog_requires(monkeypatch, capsys):
    monkeypatch.setenv("CONFLUENCE_URL", "https://confluence.test")
    monkeypatch.setenv("CONFLUENCE_TOKEN", "secret")
    assert main(["tools", "blog", "update", "--page-id", "42"]) == 2
    assert "give --append-table" in capsys.readouterr().err


def test_blog_delete(monkeypatch, capsys):
    deleted = []
    monkeypatch.setenv("CONFLUENCE_URL", "https://confluence.test")
    monkeypatch.setenv("CONFLUENCE_TOKEN", "secret")
    monkeypatch.setattr(blog_cli, "make_confluence_client", lambda **kwargs: object())
    monkeypatch.setattr(blog_cli, "find_page", lambda client, space, title: "55")
    monkeypatch.setattr(
        blog_cli,
        "delete_page",
        lambda client, page_id, purge: deleted.append((page_id, purge)) or {"page_id": page_id},
    )
    assert main(["tools", "blog", "delete", "--space", "DT", "--title", "t"]) == 0
    assert json.loads(capsys.readouterr().out)["mode"] == "dry-run" and not deleted
    assert main(["tools", "blog", "delete", "--page-id", "55", "--purge", "--yes"]) == 0
    assert deleted == [("55", True)]
    assert (
        main(["tools", "blog", "delete", "--page-id", "55", "--space", "DT", "--title", "t"]) == 2
    )


def test_blog_inspect(monkeypatch, capsys):
    monkeypatch.setenv("CONFLUENCE_URL", "https://confluence.test")
    monkeypatch.setenv("CONFLUENCE_TOKEN", "secret")
    client = object()
    monkeypatch.setattr(blog_cli, "make_confluence_client", lambda **kwargs: client)
    monkeypatch.setattr(blog_cli, "find_page", lambda found, space, title: "55")
    monkeypatch.setattr(
        blog_cli,
        "inspect_page",
        lambda found, page_id: {"page_id": page_id, "versions": [1, 2], "attachments": []},
    )
    assert main(["tools", "blog", "inspect", "--space", "DT", "--title", "t"]) == 0
    assert json.loads(capsys.readouterr().out) == {
        "mode": "inspect",
        "page_id": "55",
        "versions": [1, 2],
        "attachments": [],
    }


def test_old_flat(tmp_path, capsys):
    report = _report(tmp_path)
    assert main(["tools", "blog", "--report", str(report)]) == 2
    assert main(["tools", "blog-delete", "--page-id", "55"]) == 2
    errors = capsys.readouterr().err
    assert "No such option" in errors and "No such command" in errors
