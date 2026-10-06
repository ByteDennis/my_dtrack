from __future__ import annotations

import csv
import json
from collections import Counter
from html import escape
from pathlib import Path
from typing import Any

STATUS_COLOURS = {
    "PASS": "Green",
    "FAIL": "Red",
    "PARTIAL": "Yellow",
    "SKIPPED": "Grey",
    "RUNNING": "Blue",
}


def _verdicts(path: Path) -> Counter[str]:
    if not path.is_file():
        return Counter()
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return Counter(str(row.get("verdict") or "unknown") for row in csv.DictReader(handle))


def _status(value: str) -> str:
    colour = STATUS_COLOURS.get(value, "Grey")
    return (
        '<ac:structured-macro ac:name="status" ac:schema-version="1">'
        f'<ac:parameter ac:name="colour">{colour}</ac:parameter>'
        f'<ac:parameter ac:name="title">{escape(value)}</ac:parameter>'
        "</ac:structured-macro>"
    )


def _findings(counts: Counter[str]) -> str:
    issues = [(name, count) for name, count in sorted(counts.items()) if name != "match" and count]
    return ", ".join(f"{escape(name)}: {count}" for name, count in issues) or "0 findings"


def _pair_results(report: Path) -> list[dict[str, Any]]:
    rows = []
    for row_path in sorted(report.glob("*_compare_row.csv")):
        pair = row_path.name.removesuffix("_compare_row.csv")
        row_counts = _verdicts(row_path)
        col_counts = _verdicts(report / f"{pair}_compare_col.csv")
        row_total = sum(row_counts.values())
        col_total = sum(col_counts.values())
        row_issues = row_total - row_counts.get("match", 0)
        col_issues = col_total - col_counts.get("match", 0)
        rows.append(
            {
                "pair": pair,
                "row_status": "PASS" if row_total and not row_issues else "FAIL",
                "row_findings": _findings(row_counts),
                "col_status": "SKIPPED" if not col_total else "PASS" if not col_issues else "FAIL",
                "col_findings": "not released" if not col_total else _findings(col_counts),
                "coverage": f"row {row_counts.get('match', 0)}/{row_total}; col {col_counts.get('match', 0)}/{col_total}",
            }
        )
    return rows


# >>> a stable Confluence storage-XHTML index over the durable dtrack report artifacts <<< #
def build_tracking_page(
    report_dir: str | Path,
    *,
    title: str | None = None,
    run_url: str | None = None,
    attachment_names: list[str] | None = None,
) -> dict[str, Any]:
    report = Path(report_dir)
    summary_path = report / "summary.json"
    if not summary_path.is_file():
        raise FileNotFoundError(f"report summary not found: {summary_path}")
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    run_id = str(summary["run_id"])
    generated = str(summary.get("generated_at") or "")[:10] or "undated"
    title = title or f"{generated} — {run_id}"
    findings = summary.get("findings") or {}
    pairs = _pair_results(report)
    cells = [
        "<h1>DTrack Reconciliation</h1>",
        "<table><tbody>",
        f"<tr><th>Run</th><td>{escape(run_id)}</td></tr>",
        f"<tr><th>Status</th><td>{_status(str(summary.get('status') or 'PARTIAL'))}</td></tr>",
        f"<tr><th>Generated</th><td>{escape(str(summary.get('generated_at') or ''))}</td></tr>",
        f"<tr><th>Units</th><td>{int(summary.get('units_completed') or 0)}/{int(summary.get('units_expected') or 0)}</td></tr>",
        "<tr><th>Findings</th><td>"
        + escape(", ".join(f"{key}: {value}" for key, value in sorted(findings.items())))
        + "</td></tr>",
    ]
    if run_url:
        cells.append(
            f'<tr><th>Report</th><td><a href="{escape(run_url, quote=True)}">View run HTML</a></td></tr>'
        )
    attachments = [Path(name).name for name in attachment_names or []]
    if attachments:
        links = "<br />".join(
            '<ac:link><ri:attachment ri:filename="'
            + escape(name, quote=True)
            + '" /><ac:plain-text-link-body><![CDATA['
            + name.replace("]]>", "]]]]><![CDATA[>")
            + "]]></ac:plain-text-link-body></ac:link>"
            for name in attachments
        )
        cells.append(f"<tr><th>Attachments</th><td>{links}</td></tr>")
    cells += [
        "</tbody></table>",
        "<h2>Pair Results</h2>",
        (
            "<table><thead><tr><th>Pair</th><th>Row Check</th><th>Row Findings</th>"
            "<th>Col Check</th><th>Col Findings</th><th>Coverage</th></tr></thead><tbody>"
        ),
    ]
    for row in pairs:
        cells.append(
            "<tr>"
            f"<td>{escape(row['pair'])}</td>"
            f"<td>{_status(row['row_status'])}</td>"
            f"<td>{row['row_findings']}</td>"
            f"<td>{_status(row['col_status'])}</td>"
            f"<td>{row['col_findings']}</td>"
            f"<td>{escape(row['coverage'])}</td>"
            "</tr>"
        )
    cells.append("</tbody></table>")
    return {"title": title, "body": "".join(cells), "summary": summary, "pairs": pairs}


# >>> REST v1: /rest/api on Server/DC, /wiki/rest/api on Cloud <<< #
def make_confluence_client(
    *, url: str, token: str, username: str | None = None, cloud: bool = False
):
    try:
        from atlassian import ConfluenceServer
    except ImportError as exc:
        raise RuntimeError(
            "Confluence support needs dtrack[confluence] (uv sync --extra confluence)"
        ) from exc
    base_url = url.rstrip("/")
    if cloud:
        if not username:
            raise ValueError("Confluence Cloud requires CONFLUENCE_USERNAME with its API token")
        if not base_url.endswith("/wiki"):
            base_url += "/wiki"
        return ConfluenceServer(
            url=base_url,
            username=username,
            password=token,
            cloud=True,
            api_root="rest/api",
        )
    return ConfluenceServer(url=base_url, token=token, api_root="rest/api")


def publish_tracking_page(
    client,
    page: dict[str, Any],
    *,
    space: str,
    parent_id: str | None = None,
    attachments: list[str | Path] | None = None,
) -> dict[str, Any]:
    client.get_current_user()
    if parent_id:
        result = client.update_or_create(
            parent_id, page["title"], page["body"], representation="storage"
        )
    else:
        result = client.update_or_create(
            title=page["title"], body=page["body"], space=space, representation="storage"
        )
    page_id = str((result or {}).get("id") or "")
    if not page_id:
        raise RuntimeError(f"Confluence did not return a page id: {result!r}")
    attached = []
    for item in attachments or []:
        path = Path(item)
        client.attach_file(str(path), page_id=page_id)
        attached.append(path.name)
    return {"page_id": page_id, "attachments": attached}
