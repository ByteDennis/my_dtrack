from __future__ import annotations

import html
import json
import sys
from collections.abc import Callable
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

REPORT_FILES = (
    ("report/index.html", "index.html"),
    ("report/report.xlsx", "report.xlsx"),
    ("report/summary.json", "summary.json"),
)


def _summary(store: Any, run_id: str, error: str | None) -> dict[str, Any]:
    if store.exists("report/summary.json"):
        document = store.get_json("report/summary.json")
        if not isinstance(document, dict):
            raise TypeError("report/summary.json must contain a JSON object")
        return document
    return {
        "run_id": run_id,
        "status": "FAILED" if error else "NO_REPORT",
        "pairs": 0,
        "units_expected": 0,
        "units_completed": 0,
        "findings": {},
        "errors": [],
    }


def _body(
    summary: dict[str, Any],
    *,
    run_uri: str,
    dashboard_url: str,
    error: str | None,
) -> str:
    status = "FAILED" if error else str(summary.get("status") or "UNKNOWN")
    findings = summary.get("findings")
    if not isinstance(findings, dict):
        findings = {}
    finding_rows = (
        "".join(
            f"<tr><td>{html.escape(str(name))}</td><td>{html.escape(str(value))}</td></tr>"
            for name, value in sorted(findings.items())
        )
        or '<tr><td colspan="2">None</td></tr>'
    )
    errors = summary.get("errors")
    error_parts = [error] if error else []
    if errors:
        error_parts.append(json.dumps(errors, ensure_ascii=False, indent=2))
    error_html = ""
    if error_parts:
        error_html = f"<h3>Errors</h3><pre>{html.escape(chr(10).join(error_parts))}</pre>"
    dashboard = html.escape(dashboard_url, quote=True)
    run = html.escape(run_uri)
    return f"""<!doctype html>
<html><head><meta charset="utf-8"></head><body>
<h2>DTrack reconciliation: {html.escape(status)}</h2>
<p><a href="{dashboard}">Open the live dashboard</a></p>
<table border="1" cellpadding="6" cellspacing="0">
<tr><th>Run</th><td>{run}</td></tr>
<tr><th>Pairs</th><td>{html.escape(str(summary.get("pairs", 0)))}</td></tr>
<tr><th>Units</th><td>{html.escape(str(summary.get("units_completed", 0)))} / {html.escape(str(summary.get("units_expected", 0)))}</td></tr>
</table>
<h3>Findings</h3><table border="1" cellpadding="6" cellspacing="0">{finding_rows}</table>
{error_html}
<p>The available HTML, XLSX, and JSON reports are attached.</p>
</body></html>"""


def require_outlook(platform: str = sys.platform) -> Callable[[str], Any]:
    if platform != "win32":
        raise RuntimeError("Outlook delivery requires Windows with Classic Outlook")
    try:
        from win32com.client import Dispatch
    except ImportError as exc:
        raise RuntimeError(
            "Outlook delivery requires pywin32; run this launcher on Windows with the dbx extra"
        ) from exc
    return Dispatch


def send_report(
    store: Any,
    *,
    run_id: str,
    run_uri: str,
    dashboard_url: str,
    recipients: list[str],
    error: str | None = None,
    dispatch: Callable[[str], Any] | None = None,
    platform: str = sys.platform,
) -> dict[str, Any]:
    if not recipients:
        raise ValueError("at least one Outlook recipient is required")

    summary = _summary(store, run_id, error)
    status = "FAILED" if error else str(summary.get("status") or "UNKNOWN")
    outlook = (dispatch or require_outlook(platform))("Outlook.Application")
    mail = outlook.CreateItem(0)
    mail.To = "; ".join(recipients)
    mail.Subject = f"[DTrack] {status} — {run_id}"
    mail.HTMLBody = _body(
        summary,
        run_uri=run_uri,
        dashboard_url=dashboard_url,
        error=error,
    )

    attached = []
    with TemporaryDirectory(prefix="dtrack-mail-") as directory:
        for source, name in REPORT_FILES:
            if not store.exists(source):
                continue
            path = Path(directory, name)
            path.write_bytes(store.get_bytes(source))
            mail.Attachments.Add(str(path))
            attached.append(name)
        mail.Save()
        mail.Send()
    return {"status": status, "attachments": attached}
