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


API_ROOTS = ("rest/api", "rest/experimental")
UPLOAD_HEADERS = {"X-Atlassian-Token": "no-check", "Accept": "application/json"}


class ConfluenceError(RuntimeError):
    pass


# >>> one REST call; retried under rest/experimental where this Confluence version lacks the rest/api route <<< #
def confluence_call(client, method: str, path: str, **kwargs) -> Any:
    response = None
    for root in API_ROOTS:
        response = client.request(method, f"{root}/{path}", advanced_mode=True, **kwargs)
        if response.status_code not in (404, 405):
            break
    if response.status_code >= 300:
        raise ConfluenceError(f"{method} {path}: HTTP {response.status_code} {response.text[:300]}")
    if not response.content or "json" not in response.headers.get("Content-Type", ""):
        return None
    return response.json()


def _results(client, path: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    rows, start = [], 0
    while True:
        page = confluence_call(
            client, "GET", path, params={**(params or {}), "start": start, "limit": 200}
        )
        batch = (page or {}).get("results") or []
        rows += batch
        if len(batch) < 200:
            return rows
        start += len(batch)


def current_version(client, page_id: str) -> int:
    page = confluence_call(client, "GET", f"content/{page_id}", params={"expand": "version"})
    return int(page["version"]["number"])


def page_versions(client, page_id: str) -> list[int]:
    return sorted(int(row["number"]) for row in _results(client, f"content/{page_id}/version"))


# >>> delete historical versions highest first, so DCs that renumber after a delete keep the rest valid <<< #
def delete_versions(client, page_id: str, numbers: list[int]) -> list[int]:
    wanted = sorted({int(number) for number in numbers}, reverse=True)
    current = current_version(client, page_id)
    if current in wanted:
        raise ValueError(f"version {current} is the current version and cannot be deleted")
    missing = sorted(set(wanted) - set(page_versions(client, page_id)))
    if missing:
        raise ValueError(f"page {page_id} has no version(s) {missing}")
    for number in wanted:
        confluence_call(client, "DELETE", f"content/{page_id}/version/{number}")
    return sorted(wanted)


# >>> drop the oldest existing version until `keep` remain; re-listed each time (7.x renumbers, 9.x leaves gaps) <<< #
def keep_versions(client, page_id: str, keep: int) -> dict[str, Any]:
    if keep < 1:
        raise ValueError("--keep-versions must be at least 1")
    deleted = 0
    versions = page_versions(client, page_id)
    while len(versions) > keep:
        confluence_call(client, "DELETE", f"content/{page_id}/version/{versions[0]}")
        remaining = page_versions(client, page_id)
        if len(remaining) >= len(versions):
            raise ConfluenceError(f"deleting version {versions[0]} of page {page_id} had no effect")
        deleted, versions = deleted + 1, remaining
    return {"deleted": deleted, "remaining": versions}


def _identity(user: dict[str, Any], cloud: bool) -> str:
    return str(user.get("accountId") if cloud else user.get("username") or user.get("userKey"))


def _principals(names: list[str], me: str, cloud: bool) -> dict[str, list[dict[str, str]]]:
    users, groups = [], []
    for name in names:
        kind, _, value = name.partition(":") if ":" in name else ("user", "", name)
        if kind not in ("user", "group") or not value:
            raise ValueError(f"restriction {name!r} must be a user name or group:<name>")
        (groups if kind == "group" else users).append(value)
    if names and me not in users:
        users.append(me)
    key = "accountId" if cloud else "username"
    return {
        "user": [{"type": "known", key: user} for user in dict.fromkeys(users)],
        "group": [{"type": "group", "name": group} for group in dict.fromkeys(groups)],
    }


def get_restrictions(client, page_id: str) -> dict[str, dict[str, list[str]]]:
    raw = confluence_call(client, "GET", f"content/{page_id}/restriction/byOperation") or {}
    out = {}
    for operation in ("read", "update"):
        found = (raw.get(operation) or {}).get("restrictions") or {}
        out[operation] = {
            "users": [
                str(row.get("username") or row.get("accountId"))
                for row in (found.get("user") or {}).get("results") or []
            ],
            "groups": [row["name"] for row in (found.get("group") or {}).get("results") or []],
        }
    return out


# >>> replace read/edit restrictions; the caller is always kept in a non-empty list, [] clears it <<< #
def set_restrictions(
    client,
    page_id: str,
    *,
    read: list[str] | None = None,
    edit: list[str] | None = None,
    me: str,
) -> dict[str, dict[str, list[str]]]:
    cloud = bool(getattr(client, "cloud", False))
    body = [
        {"operation": operation, "restrictions": _principals(names, me, cloud)}
        for operation, names in (("read", read), ("update", edit))
        if names is not None
    ]
    if body:
        confluence_call(client, "PUT", f"content/{page_id}/restriction", json=body)
    return get_restrictions(client, page_id)


def _attachment(client, page_id: str, name: str) -> dict[str, Any] | None:
    found = confluence_call(
        client,
        "GET",
        f"content/{page_id}/child/attachment",
        params={"filename": name, "expand": "version"},
    )
    return next((row for row in (found or {}).get("results") or [] if row["title"] == name), None)


# >>> upload a file; an attachment with the same name gets a new version instead of a duplicate <<< #
def upload_attachment(client, page_id: str, path: str | Path, comment: str | None = None):
    path = Path(path)
    existing = _attachment(client, page_id, path.name)
    target = f"content/{page_id}/child/attachment"
    if existing:
        target += f"/{existing['id']}/data"
    form = {"minorEdit": "true", **({"comment": comment} if comment else {})}
    with path.open("rb") as handle:
        result = confluence_call(
            client,
            "POST",
            target,
            data=form,
            files={"file": (path.name, handle)},
            headers=UPLOAD_HEADERS,
        )
    row = (result.get("results") or [result])[0]
    return {"name": path.name, "id": row["id"], "version": row["version"]["number"]}


def delete_attachment(client, page_id: str, name: str) -> str:
    existing = _attachment(client, page_id, name)
    if not existing:
        raise ValueError(f"page {page_id} has no attachment named {name!r}")
    confluence_call(client, "DELETE", f"content/{existing['id']}")
    return existing["id"]


def find_page(client, space: str, title: str) -> str | None:
    found = confluence_call(
        client, "GET", "content", params={"spaceKey": space, "title": title, "type": "page"}
    )
    rows = (found or {}).get("results") or []
    return str(rows[0]["id"]) if rows else None


# >>> move a page to the space trash; purge also empties it from the trash (cannot be undone) <<< #
def delete_page(client, page_id: str, *, purge: bool = False) -> dict[str, Any]:
    confluence_call(client, "DELETE", f"content/{page_id}")
    if purge:
        confluence_call(client, "DELETE", f"content/{page_id}", params={"status": "trashed"})
    return {"page_id": page_id, "state": "purged" if purge else "trashed"}


# >>> create or update the page, then attachments, detaches, version pruning and restrictions in that order <<< #
def publish_tracking_page(
    client,
    page: dict[str, Any],
    *,
    space: str,
    parent_id: str | None = None,
    attachments: list[str | Path] | None = None,
    detach: list[str] | None = None,
    delete_version_numbers: list[int] | None = None,
    keep: int | None = None,
    restrict_read: list[str] | None = None,
    restrict_edit: list[str] | None = None,
) -> dict[str, Any]:
    me = _identity(client.get_current_user(), bool(getattr(client, "cloud", False)))
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
    outcome: dict[str, Any] = {
        "page_id": page_id,
        "attachments": [upload_attachment(client, page_id, item) for item in attachments or []],
        "detached": [],
    }
    for name in detach or []:
        delete_attachment(client, page_id, name)
        outcome["detached"].append(name)
    if delete_version_numbers:
        outcome["deleted_versions"] = delete_versions(client, page_id, delete_version_numbers)
    if keep:
        outcome["versions"] = keep_versions(client, page_id, keep)
    if restrict_read is not None or restrict_edit is not None:
        outcome["restrictions"] = set_restrictions(
            client, page_id, read=restrict_read, edit=restrict_edit, me=me
        )
    return outcome
