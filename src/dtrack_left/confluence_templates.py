from __future__ import annotations

import copy
import json
import re
from html import escape
from pathlib import Path
from typing import Any

TEMPLATE_STATE_VERSION = 1


class _SafeHtml(str):
    pass


_SECTION = re.compile(r"{{([#^])\s*([A-Za-z0-9_.-]+)\s*}}(.*?){{/\s*\2\s*}}", re.DOTALL)
_TOKEN = re.compile(
    r"{{{\s*([A-Za-z0-9_.-]+)\s*}}}|{{\s*([A-Za-z0-9_.-]+)\s*}}"
)


def _lookup(scopes: list[dict[str, Any]], name: str) -> Any:
    for scope in reversed(scopes):
        value: Any = scope
        found = True
        for part in name.split("."):
            if not isinstance(value, dict) or part not in value:
                found = False
                break
            value = value[part]
        if found:
            return value
    return None


def _render(text: str, scopes: list[dict[str, Any]]) -> str:
    def section(match):
        inverted, name, body = match.groups()
        value = _lookup(scopes, name)
        if inverted == "^":
            return _render(body, scopes) if not value else ""
        if isinstance(value, list):
            return "".join(
                _render(body, [*scopes, item if isinstance(item, dict) else {name: item}])
                for item in value
            )
        if isinstance(value, dict):
            return _render(body, [*scopes, value])
        return _render(body, scopes) if value else ""

    previous = None
    while previous != text:
        previous, text = text, _SECTION.sub(section, text)

    def token(match):
        raw_name, escaped_name = match.groups()
        value = _lookup(scopes, raw_name or escaped_name)
        if raw_name:
            if not isinstance(value, _SafeHtml):
                raise ValueError(f"template raw field {raw_name!r} is not generated safe XHTML")
            return str(value)
        return escape("" if value is None else str(value), quote=True)

    rendered = _TOKEN.sub(token, text)
    unresolved = re.findall(r"{{[^{}]+}}", rendered)
    if unresolved:
        raise ValueError(f"unresolved template token: {unresolved[0]}")
    return rendered


def _passed(value: Any) -> bool:
    return str(value or "").strip().lower() in {"pass", "passed", "complete", "completed", "done"}


def _status(value: Any) -> _SafeHtml:
    label = str(value or "NOT STARTED").strip().upper()
    if _passed(label):
        colour = "Green"
    elif label in {"IN PROGRESS", "STARTED", "PENDING"}:
        colour = "Yellow"
    elif label == "NOT STARTED":
        colour = "Grey"
    else:
        colour = "Red"
    return _SafeHtml(
        '<ac:structured-macro ac:name="status" ac:schema-version="1">'
        f'<ac:parameter ac:name="colour">{colour}</ac:parameter>'
        f'<ac:parameter ac:name="title">{escape(label)}</ac:parameter>'
        "</ac:structured-macro>"
    )


def _artifact_links(artifacts: list[dict[str, Any]]) -> _SafeHtml:
    links = []
    for artifact in artifacts:
        label = escape(str(artifact.get("label") or artifact.get("type") or "artifact"))
        if artifact.get("url"):
            links.append(f'<a href="{escape(str(artifact["url"]), quote=True)}">{label}</a>')
        elif artifact.get("attachment"):
            name = escape(Path(str(artifact["attachment"])).name, quote=True)
            links.append(
                f'<ac:link><ri:attachment ri:filename="{name}" />'
                f"<ac:plain-text-link-body><![CDATA[{label}]]></ac:plain-text-link-body></ac:link>"
            )
    return _SafeHtml("<br />".join(links) or "—")


def _enrich(document: dict[str, Any], attachment_names: list[str]) -> dict[str, Any]:
    data = copy.deepcopy(document)
    tables = data.get("tables") or []
    if not isinstance(tables, list):
        raise TypeError("template data field 'tables' must be a list")
    evidence = []
    for row in tables:
        if not isinstance(row, dict):
            raise TypeError("every template table entry must be an object")
        row["status_xhtml"] = _status(row.get("status"))
        for key, label in (("tech_pvt", "Tech PVT"), ("biz_pvt", "Biz PVT")):
            phase = row.setdefault(key, {})
            phase["status_xhtml"] = _status(phase.get("status"))
            phase["artifacts_xhtml"] = _artifact_links(phase.get("artifacts") or [])
            for artifact in phase.get("artifacts") or []:
                evidence.append(
                    {
                        "table": row.get("table") or row.get("name") or "",
                        "phase": label,
                        "artifact_xhtml": _artifact_links([artifact]),
                    }
                )
    total = len(tables)
    data.update(
        total=total,
        tech_started=sum(bool((row.get("tech_pvt") or {}).get("started_at")) for row in tables),
        tech_passed=sum(_passed((row.get("tech_pvt") or {}).get("status")) for row in tables),
        biz_started=sum(bool((row.get("biz_pvt") or {}).get("started_at")) for row in tables),
        biz_passed=sum(_passed((row.get("biz_pvt") or {}).get("status")) for row in tables),
        evidence=evidence,
        attachments=[{"name": Path(name).name} for name in attachment_names],
    )
    return data


def _build_page(
    template_text: str,
    document: dict[str, Any],
    *,
    template_name: str,
    data_name: str,
    title: str | None = None,
    attachment_names: list[str] | None = None,
) -> dict[str, Any]:
    if not isinstance(document, dict):
        raise TypeError("template data must be one JSON object")
    context = _enrich(document, attachment_names or [])
    page_title = title or str(context.get("title") or Path(template_name).stem.replace("-", " ").title())
    context["title"] = page_title
    return {
        "title": page_title,
        "body": _render(template_text, [context]),
        "summary": {
            "run_id": str(
                context.get("run_id") or context.get("validation_id") or Path(data_name).stem
            ),
            "generated_at": str(context.get("generated_at") or ""),
        },
        "pairs": context.get("tables") or [],
        "template_state": {
            "version": TEMPLATE_STATE_VERSION,
            "template_name": Path(template_name).name,
            "template_xhtml": template_text,
            "data": copy.deepcopy(document),
        },
    }


def build_template_page(
    template_path: str | Path,
    data_path: str | Path,
    *,
    title: str | None = None,
    attachment_names: list[str] | None = None,
) -> dict[str, Any]:
    template = Path(template_path)
    source = Path(data_path)
    document = json.loads(source.read_text(encoding="utf-8"))
    page = _build_page(
        template.read_text(encoding="utf-8"),
        document,
        template_name=template.name,
        data_name=source.name,
        title=title,
        attachment_names=attachment_names,
    )
    page.update(template=str(template.resolve()), data=str(source.resolve()))
    return page


def build_stored_template_page(
    state: dict[str, Any],
    *,
    title: str | None = None,
    attachment_names: list[str] | None = None,
) -> dict[str, Any]:
    if int(state.get("version") or 0) != TEMPLATE_STATE_VERSION:
        raise ValueError(f"unsupported template state version: {state.get('version')!r}")
    document = state.get("data")
    template_text = state.get("template_xhtml")
    if not isinstance(document, dict) or not isinstance(template_text, str):
        raise TypeError("template state is missing data or template_xhtml")
    return _build_page(
        template_text,
        document,
        template_name=str(state.get("template_name") or "page.xhtml"),
        data_name="stored-state.json",
        title=title,
        attachment_names=attachment_names,
    )


def _objects(value: Any, label: str) -> list[dict[str, Any]]:
    rows = value if isinstance(value, list) else [value]
    if not all(isinstance(row, dict) for row in rows):
        raise TypeError(f"{label} must contain one object or a list of objects")
    return rows


def _table(document: dict[str, Any], identity: str) -> dict[str, Any]:
    matches = [
        row
        for row in document.get("tables") or []
        if identity in {str(row.get("name") or ""), str(row.get("table") or "")}
    ]
    if len(matches) != 1:
        raise ValueError(f"table selector {identity!r} matched {len(matches)} entries")
    return matches[0]


def merge_template_data(
    document: dict[str, Any],
    *,
    append_tables: list[Any] | None = None,
    add_evidence: list[Any] | None = None,
    set_status: list[str] | None = None,
) -> dict[str, Any]:
    """Merge logical page changes; the rendered XHTML is always rebuilt afterward."""

    merged = copy.deepcopy(document)
    tables = merged.setdefault("tables", [])
    if not isinstance(tables, list):
        raise TypeError("template data field 'tables' must be a list")
    identities = {
        str(value)
        for row in tables
        for value in (row.get("name"), row.get("table"))
        if value
    }
    for payload in append_tables or []:
        for row in _objects(payload, "--append-table"):
            names = {str(value) for value in (row.get("name"), row.get("table")) if value}
            if not names:
                raise ValueError("an appended table requires name or table")
            duplicate = sorted(names & identities)
            if duplicate:
                raise ValueError(f"table already exists: {duplicate[0]}")
            tables.append(copy.deepcopy(row))
            identities.update(names)

    aliases = {"tech": "tech_pvt", "tech_pvt": "tech_pvt", "business": "biz_pvt", "business_pvt": "biz_pvt", "biz_pvt": "biz_pvt"}
    for payload in add_evidence or []:
        for entry in _objects(payload, "--add-evidence"):
            identity = str(entry.get("table") or entry.get("name") or "")
            phase = aliases.get(str(entry.get("phase") or "").lower())
            if not identity or not phase:
                raise ValueError("evidence requires table and phase=tech_pvt|business_pvt")
            artifacts = entry.get("artifacts")
            if artifacts is None:
                artifact = entry.get("artifact")
                if artifact is None:
                    artifact = {
                        key: entry[key]
                        for key in ("label", "type", "url", "attachment")
                        if key in entry
                    }
                artifacts = [artifact]
            artifacts = _objects(artifacts, "evidence artifacts")
            if any(not (artifact.get("url") or artifact.get("attachment")) for artifact in artifacts):
                raise ValueError("every evidence artifact requires url or attachment")
            phase_data = _table(merged, identity).setdefault(phase, {})
            current = phase_data.setdefault("artifacts", [])
            for artifact in artifacts:
                if artifact not in current:
                    current.append(copy.deepcopy(artifact))

    for change in set_status or []:
        target, separator, status = change.partition("=")
        if not separator or not target.strip() or not status.strip():
            raise ValueError("--set-status must be TABLE[.tech_pvt|.business_pvt]=STATUS")
        identity = target.strip()
        phase = None
        for suffix, canonical in (
            (".business_pvt", "biz_pvt"),
            (".biz_pvt", "biz_pvt"),
            (".tech_pvt", "tech_pvt"),
        ):
            if identity.endswith(suffix):
                identity, phase = identity[: -len(suffix)], canonical
                break
        row = _table(merged, identity)
        target_data = row.setdefault(phase, {}) if phase else row
        target_data["status"] = status.strip().upper()
    return merged
