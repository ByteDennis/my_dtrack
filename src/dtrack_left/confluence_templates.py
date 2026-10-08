from __future__ import annotations

import copy
import json
import re
from html import escape
from pathlib import Path
from typing import Any


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
    if not isinstance(document, dict):
        raise TypeError("template data must be one JSON object")
    context = _enrich(document, attachment_names or [])
    page_title = title or str(context.get("title") or template.stem.replace("-", " ").title())
    context["title"] = page_title
    return {
        "title": page_title,
        "body": _render(template.read_text(encoding="utf-8"), [context]),
        "summary": {
            "run_id": str(context.get("run_id") or context.get("validation_id") or source.stem),
            "generated_at": str(context.get("generated_at") or ""),
        },
        "pairs": context.get("tables") or [],
        "template": str(template.resolve()),
        "data": str(source.resolve()),
    }
