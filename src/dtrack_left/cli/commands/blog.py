from __future__ import annotations

import json
import os
from pathlib import Path

import click

from ...backends.cloud.confluence import (
    attachment_names,
    build_tracking_page,
    delete_page,
    find_page,
    get_template_state,
    inspect_page,
    make_confluence_client,
    publish_tracking_page,
    update_template_page,
)
from ...backends.cloud.templates import (
    build_stored_template_page,
    build_template_page,
    merge_template_data,
)
from ..options import CONTEXT_SETTINGS, emit_json, log_level_option


def connection_options(command):
    for option in reversed(
        (
            click.option("--url", envvar="CONFLUENCE_URL", help="Confluence base URL"),
            click.option(
                "--username", envvar="CONFLUENCE_USERNAME", help="required for Confluence Cloud"
            ),
            click.option(
                "--cloud/--server",
                default=False,
                envvar="CONFLUENCE_CLOUD",
                help="Cloud uses username + API token; server uses a bearer PAT",
            ),
        )
    ):
        command = option(command)
    return command


def report_options(command):
    for option in reversed(
        (
            click.option(
                "--report",
                "report_dir",
                type=click.Path(exists=True, file_okay=False),
                help="reconciliation report folder; omit when --template and --data are used",
            ),
            click.option(
                "--template",
                "template_path",
                type=click.Path(exists=True, dir_okay=False),
                help="editable Confluence storage-XHTML template",
            ),
            click.option(
                "--data",
                "data_path",
                type=click.Path(exists=True, dir_okay=False),
                help="JSON context for --template",
            ),
            click.option("--title", help="page title; default <generated date> — <run id>"),
            click.option("--run-url", help="stable URL for the run-level index.html"),
            click.option(
                "--out", type=click.Path(dir_okay=False), help="storage-XHTML preview path"
            ),
        )
    ):
        command = option(command)
    return command


def page_selector_options(command):
    for option in reversed(
        (
            click.option("--page-id", help="page id"),
            click.option("--space", envvar="CONFLUENCE_SPACE", help="space key, with --title"),
            click.option("--title", help="page title, with --space"),
        )
    ):
        command = option(command)
    return command


def connect(url, username, cloud, **required):
    token = os.getenv("CONFLUENCE_TOKEN")
    missing = [
        name
        for name, value in (("CONFLUENCE_URL", url), ("CONFLUENCE_TOKEN", token), *required.items())
        if not value
    ]
    if missing:
        raise click.UsageError("Confluence access requires " + ", ".join(missing))
    return make_confluence_client(url=url, token=token, username=username, cloud=cloud)


def _render(report_dir, template_path, data_path, title, run_url, out, attachments=()):
    custom = bool(template_path or data_path)
    if custom and not (template_path and data_path):
        raise click.UsageError("--template and --data must be supplied together")
    if custom and report_dir:
        raise click.UsageError("use either --report or --template with --data")
    if not custom and not report_dir:
        raise click.UsageError("give --report, or both --template and --data")
    if custom:
        page = build_template_page(
            template_path,
            data_path,
            title=title,
            attachment_names=[Path(item).name for item in attachments],
        )
        base = Path(data_path).parent
        default_name = f"{Path(template_path).stem}.preview.xhtml"
    else:
        report = Path(report_dir)
        page = build_tracking_page(
            report,
            title=title,
            run_url=run_url,
            attachment_names=[Path(item).name for item in attachments],
        )
        base, default_name = report, "confluence.storage.xhtml"
    output = Path(out) if out else base / default_name
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(page["body"], encoding="utf-8")
    return page, {
        "confluence_api": "v1",
        "run_id": page["summary"]["run_id"],
        "title": page["title"],
        "pairs": len(page["pairs"]),
        "preview": str(output.resolve()),
    }


def _resolve_page(client, page_id, space, title):
    if bool(page_id) == bool(space and title):
        raise click.UsageError("give either --page-id or both --space and --title")
    resolved = page_id or find_page(client, space, title)
    if not resolved:
        raise click.UsageError(f"no page titled {title!r} in space {space}")
    return str(resolved)


def _json_file(path: str) -> object:
    return json.loads(Path(path).read_text(encoding="utf-8"))


@click.group(
    "blog",
    help="preview, publish, update, inspect, or delete Confluence tracking pages",
    context_settings=CONTEXT_SETTINGS,
)
def blog_group():
    pass


@blog_group.command("preview", help="render one report as local Confluence storage XHTML")
@report_options
@log_level_option
@emit_json
def blog_preview_command(report_dir, template_path, data_path, title, run_url, out):
    _page, outcome = _render(report_dir, template_path, data_path, title, run_url, out)
    return {"mode": "preview", **outcome}


@blog_group.command("publish", help="create or update one Confluence tracking page")
@report_options
@click.option("--space", envvar="CONFLUENCE_SPACE", required=True, help="Confluence space key")
@click.option("--parent-id", envvar="CONFLUENCE_PARENT_ID", help="tracking parent page id")
@connection_options
@click.option(
    "--attach",
    "attachments",
    multiple=True,
    type=click.Path(exists=True, dir_okay=False),
    help="upload a file; same name = new attachment version (repeatable)",
)
@click.option("--detach", multiple=True, help="delete the attachment with this name (repeatable)")
@click.option(
    "--delete-version",
    "delete_version_numbers",
    multiple=True,
    type=click.IntRange(min=1),
    help="delete this historical page version after publishing (repeatable)",
)
@click.option(
    "--keep-versions",
    type=click.IntRange(min=1),
    help="after publishing, delete the oldest versions until N remain",
)
@click.option(
    "--restrict-read",
    multiple=True,
    help="only these may view: a user name or group:<name> (repeatable; you are always added)",
)
@click.option(
    "--restrict-edit",
    multiple=True,
    help="only these may edit: a user name or group:<name> (repeatable; you are always added)",
)
@click.option("--clear-restrictions", is_flag=True, help="remove every view and edit restriction")
@click.option(
    "--private/--space-visible",
    "private_page",
    default=True,
    show_default=True,
    help="default restricts view and edit to the publishing user",
)
@log_level_option
@emit_json
def blog_publish_command(
    report_dir,
    template_path,
    data_path,
    title,
    run_url,
    out,
    space,
    parent_id,
    url,
    username,
    cloud,
    attachments,
    detach,
    delete_version_numbers,
    keep_versions,
    restrict_read,
    restrict_edit,
    clear_restrictions,
    private_page,
):
    if clear_restrictions and (restrict_read or restrict_edit):
        raise click.UsageError("--clear-restrictions cannot be combined with --restrict-read/-edit")
    page, outcome = _render(report_dir, template_path, data_path, title, run_url, out, attachments)
    client = connect(url, username, cloud)
    published = publish_tracking_page(
        client,
        page,
        space=space,
        parent_id=parent_id,
        attachments=list(attachments),
        detach=list(detach),
        delete_version_numbers=list(delete_version_numbers),
        keep=keep_versions,
        restrict_read=[] if clear_restrictions else list(restrict_read) or None,
        restrict_edit=[] if clear_restrictions else list(restrict_edit) or None,
        private=private_page and not clear_restrictions,
    )
    return {"mode": "published", **outcome, **published, "space": space}


@blog_group.command(
    "update",
    help="merge structured changes into one template-backed page and redraw it",
)
@page_selector_options
@connection_options
@click.option(
    "--append-table",
    "append_table_paths",
    multiple=True,
    type=click.Path(exists=True, dir_okay=False),
    help="JSON table object or list to append (repeatable)",
)
@click.option(
    "--add-evidence",
    "evidence_paths",
    multiple=True,
    type=click.Path(exists=True, dir_okay=False),
    help="JSON evidence object or list to merge (repeatable)",
)
@click.option(
    "--set-status",
    "status_changes",
    multiple=True,
    help="TABLE[.tech_pvt|.business_pvt]=STATUS (repeatable)",
)
@click.option(
    "--attach",
    "attachments",
    multiple=True,
    type=click.Path(exists=True, dir_okay=False),
    help="upload evidence file; same name creates a new attachment version (repeatable)",
)
@click.option("--out", type=click.Path(dir_okay=False), help="write the merged XHTML locally")
@log_level_option
@emit_json
def blog_update_command(
    page_id,
    space,
    title,
    url,
    username,
    cloud,
    append_table_paths,
    evidence_paths,
    status_changes,
    attachments,
    out,
):
    if not (append_table_paths or evidence_paths or status_changes or attachments):
        raise click.UsageError("give --append-table, --add-evidence, --set-status, or --attach")
    client = connect(url, username, cloud)
    resolved = _resolve_page(client, page_id, space, title)
    try:
        stored = get_template_state(client, resolved)["value"]
        merged = merge_template_data(
            stored["data"],
            append_tables=[_json_file(path) for path in append_table_paths],
            add_evidence=[_json_file(path) for path in evidence_paths],
            set_status=list(status_changes),
        )
        state = {**stored, "data": merged}
        known_attachments = set(attachment_names(client, resolved))
        known_attachments.update(Path(path).name for path in attachments)
        page = build_stored_template_page(
            state,
            attachment_names=sorted(known_attachments),
        )
        if out:
            output = Path(out)
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(page["body"], encoding="utf-8")
        updated = update_template_page(
            client,
            resolved,
            page,
            attachments=list(attachments),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise click.UsageError(str(exc)) from None
    return {
        "mode": "updated",
        "title": page["title"],
        "tables": len(page["pairs"]),
        **({"preview": str(Path(out).resolve())} if out else {}),
        **updated,
    }


@blog_group.command("inspect", help="show page metadata, versions, restrictions, and attachments")
@page_selector_options
@connection_options
@log_level_option
@emit_json
def blog_inspect_command(page_id, space, title, url, username, cloud):
    client = connect(url, username, cloud)
    return {"mode": "inspect", **inspect_page(client, _resolve_page(client, page_id, space, title))}


@blog_group.command("delete", help="trash or permanently purge one tracking page")
@page_selector_options
@connection_options
@click.option("--purge", is_flag=True, help="also remove it from the space trash (permanent)")
@click.option("--yes", is_flag=True, help="actually delete; default is a dry run")
@log_level_option
@emit_json
def blog_delete_command(page_id, space, title, url, username, cloud, purge, yes):
    client = connect(url, username, cloud)
    resolved = _resolve_page(client, page_id, space, title)
    if not yes:
        return {"mode": "dry-run", "page_id": resolved, "purge": purge}
    return {"mode": "deleted", **delete_page(client, resolved, purge=purge)}
