from __future__ import annotations

import os
from pathlib import Path

import click

from ..confluence import (
    build_tracking_page,
    delete_page,
    find_page,
    make_confluence_client,
    publish_tracking_page,
)
from .options import CONTEXT_SETTINGS, emit_json, log_level_option


# >>> --url / --username / --cloud shared by every command that talks to Confluence <<< #
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


# >>> client from CONFLUENCE_TOKEN (never a flag), failing early on whatever is missing <<< #
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


# >>> preview locally by default; --publish is the explicit external-write boundary <<< #
@click.command(
    "blog",
    help="preview or publish a Confluence tracking page for one report",
    context_settings=CONTEXT_SETTINGS,
)
@click.option(
    "--report",
    "report_dir",
    required=True,
    type=click.Path(exists=True, file_okay=False),
    help="local report folder containing summary.json and pair CSVs",
)
@click.option("--title", help="page title; default <generated date> — <run id>")
@click.option("--run-url", help="stable URL for the run-level index.html")
@click.option("--out", type=click.Path(dir_okay=False), help="storage-XHTML preview path")
@click.option("--space", envvar="CONFLUENCE_SPACE", help="Confluence space key")
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
@click.option("--publish", is_flag=True, help="create or update the page; default is preview only")
@log_level_option
@emit_json
def blog_command(
    report_dir,
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
    publish,
):
    if clear_restrictions and (restrict_read or restrict_edit):
        raise click.UsageError("--clear-restrictions cannot be combined with --restrict-read/-edit")
    report = Path(report_dir)
    output = Path(out) if out else report / "confluence.storage.xhtml"
    page = build_tracking_page(
        report,
        title=title,
        run_url=run_url,
        attachment_names=[Path(item).name for item in attachments],
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(page["body"], encoding="utf-8")
    outcome = {
        "mode": "preview",
        "confluence_api": "v1",
        "run_id": page["summary"]["run_id"],
        "title": page["title"],
        "pairs": len(page["pairs"]),
        "preview": str(output.resolve()),
    }
    if not publish:
        return outcome
    client = connect(url, username, cloud, **{"--space": space})
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
    )
    return {**outcome, **published, "mode": "published", "space": space}


# >>> trash (or purge) one tracking page; without --yes it only reports what it would delete <<< #
@click.command(
    "blog-delete",
    help="delete a Confluence tracking page by id or by space + title",
    context_settings=CONTEXT_SETTINGS,
)
@click.option("--page-id", help="page id to delete")
@click.option("--space", envvar="CONFLUENCE_SPACE", help="space key, with --title")
@click.option("--title", help="page title, with --space")
@connection_options
@click.option("--purge", is_flag=True, help="also remove it from the space trash (permanent)")
@click.option("--yes", is_flag=True, help="actually delete; default is a dry run")
@log_level_option
@emit_json
def blog_delete_command(page_id, space, title, url, username, cloud, purge, yes):
    if bool(page_id) == bool(space and title):
        raise click.UsageError("give either --page-id or both --space and --title")
    client = connect(url, username, cloud)
    page_id = page_id or find_page(client, space, title)
    if not page_id:
        raise click.UsageError(f"no page titled {title!r} in space {space}")
    if not yes:
        return {"mode": "dry-run", "page_id": page_id, "purge": purge}
    return {"mode": "deleted", **delete_page(client, page_id, purge=purge)}
