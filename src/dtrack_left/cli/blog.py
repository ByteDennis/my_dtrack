from __future__ import annotations

import os
from pathlib import Path

import click

from ..confluence import build_tracking_page, make_confluence_client, publish_tracking_page
from .options import CONTEXT_SETTINGS, emit_json, log_level_option


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
@click.option("--url", envvar="CONFLUENCE_URL", help="Confluence base URL")
@click.option("--username", envvar="CONFLUENCE_USERNAME", help="required for Confluence Cloud")
@click.option(
    "--cloud/--server",
    default=False,
    envvar="CONFLUENCE_CLOUD",
    help="Cloud uses username + API token; server uses a bearer PAT",
)
@click.option(
    "--attach",
    "attachments",
    multiple=True,
    type=click.Path(exists=True, dir_okay=False),
    help="attach a file to the published page (repeatable)",
)
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
    publish,
):
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
    token = os.getenv("CONFLUENCE_TOKEN")
    missing = [
        name
        for name, value in (
            ("CONFLUENCE_URL", url),
            ("CONFLUENCE_TOKEN", token),
            ("--space", space),
        )
        if not value
    ]
    if missing:
        raise click.UsageError("publishing requires " + ", ".join(missing))
    client = make_confluence_client(url=url, token=token, username=username, cloud=cloud)
    published = publish_tracking_page(
        client,
        page,
        space=space,
        parent_id=parent_id,
        attachments=list(attachments),
    )
    return {**outcome, **published, "mode": "published", "space": space}
