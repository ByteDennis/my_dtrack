from __future__ import annotations

import functools
import logging
import time

import click

from ...backends import aws
from ..options import CONTEXT_SETTINGS, emit_json, log_level_option

s3_log = logging.getLogger("dtrack.left.aws")
QUICK_EXIT = 15

profile_option = click.option(
    "--profile",
    default=aws.DEFAULT_PROFILE,
    show_default=True,
    envvar="DTRACK_AWS_PROFILE",
    help="~/.aws/config profile (credential_process, sso or static)",
)
target_option = click.option(
    "--target",
    default=aws.DEFAULT_TARGET,
    show_default=True,
    envvar="DTRACK_AWS_TARGET",
    help="static profile written to ~/.aws/credentials",
)
every_option = click.option(
    "--every",
    default=aws.DEFAULT_EVERY,
    show_default=True,
    help="seconds between refreshes (earlier when the credentials expire first)",
)


# >>> AWS, file and WinSCP errors become {"error"} with exit 1 <<< #
def with_aws(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except aws.AWS_ERRORS as exc:
            s3_log.error("s3 %s failed: %s", func.__name__.removeprefix("s3_"), exc)
            return {"error": f"{type(exc).__name__}: {exc}", "exit_code": 1}

    return wrapper


# >>> one refresh of the static profile, logged on stderr <<< #
def refresher(profile: str, target: str):
    def refresh() -> dict:
        outcome = aws.refresh_profile(profile, target)
        left = outcome["expires_in"]
        s3_log.info(
            "refreshed [%s] from %s, %s",
            target,
            profile,
            "static keys, no expiry" if left is None else f"expires in {left}s",
        )
        return outcome

    return refresh


# >>> refresh until alive() turns false or Ctrl+C <<< #
def keep_fresh(refresh, alive, every: int, first: dict) -> int:
    try:
        return aws.watch(
            refresh, alive, every=every, first=aws.next_delay(first["expires_in"], every)
        )
    except KeyboardInterrupt:
        s3_log.info("stopped refreshing")
        return -1


# >>> dtrack dbx s3: AWS credentials for the S3 side (WinSCP and friends) <<< #
@click.group(
    "s3",
    help="S3 helpers: static credentials for WinSCP, identity",
    context_settings=CONTEXT_SETTINGS,
)
def s3_group():
    pass


# >>> dtrack dbx s3 creds: write the static profile once, or keep it fresh with --watch <<< #
@s3_group.command(
    "creds",
    help="copy --profile into the static --target profile (WinSCP reads it)",
    context_settings=CONTEXT_SETTINGS,
)
@profile_option
@target_option
@click.option("--watch", is_flag=True, help="keep refreshing until Ctrl+C")
@every_option
@log_level_option
@emit_json
@with_aws
def s3_creds(profile, target, watch, every):
    refresh = refresher(profile, target)
    outcome = refresh()
    if watch:
        outcome["refreshes"] = 1 + max(0, keep_fresh(refresh, lambda: True, every, outcome))
    return outcome


# >>> dtrack dbx s3 winscp: refresh, open the saved site, keep refreshing until WinSCP closes <<< #
@s3_group.command(
    "winscp",
    help="refresh --target, open a saved WinSCP site, refresh until it closes",
    context_settings=CONTEXT_SETTINGS,
)
@click.option(
    "--site",
    default=aws.DEFAULT_SITE,
    show_default=True,
    envvar="DTRACK_WINSCP_SITE",
    help="saved WinSCP site that uses --target",
)
@profile_option
@target_option
@every_option
@log_level_option
@emit_json
@with_aws
def s3_winscp(site, profile, target, every):
    refresh = refresher(profile, target)
    first = refresh()
    started = time.monotonic()
    process = aws.launch_winscp(site)
    count = keep_fresh(refresh, lambda: process.poll() is None, every, first)
    outcome = {"site": site, "target": target, "refreshes": 1 + max(0, count)}
    if time.monotonic() - started < QUICK_EXIT:
        outcome["note"] = (
            "WinSCP exited at once (a running WinSCP took the site?); "
            "keep `dtrack dbx s3 creds --watch` running instead"
        )
    return outcome


# >>> dtrack dbx s3 whoami: account, ARN and how long the credentials last <<< #
@s3_group.command(
    "whoami", help="identity and credential expiry of --profile", context_settings=CONTEXT_SETTINGS
)
@profile_option
@log_level_option
@emit_json
@with_aws
def s3_whoami(profile):
    return aws.whoami(profile)
