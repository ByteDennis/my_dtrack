from __future__ import annotations

import os
import shutil
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from ...common.runtime.run_logging import get_left_logger

DEFAULT_PROFILE = "corp"
DEFAULT_TARGET = "corp-winscp"
DEFAULT_SITE = "corp-s3"
DEFAULT_EVERY = 1200
EARLY = 300
MIN_DELAY = 60
WINSCP_PATHS = (
    r"C:\Program Files (x86)\WinSCP\WinSCP.exe",
    r"C:\Program Files\WinSCP\WinSCP.exe",
)
AWS_ERRORS = (BotoCoreError, ClientError, OSError, RuntimeError, ValueError)

log = get_left_logger("aws")


# >>> a boto3 session for an ~/.aws/config profile; credential_process and sso run inside botocore <<< #
def session(profile: str):
    return boto3.Session(profile_name=profile)


# >>> the shared credentials file every AWS tool (WinSCP included) reads <<< #
def credentials_file() -> Path:
    return Path(os.getenv("AWS_SHARED_CREDENTIALS_FILE") or Path.home() / ".aws" / "credentials")


# >>> the session's credentials and their expiry; static keys have none <<< #
def resolve(sess) -> tuple:
    credentials = sess.get_credentials()
    if credentials is None:
        raise RuntimeError(f"no AWS credentials for profile {sess.profile_name!r}")
    return credentials.get_frozen_credentials(), getattr(credentials, "_expiry_time", None)


# >>> expiry as ISO time plus seconds left <<< #
def _expiry(expires: datetime | None, now: datetime | None = None) -> dict:
    if expires is None:
        return {"expires": None, "expires_in": None}
    left = int((expires - (now or datetime.now(UTC))).total_seconds())
    return {"expires": expires.isoformat(timespec="seconds"), "expires_in": left}


# >>> replace only the [name] section of a credentials file, atomically, keeping every other profile <<< #
def write_profile(path: Path, name: str, frozen) -> Path:
    lines = path.read_text(encoding="utf-8-sig").splitlines() if path.exists() else []
    kept, skip = [], False
    for line in lines:
        if line.strip().startswith("["):
            skip = line.strip() == f"[{name}]"
        if not skip:
            kept.append(line)
    while kept and not kept[-1].strip():
        kept.pop()
    block = [
        f"[{name}]",
        f"aws_access_key_id = {frozen.access_key}",
        f"aws_secret_access_key = {frozen.secret_key}",
    ]
    if frozen.token:
        block.append(f"aws_session_token = {frozen.token}")
    path.parent.mkdir(parents=True, exist_ok=True)
    staging = path.with_name(path.name + ".tmp")
    staging.write_text("\n".join([*kept, "", *block] if kept else block) + "\n", encoding="utf-8")
    staging.chmod(0o600)
    os.replace(staging, path)
    return path


# >>> copy a refreshable profile into a static one that tools without credential_process can read <<< #
def refresh_profile(
    profile: str, target: str, *, path: Path | None = None, session_factory=session
) -> dict:
    if target == profile:
        raise ValueError(f"target {target!r} would shadow the source profile; pick another name")
    frozen, expires = resolve(session_factory(profile))
    written = write_profile(path or credentials_file(), target, frozen)
    return {
        "profile": profile,
        "target": target,
        "file": str(written),
        "access_key": f"{frozen.access_key[:4]}...{frozen.access_key[-4:]}",
        **_expiry(expires),
    }


# >>> seconds until the next refresh: `every`, or earlier when the credentials expire first <<< #
def next_delay(expires_in: int | None, every: int) -> int:
    if expires_in is None:
        return every
    return max(MIN_DELAY, min(every, expires_in - EARLY))


# >>> refresh on schedule while alive(); a failed refresh is retried after MIN_DELAY <<< #
def watch(refresh, alive, *, every: int, first: int, tick: float = 10, sleep=time.sleep) -> int:
    count, due, waited = 0, first, 0.0
    while alive():
        if waited >= due:
            try:
                due = next_delay(refresh().get("expires_in"), every)
                count += 1
            except AWS_ERRORS as exc:
                log.warning("refresh failed, retrying in %ss: %s", MIN_DELAY, exc)
                due = MIN_DELAY
            waited = 0.0
        sleep(tick)
        waited += tick
    return count


# >>> WinSCP.exe from DTRACK_WINSCP, the default install folders, or PATH <<< #
def winscp_path() -> str:
    candidates = (
        os.getenv("DTRACK_WINSCP"),
        *WINSCP_PATHS,
        shutil.which("WinSCP.exe"),
        shutil.which("winscp"),
    )
    for candidate in candidates:
        if candidate and Path(candidate).is_file():
            return candidate
    raise FileNotFoundError("WinSCP.exe not found; set DTRACK_WINSCP to its full path")


# >>> open a saved WinSCP site in its own process <<< #
def launch_winscp(site: str, *, exe: str | None = None, popen=subprocess.Popen):
    return popen([exe or winscp_path(), site])


# >>> who the profile is (account, ARN) and how long its credentials last <<< #
def whoami(profile: str, *, session_factory=session) -> dict:
    sess = session_factory(profile)
    identity = sess.client("sts").get_caller_identity()
    _, expires = resolve(sess)
    return {
        "profile": profile,
        "account": identity["Account"],
        "arn": identity["Arn"],
        "region": sess.region_name,
        **_expiry(expires),
    }
