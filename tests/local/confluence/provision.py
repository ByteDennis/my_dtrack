import argparse
import html
import os
import re
import sys
import time
from pathlib import Path

import requests

HERE = Path(__file__).resolve().parent
ENV_FILE = HERE / "confluence.env"
TIMEBOMB_URL = "https://developer.atlassian.com/platform/marketplace/timebomb-licenses-for-testing-server-apps/"
NO_CHECK = {"X-Atlassian-Token": "no-check"}


# >>> the public 10-user / 3-hour Confluence Data Center timebomb key (or DTRACK_CONFLUENCE_LICENSE) <<< #
def timebomb_license() -> str:
    if os.getenv("DTRACK_CONFLUENCE_LICENSE"):
        return os.environ["DTRACK_CONFLUENCE_LICENSE"]
    text = html.unescape(re.sub(r"<[^>]+>", "\n", requests.get(TIMEBOMB_URL, timeout=60).text))
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    start = lines.index("10 user Confluence Data Center license, expires in 3 hours")
    key = []
    for line in lines[start + 1 :]:
        if "license" in line.lower():
            break
        if not re.fullmatch(r"\d+", line):
            key.append(line)
    if not key:
        raise RuntimeError(f"no Confluence DC timebomb key found on {TIMEBOMB_URL}")
    return "".join(key)


def _form(page: str) -> tuple[str, dict[str, str]]:
    form = re.search(
        r'<form[^>]*method="post"[^>]*>.*?</form>', page, re.DOTALL | re.IGNORECASE
    ).group(0)
    action = re.search(r'action="([^"]*)"', form).group(1)
    fields = {}
    for tag in re.finditer(r"<input\b([^>]*)>", form):
        attrs = dict(re.findall(r'([\w-]+)="([^"]*)"', tag.group(1)))
        if attrs.get("type") == "hidden" and attrs.get("name"):
            fields[attrs["name"]] = html.unescape(attrs.get("value", ""))
    return action, fields


# >>> drive the setup wizard: license, no cluster, empty site, internal users, admin/admin <<< #
def run_wizard(base: str, admin: str, password: str) -> None:
    session = requests.Session()
    steps = {
        "setuplicense": {"confLicenseString": timebomb_license(), "setupTypeCustom": "Next"},
        "setupcluster": {"newCluster": "skipCluster"},
        "setupdata": {"contentChoice": "blank", "dbchoiceSelect": "Empty Site"},
        "setupusermanagementchoice": {"userManagementChoice": "internal", "internal": "x"},
        "setupadministrator": {
            "username": admin,
            "fullName": "dtrack test admin",
            "email": f"{admin}@example.com",
            "password": password,
            "confirm": password,
            "setup-next-button": "Next",
        },
    }
    page = session.get(f"{base}/", timeout=600)
    for _ in range(12):
        step = re.search(r"/setup/([a-z]+?)(?:-start)?\.action", page.url)
        if not step or step.group(1) == "finishsetup":
            return
        name = step.group(1)
        if name not in steps:
            raise RuntimeError(f"unexpected setup step {page.url}")
        action, fields = _form(page.text)
        print(f"setup: {name}", flush=True)
        page = session.post(
            requests.compat.urljoin(page.url, action),
            data={**fields, **steps[name]},
            headers=NO_CHECK,
            timeout=900,
        )
    raise RuntimeError(f"setup wizard did not finish; last page {page.url}")


def wait_up(base: str, timeout: float = 600) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if requests.get(f"{base}/status", timeout=10).json().get("state") in (
                "RUNNING",
                "FIRST_RUN",
            ):
                return
        except (requests.RequestException, ValueError):
            pass
        time.sleep(5)
    raise TimeoutError(f"Confluence at {base} did not come up in {timeout:.0f}s")


# >>> space DT, user viewer/viewer in group dtrack-readers, a PAT for admin; idempotent <<< #
def seed(base: str, admin: str, password: str) -> str:
    api = requests.Session()
    api.auth, api.headers = (admin, password), {**NO_CHECK}
    if api.get(f"{base}/rest/api/space/DT", timeout=60).status_code == 404:
        api.post(f"{base}/rest/api/space", json={"key": "DT", "name": "dtrack test"}, timeout=60)
    if (
        api.get(f"{base}/rest/api/user", params={"username": "viewer"}, timeout=60).status_code
        != 200
    ):
        api.post(
            f"{base}/rest/api/admin/user",
            json={
                "userName": "viewer",
                "fullName": "Viewer",
                "email": "viewer@example.com",
                "password": "viewer",
                "notifyViaEmail": False,
            },
            timeout=60,
        ).raise_for_status()
    if api.get(f"{base}/rest/api/group/dtrack-readers", timeout=60).status_code != 200:
        api.post(
            f"{base}/rest/api/admin/group",
            json={"type": "group", "name": "dtrack-readers"},
            timeout=60,
        ).raise_for_status()
    api.put(f"{base}/rest/api/user/viewer/group/dtrack-readers", timeout=60).raise_for_status()
    token = api.post(
        f"{base}/rest/pat/latest/tokens",
        json={"name": f"dtrack-it-{int(time.time())}", "expirationDuration": 7},
        timeout=60,
    )
    token.raise_for_status()
    return token.json()["rawToken"]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="set up the local Confluence DC test instance")
    parser.add_argument(
        "--url", default=os.getenv("DTRACK_CONFLUENCE_URL", "http://127.0.0.1:8090")
    )
    parser.add_argument("--admin", default="admin")
    parser.add_argument("--password", default="admin")
    args = parser.parse_args(argv)
    base = args.url.rstrip("/")
    wait_up(base)
    run_wizard(base, args.admin, args.password)
    token = seed(base, args.admin, args.password)
    ENV_FILE.write_text(
        f"DTRACK_CONFLUENCE_URL={base}\nCONFLUENCE_TOKEN={token}\n"
        f"DTRACK_CONFLUENCE_ADMIN={args.admin}\nDTRACK_CONFLUENCE_PASSWORD={args.password}\n",
        encoding="utf-8",
    )
    print(f"ready: {base} space DT, users {args.admin} + viewer, PAT in {ENV_FILE}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
