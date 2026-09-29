from __future__ import annotations

import argparse
import configparser
import getpass
import io
import json
import os
import sys
import time
import uuid
from pathlib import Path
from urllib.parse import urlparse

try:
    from databricks.sdk.errors import DatabricksError
except ImportError:
    DatabricksError = RuntimeError

HERE = Path(__file__).resolve().parent
ENV_FILE = HERE / "live.env"
STATE_FILE = HERE / ".provision.json"
DATABRICKSCFG = Path.home() / ".databrickscfg"
UC_MASTER_ROLE = "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"
MODES = {
    "existing": "a Volume already mapped to S3 by an admin (company workspace): only verify it",
    "external": "you are metastore admin: IAM role + storage credential + external location + Volumes",
    "bridge": "no external storage (Free Edition): the job reads S3 itself with keys from a secret scope",
}


# >>> interactive state: answers become live.env, created resources go to .provision.json <<< #
class Session:
    def __init__(self, yes: bool):
        self.yes = yes
        self.env = _read_env()
        self.state = json.loads(STATE_FILE.read_text()) if STATE_FILE.exists() else {}
        self._workspace = None

    def ask(self, name: str, prompt: str, default: str = "") -> str:
        current = self.env.get(name) or default
        if self.yes and current:
            return current
        answer = input(f"  {prompt} [{current}]: ").strip() or current
        self.env[name] = answer
        return answer

    def confirm(self, title: str, calls: list[str]) -> bool:
        print(f"\n== {title}")
        for call in calls:
            print(f"   - {call}")
        if self.yes:
            return True
        answer = input("   run it? [Y/n/q] ").strip().lower()
        if answer == "q":
            self.save()
            sys.exit(0)
        return answer in ("", "y", "yes")

    def remember(self, kind: str, value) -> None:
        self.state.setdefault(kind, [])
        if value not in self.state[kind]:
            self.state[kind].append(value)
        self.save()

    def save(self) -> None:
        STATE_FILE.write_text(json.dumps(self.state, indent=2) + "\n")
        _write_env(self.env)

    @property
    def workspace(self):
        if self._workspace is None:
            from databricks.sdk import WorkspaceClient

            profile = self.env.get("DATABRICKS_CONFIG_PROFILE") or "corp"
            config = configparser.ConfigParser()
            config.read(DATABRICKSCFG)
            if profile not in config:
                sys.exit(
                    f"profile [{profile}] is not in {DATABRICKSCFG}; "
                    "run: just live setup --only databricks_login"
                )
            self._workspace = WorkspaceClient(profile=profile)
        return self._workspace


def _read_env() -> dict[str, str]:
    source = ENV_FILE if ENV_FILE.exists() else HERE / "live.env.example"
    values = {}
    for line in source.read_text().splitlines():
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip()
    return values


def _write_env(values: dict[str, str]) -> None:
    lines = []
    for line in (HERE / "live.env.example").read_text().splitlines():
        key = line.split("=", 1)[0].strip()
        lines.append(
            f"{key}={values.get(key, '')}" if "=" in line and not line.startswith("#") else line
        )
    extra = sorted(set(values) - {line.split("=", 1)[0] for line in lines if "=" in line})
    lines += [f"{key}={values[key]}" for key in extra]
    ENV_FILE.write_text("\n".join(lines) + "\n")


# >>> step 1: which AWS account and region boto3 is using <<< #
def aws_identity(session: Session) -> None:
    import boto3

    region = session.ask(
        "AWS_DEFAULT_REGION", "AWS region", boto3.session.Session().region_name or "us-east-1"
    )
    if not session.confirm("AWS identity", ["sts.get_caller_identity()"]):
        return
    identity = boto3.client("sts", region_name=region).get_caller_identity()
    session.env["_AWS_ACCOUNT"] = identity["Account"]
    print(f"   account {identity['Account']}  as {identity['Arn']}")


# >>> step 2: a ~/.databrickscfg profile (browser login or token) and a test call <<< #
def databricks_login(session: Session) -> None:
    profile = session.ask("DATABRICKS_CONFIG_PROFILE", "Databricks profile name", "corp")
    config = configparser.ConfigParser()
    config.read(DATABRICKSCFG)
    if profile not in config:
        host = session.ask(
            "_DATABRICKS_HOST", "workspace URL, e.g. https://dbc-1234.cloud.databricks.com"
        )
        parsed = urlparse(host if "://" in host else f"https://{host}")
        host = session.env["_DATABRICKS_HOST"] = f"https://{parsed.netloc}"
        print(f"   workspace host: {host}")
        print("   token:   paste a personal access token (Settings > Developer > Access tokens)")
        print(
            "   link:    print the login link; open it on your laptop with `ssh -L 8020:localhost:8020`"
        )
        print("   browser: open a browser on this machine")
        auth = session.ask("_DATABRICKS_AUTH", "auth: token | link | browser", "token")
        calls = [f"write [{profile}] host={host} to {DATABRICKSCFG} (mode 600)"]
        if not session.confirm("Databricks profile", calls):
            sys.exit("no Databricks profile written; the later steps need it")
        config[profile] = {"host": host}
        if auth == "token":
            config[profile]["token"] = getpass.getpass("   personal access token (hidden): ")
        else:
            config[profile]["auth_type"] = "external-browser"
        DATABRICKSCFG.touch(mode=0o600)
        with DATABRICKSCFG.open("w") as handle:
            config.write(handle)
        DATABRICKSCFG.chmod(0o600)
    if session.env.get("_DATABRICKS_AUTH") == "link":
        os.environ["BROWSER"] = "echo"
        print(
            "   the login link is printed below; the redirect to localhost:8020 needs the ssh tunnel"
        )
    if not session.confirm(
        "Databricks identity", ["current_user.me() (opens the browser the first time)"]
    ):
        return
    me = session.workspace.current_user.me()
    session.env["DTRACK_LIVE_COMPUTE"] = session.env.get("DTRACK_LIVE_COMPUTE") or "serverless"
    folder = f"/Workspace/Users/{me.user_name}/dtrack_live"
    session.env["DTRACK_LIVE_WORKSPACE_DIR"] = (
        session.env.get("DTRACK_LIVE_WORKSPACE_DIR") or folder
    )
    if "example.com" in session.env["DTRACK_LIVE_WORKSPACE_DIR"]:
        session.env["DTRACK_LIVE_WORKSPACE_DIR"] = folder
    print(f"   signed in as {me.user_name} on {session.workspace.config.host}")


# >>> step 3: the bucket that holds work/ and report/, private and encrypted <<< #
def s3_bucket(session: Session) -> None:
    import boto3

    region = session.env.get("AWS_DEFAULT_REGION") or "us-east-1"
    account = session.env.get("_AWS_ACCOUNT", "acct")
    bucket = session.ask("_BUCKET", "S3 bucket for dtrack", f"dtrack-live-{account}")
    client = boto3.client("s3", region_name=region)
    try:
        client.head_bucket(Bucket=bucket)
        print(f"   bucket {bucket} exists")
    except client.exceptions.ClientError:
        calls = [
            f"s3.create_bucket({bucket}, region={region})",
            "put_public_access_block(all blocked)",
        ]
        if session.confirm("S3 bucket", calls):
            options = (
                {}
                if region == "us-east-1"
                else {"CreateBucketConfiguration": {"LocationConstraint": region}}
            )
            client.create_bucket(Bucket=bucket, **options)
            client.put_public_access_block(
                Bucket=bucket,
                PublicAccessBlockConfiguration={
                    name: True
                    for name in (
                        "BlockPublicAcls",
                        "IgnorePublicAcls",
                        "BlockPublicPolicy",
                        "RestrictPublicBuckets",
                    )
                },
            )
            session.remember("buckets", bucket)
    base = f"s3://{bucket}"
    session.env["DTRACK_LIVE_S3"] = f"{base}/dtrack-live"
    session.env["DTRACK_LIVE_S3_WORK"] = f"{base}/dtrack/work"
    session.env["DTRACK_LIVE_S3_REPORT"] = f"{base}/dtrack/report"


# >>> step 4: a catalog.schema the tests may create tables in <<< #
def uc_schema(session: Session) -> None:
    names = [
        c.name for c in session.workspace.catalogs.list() if c.name not in ("system", "samples")
    ]
    print(f"   catalogs you can see: {', '.join(names) or '(none)'}")
    catalog = session.ask("_CATALOG", "catalog", names[0] if names else "main")
    current = session.env.get("DTRACK_LIVE_SCHEMA", "")
    default = current.split(".", 1)[-1] if current and "example" not in current else "dtrack_live"
    session.env.pop("DTRACK_LIVE_SCHEMA", None)
    name = session.ask("_SCHEMA", f"schema inside {catalog}", default).split(".")[-1]
    session.env["DTRACK_LIVE_SCHEMA"] = f"{catalog}.{name}"
    existing = {s.name for s in session.workspace.schemas.list(catalog_name=catalog)}
    if name in existing:
        print(f"   schema {catalog}.{name} exists")
    elif session.confirm("Unity Catalog schema", [f"schemas.create({name}, catalog={catalog})"]):
        session.workspace.schemas.create(name=name, catalog_name=catalog)
        session.remember("schemas", f"{catalog}.{name}")


# >>> step 5: make the job able to reach work/ and report/ in one of three ways <<< #
def storage(session: Session) -> None:
    try:
        list(session.workspace.storage_credentials.list())
        external_ok = True
    except DatabricksError as exc:
        external_ok = False
        print(
            f"   storage credentials not available here ({type(exc).__name__}): 'external' will not work"
        )
    for mode, text in MODES.items():
        print(f"   {mode:9s} {text}")
    mode = session.ask("_STORAGE_MODE", "storage mode", "external" if external_ok else "bridge")
    {"existing": _existing, "external": _external, "bridge": _bridge}[mode](session)


def _existing(session: Session) -> None:
    for kind in ("WORK", "REPORT"):
        session.ask(
            f"DTRACK_LIVE_VOLUME_{kind}", f"{kind.lower()} Volume path (/Volumes/c/s/v/...)"
        )
        session.ask(f"DTRACK_LIVE_S3_{kind}", f"the S3 URL behind that {kind.lower()} Volume")
    session.env["DTRACK_LIVE_AWS_SECRET_SCOPE"] = ""
    verify_volume_mapping(session)


def _external(session: Session) -> None:
    import boto3

    bucket = session.env["_BUCKET"]
    role_name = session.ask("_UC_ROLE", "IAM role name for Unity Catalog", "dtrack-live-uc")
    iam = boto3.client("iam")
    account = session.env["_AWS_ACCOUNT"]
    role_arn = f"arn:aws:iam::{account}:role/{role_name}"
    calls = [
        f"iam.create_role({role_name}) trusting {UC_MASTER_ROLE.split('/')[-1]} with a placeholder external id",
        f"iam.put_role_policy(s3 read/write on {bucket})",
        f"storage_credentials.create(dtrack_live, role={role_arn})",
        "iam.update_assume_role_policy(real external id + self-assume)",
        f"external_locations.create(dtrack_live, s3://{bucket}/dtrack)",
        "volumes.create(work, report) as EXTERNAL Volumes",
    ]
    if not session.confirm(
        "External storage (IAM role -> credential -> location -> Volumes)", calls
    ):
        return
    try:
        iam.create_role(
            RoleName=role_name, AssumeRolePolicyDocument=json.dumps(_trust(role_arn, "0000"))
        )
    except iam.exceptions.EntityAlreadyExistsException:
        print(f"   role {role_name} exists")
    session.remember("iam_roles", role_name)
    iam.put_role_policy(
        RoleName=role_name,
        PolicyName="dtrack-live-s3",
        PolicyDocument=json.dumps(_bucket_policy(bucket, role_arn)),
    )
    from databricks.sdk.service.catalog import AwsIamRoleRequest, VolumeType

    known = {c.name: c for c in session.workspace.storage_credentials.list()}
    credential = known.get("dtrack_live") or session.workspace.storage_credentials.create(
        name="dtrack_live", aws_iam_role=AwsIamRoleRequest(role_arn=role_arn), skip_validation=True
    )
    session.remember("storage_credentials", "dtrack_live")
    external_id = credential.aws_iam_role.external_id
    for attempt in range(12):
        try:
            iam.update_assume_role_policy(
                RoleName=role_name, PolicyDocument=json.dumps(_trust(role_arn, external_id))
            )
            break
        except iam.exceptions.MalformedPolicyDocumentException as exc:
            if attempt == 11:
                raise
            print(
                f"   role not yet valid as a principal ({exc.response['Error']['Code']}); retrying"
            )
            time.sleep(10)
    print("   waiting 20s for IAM to propagate")
    time.sleep(20)
    url = f"s3://{bucket}/dtrack"
    locations = {x.name for x in session.workspace.external_locations.list()}
    for attempt in range(6):
        if "dtrack_live" in locations:
            break
        try:
            session.workspace.external_locations.create(
                name="dtrack_live", url=url, credential_name="dtrack_live"
            )
            break
        except DatabricksError as exc:
            if attempt == 5:
                raise
            print(f"   external location not ready ({exc}); retrying")
            time.sleep(15)
    session.remember("external_locations", "dtrack_live")
    catalog, schema = session.env["DTRACK_LIVE_SCHEMA"].split(".")
    volumes = {v.name for v in session.workspace.volumes.list(catalog, schema)}
    for kind in ("work", "report"):
        if kind in volumes:
            print(f"   volume {catalog}.{schema}.{kind} exists")
        else:
            session.workspace.volumes.create(
                catalog_name=catalog,
                schema_name=schema,
                name=kind,
                volume_type=VolumeType.EXTERNAL,
                storage_location=f"{url}/{kind}",
            )
        session.remember("volumes", f"{catalog}.{schema}.{kind}")
        session.env[f"DTRACK_LIVE_VOLUME_{kind.upper()}"] = f"/Volumes/{catalog}/{schema}/{kind}"
    session.env["DTRACK_LIVE_AWS_SECRET_SCOPE"] = ""
    verify_volume_mapping(session)


def _trust(role_arn: str, external_id: str) -> dict:
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"AWS": [UC_MASTER_ROLE, role_arn]}
                if external_id != "0000"
                else {"AWS": UC_MASTER_ROLE},
                "Action": "sts:AssumeRole",
                "Condition": {"StringEquals": {"sts:ExternalId": external_id}},
            }
        ],
    }


def _bucket_policy(bucket: str, role_arn: str | None = None) -> dict:
    statements = [
        {
            "Effect": "Allow",
            "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
            "Resource": f"arn:aws:s3:::{bucket}/*",
        },
        {
            "Effect": "Allow",
            "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
            "Resource": f"arn:aws:s3:::{bucket}",
        },
    ]
    if role_arn:
        statements.append({"Effect": "Allow", "Action": "sts:AssumeRole", "Resource": role_arn})
    return {"Version": "2012-10-17", "Statement": statements}


def _bridge(session: Session) -> None:
    import boto3

    bucket = session.env["_BUCKET"]
    user = session.ask("_BRIDGE_USER", "IAM user the job uses to reach S3", "dtrack-live-bridge")
    scope = session.ask("DTRACK_LIVE_AWS_SECRET_SCOPE", "Databricks secret scope", "dtrack_live")
    calls = [
        f"iam.create_user({user}) + inline policy: read/write on {bucket} only",
        "iam.create_access_key (long-lived; deleted by `just live teardown`)",
        f"secrets.create_scope({scope}) + put aws_access_key_id / aws_secret_access_key / aws_default_region",
    ]
    if not session.confirm("Bridge: the job reads S3 with its own scoped key", calls):
        return
    iam = boto3.client("iam")
    iam.create_user(UserName=user)
    session.remember("iam_users", user)
    iam.put_user_policy(
        UserName=user,
        PolicyName="dtrack-live-s3",
        PolicyDocument=json.dumps(_bucket_policy(bucket)),
    )
    key = iam.create_access_key(UserName=user)["AccessKey"]
    if scope not in {s.name for s in session.workspace.secrets.list_scopes()}:
        session.workspace.secrets.create_scope(scope=scope)
        session.remember("secret_scopes", scope)
    for name, value in (
        ("aws_access_key_id", key["AccessKeyId"]),
        ("aws_secret_access_key", key["SecretAccessKey"]),
        ("aws_default_region", session.env.get("AWS_DEFAULT_REGION") or "us-east-1"),
    ):
        session.workspace.secrets.put_secret(scope=scope, key=name, string_value=value)
    session.env["DTRACK_LIVE_VOLUME_WORK"] = ""
    session.env["DTRACK_LIVE_VOLUME_REPORT"] = ""
    print(
        f"   key {key['AccessKeyId']} stored in secret scope {scope}; the secret never touches disk here"
    )


# >>> write through S3, read through the Volume, and the other way round <<< #
def verify_volume_mapping(session: Session) -> None:
    import boto3

    probe = f"_probe/{uuid.uuid4().hex[:8]}.json"
    for kind in ("WORK", "REPORT"):
        volume, s3 = (
            session.env[f"DTRACK_LIVE_VOLUME_{kind}"],
            session.env[f"DTRACK_LIVE_S3_{kind}"],
        )
        if not session.confirm(
            f"Verify {kind.lower()}: {s3} == {volume}",
            [f"put {probe} via boto3, read via files API"],
        ):
            continue
        bucket, _, prefix = s3.removeprefix("s3://").partition("/")
        boto3.client("s3").put_object(Bucket=bucket, Key=f"{prefix}/{probe}", Body=b'{"ok": true}')
        body = session.workspace.files.download(f"{volume}/{probe}").contents.read()
        assert json.loads(body) == {"ok": True}, body
        session.workspace.files.delete(f"{volume}/{probe}")
        print("   same place: OK")


# >>> step 6: a tiny serverless job proves the job side can reach work/ the way the tests will use it <<< #
def probe_job(session: Session) -> None:
    scope = session.env.get("DTRACK_LIVE_AWS_SECRET_SCOPE")
    target = session.env.get("DTRACK_LIVE_VOLUME_WORK") or session.env["DTRACK_LIVE_S3_WORK"]
    if not session.confirm(
        "Probe job on serverless", [f"upload a 10-line notebook, submit it, list {target}"]
    ):
        return
    from databricks.sdk.service import jobs
    from databricks.sdk.service.workspace import ImportFormat, Language

    folder = session.env["DTRACK_LIVE_WORKSPACE_DIR"]
    session.workspace.workspace.mkdirs(folder)
    notebook = f"{folder}/_probe"
    source = PROBE.replace("__TARGET__", target).replace("__SCOPE__", scope or "")
    session.workspace.workspace.upload(
        notebook,
        io.BytesIO(source.encode()),
        format=ImportFormat.SOURCE,
        language=Language.PYTHON,
        overwrite=True,
    )
    task = jobs.SubmitTask(
        task_key="probe", notebook_task=jobs.NotebookTask(notebook_path=notebook)
    )
    run = session.workspace.jobs.submit(run_name="dtrack-live-probe", tasks=[task]).result()
    output = session.workspace.jobs.get_run_output(run.tasks[0].run_id)
    state = (
        run.state.result_state.value if run.state.result_state else run.state.life_cycle_state.value
    )
    print(
        f"   {state}: {output.notebook_output.result if output.notebook_output else output.error}"
    )


PROBE = """# Databricks notebook source
import os, subprocess, sys
target, scope = "__TARGET__", "__SCOPE__"
if target.startswith("s3://"):
    try:
        import boto3
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "boto3"])
    for key in ("aws_access_key_id", "aws_secret_access_key", "aws_default_region"):
        os.environ[key.upper()] = dbutils.secrets.get(scope, key)
    import boto3
    bucket, _, prefix = target[5:].partition("/")
    found = boto3.client("s3").list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=5).get("KeyCount", 0)
else:
    found = len(os.listdir(target)) if os.path.isdir(target) else "missing"
dbutils.notebook.exit(f"reached {target}: {found} entries")
"""


# >>> undo everything recorded in .provision.json, newest first, one confirmation each <<< #
def teardown(session: Session) -> None:
    import boto3

    workspace, iam, s3 = session.workspace, boto3.client("iam"), boto3.resource("s3")
    actions = {
        "volumes": lambda name: workspace.volumes.delete(name),
        "external_locations": lambda name: workspace.external_locations.delete(name, force=True),
        "storage_credentials": lambda name: workspace.storage_credentials.delete(name, force=True),
        "secret_scopes": lambda name: workspace.secrets.delete_scope(name),
        "schemas": lambda name: workspace.schemas.delete(name, force=True),
        "iam_users": lambda name: _delete_user(iam, name),
        "iam_roles": lambda name: _delete_role(iam, name),
        "buckets": lambda name: (s3.Bucket(name).objects.all().delete(), s3.Bucket(name).delete()),
    }
    for kind, action in actions.items():
        for name in list(reversed(session.state.get(kind, []))):
            if session.confirm(f"Delete {kind[:-1]} {name}", [f"{kind}: {name}"]):
                action(name)
                session.state[kind].remove(name)
                session.save()


def _delete_user(iam, name: str) -> None:
    for key in iam.list_access_keys(UserName=name)["AccessKeyMetadata"]:
        iam.delete_access_key(UserName=name, AccessKeyId=key["AccessKeyId"])
    iam.delete_user_policy(UserName=name, PolicyName="dtrack-live-s3")
    iam.delete_user(UserName=name)


def _delete_role(iam, name: str) -> None:
    iam.delete_role_policy(RoleName=name, PolicyName="dtrack-live-s3")
    iam.delete_role(RoleName=name)


STEPS = [aws_identity, databricks_login, s3_bucket, uc_schema, storage, probe_job]


# >>> python tests/live/provision.py setup|teardown [--yes] [--only step] <<< #
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="provision")
    parser.add_argument("action", choices=["setup", "teardown"])
    parser.add_argument("--yes", action="store_true", help="accept every default and step")
    parser.add_argument(
        "--only", choices=[step.__name__ for step in STEPS], help="run one setup step"
    )
    args = parser.parse_args(argv)
    session = Session(args.yes)
    try:
        if args.action == "teardown":
            teardown(session)
        else:
            for step in STEPS:
                if args.only in (None, step.__name__):
                    step(session)
    finally:
        session.save()
    print(f"\nsettings -> {ENV_FILE}\ncreated  -> {STATE_FILE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
