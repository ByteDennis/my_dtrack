import argparse
import json
from pathlib import Path, PurePosixPath

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language

ROOT = Path(__file__).resolve().parents[1]


def upload(workspace, source: Path, target: str):
    root = PurePosixPath(target)
    workspace.workspace.mkdirs(str(root))
    uploaded = []
    for path in sorted(source.rglob("*")):
        if not path.is_file() or "__pycache__" in path.parts or path.suffix == ".pyc":
            continue
        remote = root.joinpath(*path.relative_to(source).parts)
        workspace.workspace.mkdirs(str(remote.parent))
        options = {"format": ImportFormat.AUTO, "overwrite": True}
        if path.name == "notebook.py":
            remote = remote.with_suffix("")
            options = {
                "format": ImportFormat.SOURCE,
                "language": Language.PYTHON,
                "overwrite": True,
            }
        workspace.workspace.upload(str(remote), path.read_bytes(), **options)
        uploaded.append(str(remote))
    return uploaded


def job_spec(name: str, notebook: str, cluster_id: str):
    return {
        "name": name,
        "max_concurrent_runs": 10,
        "queue": {"enabled": True},
        "timeout_seconds": 0,
        "tasks": [
            {
                "task_key": "right",
                "existing_cluster_id": cluster_id,
                "notebook_task": {"notebook_path": notebook, "base_parameters": {}},
            }
        ],
    }


def upsert(workspace, spec):
    existing = next(
        (item for item in workspace.jobs.list() if item.settings.name == spec["name"]), None
    )
    if existing:
        workspace.api_client.do(
            "POST",
            "/api/2.2/jobs/reset",
            body={"job_id": int(existing.job_id), "new_settings": spec},
        )
        return int(existing.job_id)
    response = workspace.api_client.do("POST", "/api/2.2/jobs/create", body=spec)
    return int(response["job_id"])


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="corp")
    parser.add_argument("--workspace-dir", required=True)
    parser.add_argument("--cluster-id", required=True)
    parser.add_argument("--job-name", default="pair_recon_right")
    args = parser.parse_args(argv)
    client = WorkspaceClient(profile=args.profile)
    target = args.workspace_dir.rstrip("/") + "/pair_recon_right"
    uploaded = upload(client, ROOT / "src" / "pair_recon_right", target)
    job_id = upsert(client, job_spec(args.job_name, target + "/notebook", args.cluster_id))
    print(json.dumps({"job_id": job_id, "uploaded": uploaded}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
