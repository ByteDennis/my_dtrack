import argparse
import json
from pathlib import Path, PurePosixPath

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language

from pair_recon.app.config import env_value, load_environment
from pair_recon.app.runner import StatementSql
from pair_recon.core.metadata.workbook import read_pair_workbook, upsert_pair_configuration
from pair_recon_dbx.workspace import resolve_workspace_path

ROOT = Path(__file__).resolve().parents[1]


def upload(workspace, source: Path, target: str):
    root = PurePosixPath(target)
    workspace.workspace.mkdirs(str(root))
    uploaded = []
    expected = {str(root)}
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
        expected.add(str(remote))
        parent = remote.parent
        while parent != root.parent:
            expected.add(str(parent))
            if parent == root:
                break
            parent = parent.parent
    stale = [item for item in workspace_objects(workspace, root) if item.path not in expected]
    deleted = []
    for item in sorted(
        stale,
        key=lambda value: len(PurePosixPath(value.path).parts),
        reverse=True,
    ):
        kind = getattr(item.object_type, "value", item.object_type)
        workspace.workspace.delete(item.path, recursive=kind in {"DIRECTORY", "REPO"})
        deleted.append(item.path)
    return uploaded, deleted


def workspace_objects(workspace, root: PurePosixPath):
    objects = []

    def visit(path: str):
        for item in workspace.workspace.list(path):
            objects.append(item)
            kind = getattr(item.object_type, "value", item.object_type)
            if kind in {"DIRECTORY", "REPO"}:
                visit(item.path)

    visit(str(root))
    return objects


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


def refresh_configuration(workspace, warehouse_id: str, pair_table: str, workbook: Path):
    rows = read_pair_workbook(workbook)
    return upsert_pair_configuration(StatementSql(workspace, warehouse_id), pair_table, rows)


def main(argv=None):
    load_environment(ROOT)
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default=env_value("DBX_PROFILE", default="corp"))
    parser.add_argument("--workspace-dir", default=env_value("DBX_WORKSPACE_DIR"))
    parser.add_argument("--cluster-id", default=env_value("DBX_CLUSTER_ID"))
    parser.add_argument("--job-name", default="pair_recon_right")
    parser.add_argument(
        "--warehouse-id",
        default=env_value("DBX_WAREHOUSE_ID"),
    )
    parser.add_argument("--pair-table", default=env_value("DBX_PAIR_TABLE"))
    parser.add_argument("--config-workbook", type=Path, default=ROOT / "setup" / "pairs.xlsx")
    parser.add_argument("--skip-config", action="store_true")
    args = parser.parse_args(argv)
    if not args.workspace_dir:
        parser.error("--workspace-dir is required (or set DBX_WORKSPACE_DIR)")
    if not args.cluster_id:
        parser.error("--cluster-id is required (or set DBX_CLUSTER_ID)")
    if not args.skip_config:
        if not args.warehouse_id:
            parser.error("--warehouse-id is required unless --skip-config is used")
        if not args.pair_table:
            parser.error("--pair-table is required unless --skip-config is used")
        read_pair_workbook(args.config_workbook)
    client = WorkspaceClient(profile=args.profile)
    parent = resolve_workspace_path(client, args.workspace_dir)
    target = str(parent / "pair_recon_right")
    uploaded, deleted = upload(client, ROOT / "src" / "pair_recon_right", target)
    job_id = upsert(client, job_spec(args.job_name, target + "/notebook", args.cluster_id))
    configuration = (
        None
        if args.skip_config
        else refresh_configuration(client, args.warehouse_id, args.pair_table, args.config_workbook)
    )
    print(
        json.dumps(
            {
                "job_id": job_id,
                "uploaded": uploaded,
                "deleted": deleted,
                "configuration": configuration,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
