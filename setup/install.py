import argparse
import json
from pathlib import Path

from databricks.sdk import WorkspaceClient

from pair_recon.app.config import env_value, load_environment
from pair_recon.app.runner import StatementSql
from pair_recon.core.metadata.workbook import read_pair_workbook, upsert_pair_configuration
from pair_recon_dbx.deploy import (
    RIGHT_PACKAGE,
    RIGHT_PACKAGE_ENV,
    WORKSPACE_DIR_ENV,
    right_source,
    right_target,
    upload,
)

ROOT = Path(__file__).resolve().parents[1]


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
    parser.add_argument("--workspace-dir", default=env_value(WORKSPACE_DIR_ENV))
    parser.add_argument(
        "--right-package", default=env_value(RIGHT_PACKAGE_ENV, default=RIGHT_PACKAGE)
    )
    parser.add_argument("--cluster-id", default=env_value("DBX_CLUSTER_ID"))
    parser.add_argument("--job-name", default="pair_recon_right")
    parser.add_argument(
        "--warehouse-id",
        default=env_value("DBX_WAREHOUSE_ID"),
    )
    parser.add_argument("--pair-table", default=env_value("DBX_PAIR_TABLE"))
    parser.add_argument(
        "--config-workbook",
        type=Path,
        default=Path(
            env_value("DBX_PAIR_WORKBOOK", default=str(ROOT / "setup" / "pairs.xlsx"))
        ).expanduser(),
    )
    parser.add_argument("--skip-config", action="store_true")
    args = parser.parse_args(argv)
    if not args.workspace_dir:
        parser.error("--workspace-dir is required (or set DBX_WORKSPACE_PDIR)")
    if not args.cluster_id:
        parser.error("--cluster-id is required (or set DBX_CLUSTER_ID)")
    if not args.skip_config:
        if not args.warehouse_id:
            parser.error("--warehouse-id is required unless --skip-config is used")
        if not args.pair_table:
            parser.error("--pair-table is required unless --skip-config is used")
        read_pair_workbook(args.config_workbook)
    client = WorkspaceClient(profile=args.profile)
    target = right_target(client, args.workspace_dir, args.right_package)
    uploaded, deleted = upload(client, right_source(), target)
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
