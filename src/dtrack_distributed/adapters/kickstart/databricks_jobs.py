from __future__ import annotations

from typing import Any


class DatabricksJobsKickstarter:
    def __init__(self, job_id: int):
        self.job_id = int(job_id)

    def start(self, *, run_id: str, s3_root: str) -> dict[str, Any]:
        from databricks.sdk import WorkspaceClient

        response = WorkspaceClient().jobs.run_now(
            job_id=self.job_id,
            idempotency_token=run_id,
            job_parameters={"run_id": run_id, "s3_root": s3_root},
        )
        return {"run_id": run_id, "databricks_run_id": response.run_id}
