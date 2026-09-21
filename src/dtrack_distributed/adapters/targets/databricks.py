from __future__ import annotations

import os
from typing import Any

from ..sources.sql import SqlSource


class DatabricksTarget(SqlSource):
    dialect = "databricks"

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self.server_hostname = config.get("server_hostname") or os.getenv(
            "DATABRICKS_SERVER_HOSTNAME"
        )
        self.http_path = config.get("http_path") or os.getenv("DATABRICKS_HTTP_PATH")
        self.access_token = config.get("access_token") or os.getenv("DATABRICKS_TOKEN")

    def connect(self):
        from databricks import sql

        if not self.server_hostname or not self.http_path:
            raise ValueError("Databricks requires server_hostname and http_path")
        kwargs = {
            "server_hostname": self.server_hostname,
            "http_path": self.http_path,
        }
        if self.access_token:
            kwargs["access_token"] = self.access_token
        return sql.connect(**kwargs)

    def preflight(self) -> dict[str, Any]:
        health = super().preflight()
        if not health["ok"]:
            return health
        try:
            columns, _rows = self.query(f"DESCRIBE TABLE EXTENDED {self.relation()}")
            return {**health, "warehouse": True, "describe_columns": columns}
        except Exception as exc:  # noqa: BLE001 - preflight reports adapter health
            return {"ok": False, "source": "databricks", "error": str(exc)}
