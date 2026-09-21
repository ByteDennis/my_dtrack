from __future__ import annotations

from typing import Any

from .sql import SqlSource


class HadoopSource(SqlSource):
    dialect = "hadoop"

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self.host = config.get("host")
        self.port = int(config.get("port") or 10000)
        self.database = config.get("database") or config.get("schema") or "default"
        self.auth_mechanism = config.get("auth_mechanism") or "PLAIN"

    def connect(self):
        try:
            from impala.dbapi import connect
        except ImportError as exc:
            raise ImportError("Hadoop support requires dtrack-distributed[hadoop]") from exc
        return connect(
            host=self.host,
            port=self.port,
            database=self.database,
            auth_mechanism=self.auth_mechanism,
        )
