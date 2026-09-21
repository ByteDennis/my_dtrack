from __future__ import annotations

from typing import Any

from .sql import SqlSource


class AthenaSource(SqlSource):
    dialect = "athena"

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self.database = config.get("database") or config.get("schema")
        self.s3_staging_dir = config.get("s3_staging_dir")
        self.region = config.get("region")

    def connect(self):
        try:
            from pyathena import connect
        except ImportError as exc:
            raise ImportError("Athena support requires dtrack-distributed[athena]") from exc
        return connect(
            s3_staging_dir=self.s3_staging_dir,
            region_name=self.region,
            schema_name=self.database,
        )
