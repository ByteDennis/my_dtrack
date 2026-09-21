from __future__ import annotations

import json
from typing import Any

from ..targets.databricks import DatabricksTarget


class DeltaCheckpoint:
    def __init__(self, target: DatabricksTarget, table: str):
        self.target = target
        self.table = table
        self.target.query(
            f"CREATE TABLE IF NOT EXISTS {table} (unit_id STRING, result_json STRING, completed_at TIMESTAMP) USING DELTA"
        )

    def completed(self) -> set[str]:
        _columns, rows = self.target.query(f"SELECT unit_id FROM {self.table}")
        return {str(row[0]) for row in rows}

    def record(self, unit_id: str, result: dict[str, Any]) -> None:
        escaped_id = unit_id.replace("'", "''")
        escaped_json = json.dumps(result, ensure_ascii=False, sort_keys=True).replace("'", "''")
        self.target.query(
            f"MERGE INTO {self.table} t USING (SELECT '{escaped_id}' unit_id, "
            f"'{escaped_json}' result_json) s ON t.unit_id=s.unit_id "
            "WHEN NOT MATCHED THEN INSERT (unit_id,result_json,completed_at) "
            "VALUES (s.unit_id,s.result_json,current_timestamp())"
        )
