import re
from datetime import datetime
from zoneinfo import ZoneInfo

_TABLE = re.compile(r"^[A-Za-z_][\w$]*(?:\.[A-Za-z_][\w$]*){2}$")


class Catalog:
    def __init__(self, sql, pair_table: str, status_table: str, error_table: str):
        for table in (pair_table, status_table, error_table):
            if not _TABLE.fullmatch(table):
                raise ValueError(f"invalid table name: {table}")
        self.sql = sql
        self.pair_table = pair_table
        self.status_table = status_table
        self.error_table = error_table

    def pairs(self):
        return self.sql.execute(
            f"SELECT * FROM {self.pair_table} WHERE enabled = true ORDER BY pair_name"
        )

    def run(self, run_id: str):
        rows = self.sql.execute(
            f"SELECT run_id, state FROM {self.status_table} WHERE run_id=:run_id",
            {"run_id": run_id},
        )
        return rows[0] if rows else None

    def clear(self, run_id: str):
        values = {"run_id": run_id}
        self.sql.execute(f"DELETE FROM {self.error_table} WHERE run_id=:run_id", values)
        self.sql.execute(f"DELETE FROM {self.status_table} WHERE run_id=:run_id", values)

    def status(self, *, run_id: str, pair: str, owner: str, state: str, timezone: str):
        now = (
            datetime.now(ZoneInfo(timezone)).replace(tzinfo=None).isoformat(timespec="microseconds")
        )
        ended = now if state in {"passed", "different", "failed", "cancelled"} else None
        values = {
            "run_id": run_id,
            "pair": pair,
            "owner": owner,
            "state": state,
            "timezone": timezone,
            "now": now,
            "ended": ended,
        }
        self.sql.execute(
            f"""MERGE INTO {self.status_table} AS t
USING (SELECT :run_id run_id) AS s ON t.run_id = s.run_id
WHEN MATCHED THEN UPDATE SET pair_name=:pair, owner=:owner, state=:state, timezone=:timezone,
updated_at=CAST(:now AS TIMESTAMP_NTZ), ended_at=CAST(:ended AS TIMESTAMP_NTZ)
WHEN NOT MATCHED THEN INSERT (run_id, pair_name, owner, state, timezone, started_at, updated_at)
VALUES (:run_id, :pair, :owner, :state, :timezone,
CAST(:now AS TIMESTAMP_NTZ), CAST(:now AS TIMESTAMP_NTZ))""",
            values,
        )
