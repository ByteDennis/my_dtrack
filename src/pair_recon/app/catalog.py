import re
from datetime import datetime
from zoneinfo import ZoneInfo

from pair_recon_right.catalog import error_rows

_TABLE = re.compile(r"^[A-Za-z_][\w$]*(?:\.[A-Za-z_][\w$]*){2}$")


def _companion_table(pair_table: str, name: str) -> str:
    return f"{pair_table.rsplit('.', 1)[0]}.{name}"


class Catalog:
    def __init__(
        self,
        sql,
        pair_table: str,
        status_table: str,
        error_table: str,
        progress_table: str,
        column_map_table: str | None = None,
        pair_sql_table: str | None = None,
    ):
        column_map_table = column_map_table or _companion_table(pair_table, "pair_recon_column_map")
        pair_sql_table = pair_sql_table or _companion_table(pair_table, "pair_recon_pair_sql")
        for table in (
            pair_table,
            status_table,
            error_table,
            progress_table,
            column_map_table,
            pair_sql_table,
        ):
            if not _TABLE.fullmatch(table):
                raise ValueError(f"invalid table name: {table}")
        self.sql = sql
        self.pair_table = pair_table
        self.status_table = status_table
        self.error_table = error_table
        self.progress_table = progress_table
        self.column_map_table = column_map_table
        self.pair_sql_table = pair_sql_table

    def configuration_rows(self):
        pairs = self.sql.execute(f"SELECT * FROM {self.pair_table} ORDER BY pair_name")
        queries = {}
        for row in self.sql.execute(f"SELECT * FROM {self.pair_sql_table} ORDER BY pair_name"):
            pair = row["pair_name"]
            if pair in queries:
                raise ValueError(f"duplicate pair SQL: {pair}")
            queries[pair] = row
        return [{**row, "pair_sql": queries.get(row["pair_name"])} for row in pairs]

    def pairs(self):
        pairs = self.sql.execute(
            f"SELECT * FROM {self.pair_table} WHERE enabled = true ORDER BY pair_name"
        )
        active = {row["pair_name"] for row in pairs}
        if not active:
            return pairs

        mappings: dict[str, list[dict]] = {}
        seen_columns = set()
        for row in self.sql.execute(
            f"SELECT pair_name, left_column, right_column, data_type "
            f"FROM {self.column_map_table} ORDER BY pair_name, left_column"
        ):
            pair = row["pair_name"]
            if pair not in active:
                continue
            key = (pair, row["left_column"])
            if key in seen_columns:
                raise ValueError(f"duplicate column mapping: {pair}.{row['left_column']}")
            seen_columns.add(key)
            mappings.setdefault(pair, []).append(row)

        queries = {}
        for row in self.sql.execute(f"SELECT * FROM {self.pair_sql_table} ORDER BY pair_name"):
            pair = row["pair_name"]
            if pair not in active:
                continue
            if pair in queries:
                raise ValueError(f"duplicate pair SQL: {pair}")
            queries[pair] = row

        return [
            {
                **row,
                "column_map_rows": mappings.get(row["pair_name"], []),
                "pair_sql": queries.get(row["pair_name"]),
            }
            for row in pairs
        ]

    def run(self, run_id: str):
        rows = self.sql.execute(
            f"SELECT run_id, state FROM {self.status_table} WHERE run_id=:run_id",
            {"run_id": run_id},
        )
        return rows[0] if rows else None

    def clear(self, run_id: str):
        values = {"run_id": run_id}
        self.sql.execute(f"DELETE FROM {self.error_table} WHERE run_id=:run_id", values)
        self.sql.execute(f"DELETE FROM {self.progress_table} WHERE run_id=:run_id", values)
        self.sql.execute(f"DELETE FROM {self.status_table} WHERE run_id=:run_id", values)

    def progress(self, row: dict):
        columns = (
            "snapshot_ts",
            "run_id",
            "pair_name",
            "state",
            "units_expected",
            "units_ready",
            "units_complete",
            "units_failed",
            "worker_threads",
            "detail",
        )
        numeric = {
            "units_expected",
            "units_ready",
            "units_complete",
            "units_failed",
            "worker_threads",
        }
        expressions = {
            name: (
                f"CAST(:{name} AS TIMESTAMP_NTZ)"
                if name == "snapshot_ts"
                else f"CAST(:{name} AS BIGINT)"
                if name in numeric
                else f":{name}"
            )
            for name in columns
        }
        updates = ", ".join(f"{name}={expressions[name]}" for name in columns if name != "run_id")
        fields = ", ".join(columns)
        values = ", ".join(expressions[name] for name in columns)
        self.sql.execute(
            f"""MERGE INTO {self.progress_table} AS t
USING (SELECT :run_id run_id) AS s ON t.run_id = s.run_id
WHEN MATCHED THEN UPDATE SET {updates}
WHEN NOT MATCHED THEN INSERT ({fields}) VALUES ({values})""",
            {name: row[name] for name in columns},
        )

    def status(self, *, run_id: str, pair: str, owner: str, state: str, timezone: str):
        now = (
            datetime.now(ZoneInfo(timezone)).replace(tzinfo=None).isoformat(timespec="microseconds")
        )
        ended = (
            now
            if state in {"passed", "different", "left_complete", "failed", "cancelled"}
            else None
        )
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

    def finish(self, store, *, run_id: str, pair: str, owner: str, state: str, timezone: str):
        self.status(run_id=run_id, pair=pair, owner=owner, state=state, timezone=timezone)
        self.sql.execute(f"DELETE FROM {self.error_table} WHERE run_id=:run_id", {"run_id": run_id})
        now = datetime.now(ZoneInfo(timezone)).replace(tzinfo=None)
        rows = error_rows(store, run_id, pair, now)
        columns = (
            "run_id",
            "check_name",
            "pair_name",
            "vintage",
            "column_name",
            "partition_value",
            "left_value",
            "right_value",
            "detail",
            "payload",
            "created_at",
        )
        for start in range(0, len(rows), 100):
            parameters = {}
            values = []
            for index, row in enumerate(rows[start : start + 100]):
                names = []
                for column in columns:
                    name = f"r{index}_{column}"
                    parameters[name] = row[column]
                    names.append(
                        f"CAST(:{name} AS TIMESTAMP_NTZ)" if column == "created_at" else f":{name}"
                    )
                values.append(f"({', '.join(names)})")
            self.sql.execute(
                f"INSERT INTO {self.error_table} ({', '.join(columns)}) VALUES "
                + ", ".join(values),
                parameters,
            )
        self.sql.execute(
            f"UPDATE {self.pair_table} SET latest_run_id=:run_id, latest_status=:state, "
            "updated_at=CAST(:now AS TIMESTAMP_NTZ) WHERE pair_name=:pair",
            {
                "run_id": run_id,
                "state": state,
                "now": now.isoformat(timespec="microseconds"),
                "pair": pair,
            },
        )
        return {"status": state, "errors": len(rows)}
