import os

import pytest
from databricks.sdk import WorkspaceClient

from pair_recon.app.config import env_value, load_environment
from pair_recon.app.runner import StatementSql
from pair_recon_right.engine.common.sql import dates

pytestmark = pytest.mark.live

SOURCE_SQL = """
WITH source(native_dt, dash_dt, compact_dt, sas_date9, sas_days) AS (
  SELECT * FROM VALUES
    (DATE '2026-01-02', '2026-01-02', '20260102', '02JAN2026', 24108.75),
    (DATE '2026-01-03', '2026-01-03', '20260103', '03JAN2026', 24109.00)
)
""".strip()


def required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        pytest.fail(f"{name} is required in .pair_recon.env")
    return value


@pytest.fixture(scope="module")
def dbx_sql():
    load_environment()
    profile = required("DBX_PROFILE")
    client = WorkspaceClient(
        profile=profile,
        auth_type=env_value("DBX_AUTH_TYPE", default="external-browser"),
    )
    identity = client.current_user.me()
    assert identity.user_name, "WorkspaceClient did not return the current Databricks user"
    return StatementSql(client, required("DBX_WAREHOUSE_ID"))


def test_right_date_sql_executes_in_databricks(dbx_sql):
    specs = {
        "native_day": ("native_dt", "date"),
        "dash_day": ("dash_dt", {"type": "string", "format": "dash"}),
        "compact_day": ("compact_dt", {"type": "string", "format": "compact"}),
        "sas_date9_day": ("sas_date9", {"type": "sas", "format": "date9"}),
        "sas_days_day": ("sas_days", {"type": "sas", "format": "days"}),
    }
    projection = ",\n  ".join(
        f"CAST({dates.day_expr(column, 'databricks', spec)} AS STRING) AS {alias}"
        for alias, (column, spec) in specs.items()
    )

    rows = dbx_sql.execute(
        f"{SOURCE_SQL}\nSELECT\n  {projection}\nFROM source\nORDER BY native_day"
    )

    assert rows == [
        {
            "native_day": "2026-01-02",
            "dash_day": "2026-01-02",
            "compact_day": "20260102",
            "sas_date9_day": "2026-01-02",
            "sas_days_day": "2026-01-02",
        },
        {
            "native_day": "2026-01-03",
            "dash_day": "2026-01-03",
            "compact_day": "20260103",
            "sas_date9_day": "2026-01-03",
            "sas_days_day": "2026-01-03",
        },
    ]


def test_right_partition_predicates_execute_in_databricks(dbx_sql):
    predicates = {
        "native_matches": dates.partition_where(
            "native_dt", ["2026-01-02"], dialect="databricks", date_type="date"
        ),
        "compact_matches": dates.partition_where(
            "compact_dt",
            ["2026-01-02"],
            dialect="databricks",
            date_type={"type": "string", "format": "compact"},
        ),
        "sas_matches": dates.bounds_where(
            "sas_days",
            dialect="databricks",
            date_type={"type": "sas", "format": "days"},
            from_date="2026-01-02",
            to_date="2026-01-02",
        ),
    }
    projection = ",\n  ".join(
        f"SUM(CASE WHEN {predicate} THEN 1 ELSE 0 END) AS {alias}"
        for alias, predicate in predicates.items()
    )

    rows = dbx_sql.execute(f"{SOURCE_SQL}\nSELECT\n  {projection}\nFROM source")

    assert rows == [{"native_matches": "1", "compact_matches": "1", "sas_matches": "1"}]
