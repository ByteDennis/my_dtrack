import pytest

from pair_recon.app.catalog import Catalog


class Sql:
    def __init__(self):
        self.calls = []

    def execute(self, statement, parameters=None):
        self.calls.append((statement, parameters or {}))
        return []


@pytest.mark.parametrize("state", ["running", "complete", "failed"])
def test_status(state):
    sql = Sql()
    catalog = Catalog(sql, "main.recon.pair_meta", "main.recon.run_status", "main.recon.run_error")
    catalog.status(
        run_id="run-1",
        pair="orders",
        owner="me",
        state=state,
        timezone="America/New_York",
    )
    statement, values = sql.calls[-1]
    assert "MERGE INTO main.recon.run_status" in statement
    assert values["run_id"] == "run-1" and values["state"] == state
