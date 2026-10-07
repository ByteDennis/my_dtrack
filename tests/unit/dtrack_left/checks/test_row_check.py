from datetime import UTC, datetime

import pytest
from support import MemoryStore

from dtrack_left.checks.row_check import (
    SECRET_FIELDS,
    make_run_id,
    public_pair_config,
    run_row_check,
)


class Fake:
    def preflight(self):
        return {"ok": True}

    def partition_counts(self, *_args):
        return {"2026-01-01": 1, "20260102": 2}


def test_row_check_publishes_counts_and_public_config_without_secrets():
    config = {
        "pairs": {
            "p": {
                "left": {
                    "source": "oracle",
                    "table": "A",
                    "date_col": "dt",
                    "dsn": "host:1521/SVC",
                    "user_env": "PCDS_USER",
                    "password": "secret",
                },
                "right": {
                    "source": "databricks",
                    "catalog": "c",
                    "schema": "s",
                    "table": "a",
                    "date_col": "dt",
                    "server_hostname": "adb.example.net",
                    "access_token_env": "TOK",
                },
                "col_map": {"amount": "amount"},
                "sample": 0.1,
                "key": "id",
            }
        }
    }
    store = MemoryStore()
    run_id, documents = run_row_check(
        config, store, run_id="run", source_factory=lambda _row: Fake()
    )
    doc = store.get_json("row_check/p.left.json")
    assert run_id == "run" and len(documents) == 1
    assert doc["counts"] == {"2026-01-01": 1, "20260102": 2}
    assert doc["pair_config"]["sample"] == 0.1
    assert "dsn" not in doc["left"] and "password" not in doc["left"]
    assert "user_env" not in doc["left"]
    assert doc["right"] == {
        "source": "databricks",
        "catalog": "c",
        "schema": "s",
        "table": "a",
        "date_col": "dt",
    }
    assert store.get_json("row_check/_LEFT_CLOSED")["pairs"] == ["p"]
    assert not any(key.startswith("manifest/") for key in store.objects)


def _config(**pairs):
    base = {
        "left": {"source": "csv", "path": "x.csv", "date_col": "dt"},
        "right": {"source": "databricks", "table": "t", "date_col": "dt"},
        "col_map": {"a": "a"},
    }
    return {"pairs": {name: {**base, **extra} for name, extra in pairs.items()}}


def test_make_run_id_is_a_utc_stamp_plus_a_config_digest():
    now = datetime(2026, 9, 27, 1, 2, 3, tzinfo=UTC)
    run_id = make_run_id({"pairs": {}}, now=now)
    assert run_id.startswith("20260927T010203Z-") and len(run_id.split("-")[1]) == 6
    assert run_id == make_run_id({"pairs": {}}, now=now)
    assert run_id != make_run_id({"pairs": {"x": {}}}, now=now)
    assert make_run_id({}).endswith(make_run_id({}, now=now)[-7:])


def test_row_check_filters_pairs_and_skips_disabled_ones():
    store = MemoryStore()
    config = _config(a={}, b={}, c={"skip": True})
    run_id, documents = run_row_check(
        config, store, pair_names=["b", "c"], source_factory=lambda _row: Fake()
    )
    assert [doc["pair"] for doc in documents] == ["b"] and run_id
    assert store.get_json("_STATUS.json")["state"] == "left_row_check_closed"
    assert store.exists("_HEARTBEAT")


def test_row_check_refuses_an_empty_selection_and_failed_preflights():
    with pytest.raises(ValueError, match="no active pairs"):
        run_row_check(_config(a={"skip": True}), MemoryStore(), source_factory=lambda _r: Fake())

    class Down(Fake):
        def preflight(self):
            return {"ok": False, "error": "ORA-12541"}

    store = MemoryStore()
    with pytest.raises(RuntimeError, match="left preflight failed: a"):
        run_row_check(_config(a={}), store, source_factory=lambda _r: Down())
    assert not store.exists("row_check/a.left.json")


def test_public_pair_config_strips_every_credential_field():
    side = {name: "x" for name in SECRET_FIELDS} | {"token_env": "T", "table": "t"}
    public = public_pair_config({"left": side, "right": side, "sample": 5})
    assert public == {"sample": 5, "left": {"table": "t"}, "right": {"table": "t"}}


def test_public_pair_config_keeps_different_side_date_types_and_expands_custom_names():
    from dtrack_left.common import dates

    dates.register_date_type(
        "test_slash",
        predicate_sql={"oracle": "TO_DATE({column}, 'YYYY/MM/DD')"},
        literal_sql="DATE '{iso}'",
        range="day",
    )
    try:
        public = public_pair_config(
            {
                "left": {"table": "a", "date_type": "test_slash"},
                "right": {
                    "table": "b",
                    "date_type": {"type": "string", "format": "compact"},
                },
            }
        )
        assert public["left"]["date_type"] == {
            "predicate_sql": {"oracle": "TO_DATE({column}, 'YYYY/MM/DD')"},
            "literal_sql": "DATE '{iso}'",
            "range": "day",
        }
        assert public["right"]["date_type"] == {"type": "string", "format": "compact"}
    finally:
        dates.DATE_TYPES.pop("test_slash")
