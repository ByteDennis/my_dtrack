"""Tests for the _playground_history schema + CRUD helpers in dtrack.db."""

import os
import tempfile

import pytest

from dtrack.db import (
    init_database,
    insert_playground_run,
    list_playground_runs,
    update_playground_note,
    delete_playground_run,
)


@pytest.fixture
def fresh_db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    os.unlink(path)
    init_database(path)
    yield path
    if os.path.exists(path):
        os.unlink(path)


def test_insert_and_list(fresh_db):
    rid = insert_playground_run(
        fresh_db,
        engine="athena", conn="my_db", sql="SELECT 1",
        elapsed_sec=1.23, n_rows=1, status="ok",
    )
    assert rid >= 1
    rows = list_playground_runs(fresh_db)
    assert len(rows) == 1
    assert rows[0]["engine"] == "athena"
    assert rows[0]["conn"] == "my_db"
    assert rows[0]["sql"] == "SELECT 1"
    assert rows[0]["elapsed_sec"] == pytest.approx(1.23)
    assert rows[0]["status"] == "ok"
    assert rows[0]["note"] == ""
    assert rows[0]["ts_utc"]


def test_list_returns_newest_first(fresh_db):
    a = insert_playground_run(fresh_db, engine="athena", conn=None,
                              sql="A", elapsed_sec=0.1, n_rows=1, status="ok")
    b = insert_playground_run(fresh_db, engine="oracle", conn="pb23",
                              sql="B", elapsed_sec=0.2, n_rows=2, status="ok")
    rows = list_playground_runs(fresh_db)
    assert [r["id"] for r in rows] == [b, a]


def test_update_note_round_trip(fresh_db):
    rid = insert_playground_run(fresh_db, engine="oracle", conn="pb23",
                                sql="x", elapsed_sec=0.5, n_rows=1, status="ok")
    assert update_playground_note(fresh_db, rid, "hello") is True
    rows = list_playground_runs(fresh_db)
    assert rows[0]["note"] == "hello"


def test_update_note_returns_false_for_missing(fresh_db):
    assert update_playground_note(fresh_db, 9999, "nope") is False


def test_delete(fresh_db):
    rid = insert_playground_run(fresh_db, engine="sas", conn="both",
                                sql="gen", elapsed_sec=None, n_rows=None,
                                status="ok")
    assert delete_playground_run(fresh_db, rid) is True
    assert list_playground_runs(fresh_db) == []
    assert delete_playground_run(fresh_db, rid) is False


def test_error_status_with_message(fresh_db):
    rid = insert_playground_run(fresh_db, engine="athena", conn="db",
                                sql="bogus", elapsed_sec=0.05, n_rows=None,
                                status="error", error_msg="syntax error near 'bogus'")
    rows = list_playground_runs(fresh_db)
    assert rows[0]["status"] == "error"
    assert rows[0]["error_msg"] == "syntax error near 'bogus'"
    assert rows[0]["n_rows"] is None
