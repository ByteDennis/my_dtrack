from types import SimpleNamespace as NS

import pytest

from dtrack_local.warehouse import Warehouse


def _response(state, *, rows=None, links=None, following=None, error=None):
    return NS(
        statement_id="s1",
        status=NS(state=NS(value=state), error=NS(message=error) if error else None),
        manifest=NS(schema=NS(columns=[NS(name="a"), NS(name="b")])),
        result=NS(data_array=rows, external_links=links, next_chunk_index=following),
    )


# >>> a statement_execution stand-in: a queue of statement states and numbered result chunks <<< #
def _client(states, chunks=None):
    calls = []
    states = list(states)
    api = NS(
        execute_statement=lambda **kwargs: calls.append(kwargs) or states.pop(0),
        get_statement=lambda statement_id: states.pop(0),
        get_statement_result_chunk_n=lambda statement_id, index: chunks[index],
    )
    return NS(statement_execution=api), calls


def test_inline_rows_follow_every_chunk_after_polling():
    client, calls = _client(
        [_response("PENDING"), _response("SUCCEEDED", rows=[["1", "x"]], following=1)],
        chunks={1: NS(data_array=[["2", None]], external_links=None, next_chunk_index=None)},
    )
    warehouse = Warehouse(client, "wh", disposition="inline", poll=0)
    assert warehouse.rows("SELECT a, b FROM t") == [("1", "x"), ("2", None)]
    assert calls[0]["disposition"].value == "INLINE" and calls[0]["format"].value == "JSON_ARRAY"


def test_statement_is_audited_before_submission():
    events = []
    response = _response("SUCCEEDED", rows=[])
    api = NS(
        execute_statement=lambda **kwargs: events.append(("execute", kwargs["statement"]))
        or response,
    )
    audit = NS(record=lambda sql, **meta: events.append(("audit", sql, meta)))
    Warehouse(NS(statement_execution=api), "wh", sql_audit=audit).rows("SELECT * FROM huge_table")
    assert events == [
        (
            "audit",
            "SELECT * FROM huge_table",
            {"backend": "databricks-sql", "operation": "query"},
        ),
        ("execute", "SELECT * FROM huge_table"),
    ]


def test_external_links_are_downloaded_in_order_and_written_like_a_left_pull(tmp_path):
    links = [NS(external_link="u0", next_chunk_index=1)]
    client, _ = _client(
        [_response("SUCCEEDED", links=links)],
        chunks={1: NS(external_links=[NS(external_link="u1", next_chunk_index=None)])},
    )
    fetched = []
    pages = {"u0": [["1", "x"]], "u1": [["2", None], ["3", "z"]]}
    warehouse = Warehouse(client, "wh", fetch=lambda url: fetched.append(url) or pages[url])
    assert warehouse.to_csv("SELECT 1", tmp_path / "w" / "r.csv", ["a", "b"]) == 3
    assert fetched == ["u0", "u1"]
    assert (tmp_path / "w" / "r.csv").read_text() == "a,b\n1,x\n2,\n3,z\n"


def test_failures_carry_the_warehouse_error_and_bad_settings_fail_early():
    client, _ = _client([_response("FAILED", error="TABLE_OR_VIEW_NOT_FOUND")])
    with pytest.raises(RuntimeError, match="FAILED: TABLE_OR_VIEW_NOT_FOUND"):
        Warehouse(client, "wh").rows("SELECT * FROM nope")
    with pytest.raises(ValueError, match="disposition"):
        Warehouse(client, "wh", disposition="cloud")
    with pytest.raises(ValueError, match="DTRACK_DBX_WAREHOUSE_ID"):
        Warehouse(client, "")
