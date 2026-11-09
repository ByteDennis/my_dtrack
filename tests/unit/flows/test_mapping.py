import json
from types import SimpleNamespace as NS

import pytest

from pair_recon.core.metadata.mapping import (
    ColumnInfo,
    automatch_columns,
    discover_csv_columns,
    discover_csv_relations,
    discover_databricks_columns,
    discover_hadoop_columns,
    discover_oracle_columns,
    upsert_column_mappings,
)


def test_discover_oracle_columns_uses_real_query_description():
    events = []

    class Cursor:
        def __init__(self):
            self.description = [
                ("ORDER_ID", NS(name="DB_TYPE_NUMBER")),
                ("POSTED_AT", NS(name="DATE")),
            ]

        def execute(self, statement):
            events.append(statement)

        def close(self):
            events.append("cursor closed")

    class Connection:
        def cursor(self):
            return Cursor()

        def close(self):
            events.append("connection closed")

    class Source:
        def __init__(self, _config):
            pass

        def connect(self):
            return Connection()

        def relation(self):
            return "LEFT_ORDERS"

        def _audit(self, statement, operation):
            events.append((operation, statement))

    columns = discover_oracle_columns(
        {"source": "oracle", "table": "LEFT_ORDERS"}, source_factory=Source
    )

    assert columns == [
        ColumnInfo("ORDER_ID", "DB_TYPE_NUMBER"),
        ColumnInfo("POSTED_AT", "DATE"),
    ]
    assert "SELECT * FROM LEFT_ORDERS WHERE 1=0" in events


def test_discover_hadoop_columns_uses_odbc_description():
    events = []

    class Cursor:
        def __init__(self):
            self.description = [("ACCOUNT_ID", str), ("EFF_DT", NS(name="DATE"))]

        def execute(self, statement):
            events.append(statement)

        def close(self):
            events.append("cursor closed")

    class Connection:
        def cursor(self):
            return Cursor()

        def close(self):
            events.append("connection closed")

    class Source:
        def __init__(self, _config):
            pass

        def connect(self):
            return Connection()

        def relation(self):
            return "`risk`.`accounts`"

        def _audit(self, statement, operation):
            events.append((operation, statement))

    columns = discover_hadoop_columns(
        {"source": "hadoop", "schema": "risk", "table": "accounts"},
        source_factory=Source,
    )

    assert columns == [ColumnInfo("ACCOUNT_ID", "str"), ColumnInfo("EFF_DT", "DATE")]
    assert "SELECT * FROM `risk`.`accounts` WHERE 1=0" in events


def test_discover_csv_columns_and_relation(tmp_path):
    path = tmp_path / "orders.csv"
    path.write_text("ORDER_ID,POSTED_AT\n1,2026-01-01\n", encoding="utf-8")
    config = {"source": "csv", "path": str(path), "partition": "all"}

    columns = discover_csv_columns(config)

    assert [column.name for column in columns] == ["ORDER_ID", "POSTED_AT"]
    assert all(column.data_type == "VARCHAR" for column in columns)
    assert discover_csv_relations(config) == {
        "source": "csv",
        "relation": str(path),
        "path": str(path),
        "table": "orders.csv",
        "primary_keys": [],
    }


def test_discover_databricks_columns_and_automatch():
    class Sql:
        def execute(self, statement, parameters=None):
            assert statement == "DESCRIBE QUERY SELECT * FROM `main`.`sales`.`orders`"
            return [
                {"col_name": "order_id", "data_type": "bigint"},
                {"col_name": "postedAt", "data_type": "timestamp"},
                {"col_name": "# Partition Information", "data_type": None},
            ]

    right = discover_databricks_columns(
        Sql(),
        {
            "source": "databricks",
            "catalog": "main",
            "schema": "sales",
            "table": "orders",
        },
    )
    rows = automatch_columns(
        [ColumnInfo("ORDER_ID", "NUMBER"), ColumnInfo("POSTED_AT", "DATE"), "NO_MATCH"],
        right,
    )

    assert [(row["right_column"], row["match_method"], row["status"]) for row in rows] == [
        ("order_id", "casefold", "matched"),
        ("postedAt", "normalized", "matched"),
        (None, None, "unmatched"),
    ]


def test_automatch_rejects_ambiguous_normalized_name():
    rows = automatch_columns(["ORDER_ID"], ["order-id", "order id"])
    assert rows[0]["status"] == "ambiguous" and rows[0]["right_column"] is None

    rows = automatch_columns(["ORDER_ID", "orderId"], ["order_id"])
    assert [row["status"] for row in rows] == ["ambiguous", "ambiguous"]


def test_upsert_column_mappings():
    calls = []
    sql = NS(execute=lambda statement, parameters: calls.append((statement, parameters)))

    result = upsert_column_mappings(
        sql,
        "main.recon.pair_recon_column_map",
        "orders",
        [{"left_column": "ORDER_ID", "right_column": "order_id", "data_type": "numeric"}],
    )

    assert result["rows"] == 1
    assert "MERGE INTO main.recon.pair_recon_column_map" in calls[0][0]
    assert "WHEN NOT MATCHED BY SOURCE AND t.pair_name = :pair_name THEN DELETE" in calls[0][0]
    assert calls[0][1]["pair_name"] == "orders"
    assert json.loads(calls[0][1]["mappings_json"])[0]["data_type"] == "numeric"

    with pytest.raises(ValueError, match="invalid table name"):
        upsert_column_mappings(sql, "bad", "orders", [])


def test_upsert_column_mappings_batches_rows_in_one_write():
    calls = []
    sql = NS(execute=lambda statement, parameters: calls.append((statement, parameters)))

    result = upsert_column_mappings(
        sql,
        "main.recon.pair_recon_column_map",
        "orders",
        [
            {"left_column": "ID", "right_column": "id", "data_type": "numeric"},
            {"left_column": "NAME", "right_column": "name", "data_type": "categorical"},
        ],
    )

    assert result["rows"] == 2
    assert len(calls) == 1
    statement, parameters = calls[0]
    assert "explode(from_json(:mappings_json" in statement
    assert json.loads(parameters["mappings_json"])[0]["left_column"] == "ID"
    assert json.loads(parameters["mappings_json"])[1]["left_column"] == "NAME"


@pytest.mark.parametrize("overwrite", [True, False])
@pytest.mark.parametrize("size", [0, 1, 501])
@pytest.mark.parametrize("pair_name", ["orders", "o'rders"])
def test_mapping_write_replaces_or_patches_only_selected_pair_in_one_statement(
    overwrite, size, pair_name
):
    import duckdb

    with duckdb.connect() as connection:
        connection.execute(
            "CREATE TABLE column_map (pair_name VARCHAR, left_column VARCHAR, right_column VARCHAR, data_type VARCHAR)"
        )
        connection.executemany(
            "INSERT INTO column_map VALUES (?, ?, ?, ?)",
            [
                (pair_name, "C0", "old", None),
                (pair_name, "OBSOLETE", "obsolete", None),
                ("other", "C0", "keep", None),
                (pair_name + "_archive", "C0", "archive", None),
            ],
        )
        calls = []

        def execute(statement, parameters):
            calls.append((statement, parameters))
            # Adapt the Spark JSON source and SQL dialect for a local execution
            # of the generated MERGE, retaining its actual deletion condition.
            columns = ("pair_name", "left_column", "right_column", "data_type")
            source = ", ".join(
                f"json_extract_string(value, '$.{name}') AS {name}" for name in columns
            )
            source = f"SELECT {source} FROM json_each($mappings_json)"
            assert statement.startswith("MERGE INTO memory.main.column_map")
            assert "INSERT OVERWRITE" not in statement
            assert ("WHEN NOT MATCHED BY SOURCE" in statement) is overwrite
            start = statement.index("USING (")
            end = statement.index(") AS s\nON ", start) + len(") AS s")
            statement = statement[:start] + f"USING ({source}) AS s" + statement[end:]
            statement = (
                statement.replace("t.right_column =", "right_column =")
                .replace("t.data_type =", "data_type =")
                .replace(":pair_name", "$pair_name")
            )
            connection.execute(statement, parameters)

        rows = [
            {"left_column": f"C{i}", "right_column": f"c{i}", "data_type": "string"}
            for i in range(size)
        ]
        upsert_column_mappings(
            NS(execute=execute),
            "memory.main.column_map",
            pair_name,
            iter(rows),
            overwrite=overwrite,
        )
        assert len(calls) == (1 if size or overwrite else 0)
        actual = connection.execute(
            "SELECT left_column, right_column FROM column_map WHERE pair_name=? ORDER BY left_column",
            [pair_name],
        ).fetchall()
        expected = {row["left_column"]: row["right_column"] for row in rows}
        if not overwrite:
            expected = {"C0": "old", "OBSOLETE": "obsolete", **expected}
        assert actual == sorted(expected.items())
        others = connection.execute("SELECT * FROM column_map WHERE pair_name='other'").fetchall()
        assert others == [("other", "C0", "keep", None)]
        assert connection.execute(
            "SELECT * FROM column_map WHERE pair_name=?", [pair_name + "_archive"]
        ).fetchall() == [(pair_name + "_archive", "C0", "archive", None)]


def test_mapping_validation_finishes_before_any_write():
    calls = []
    sql = NS(execute=lambda *args: calls.append(args))
    rows = [{"left_column": f"C{i}", "right_column": f"c{i}"} for i in range(501)]
    rows.append({"left_column": "C0", "right_column": "duplicate"})
    with pytest.raises(ValueError, match="duplicate left column"):
        upsert_column_mappings(sql, "main.recon.column_map", "orders", rows)
    assert calls == []
    with pytest.raises(TypeError, match="overwrite"):
        upsert_column_mappings(sql, "main.recon.column_map", "orders", [], overwrite="false")
    assert calls == []
