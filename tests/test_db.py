"""Tests for database operations"""

import pytest
import sqlite3
import tempfile
import os
from pathlib import Path
from dtrack.db import (
    init_database,
    insert_row_counts,
    upsert_row_counts,
    get_row_counts,
    insert_col_stats,
    get_col_stats,
    update_metadata,
    get_metadata,
    list_tables,
    register_table_pair,
    get_table_pair,
    list_table_pairs,
    insert_column_meta,
    get_column_meta,
    generic_upsert,
    generic_update,
    generic_delete,
    parse_where_clause,
)


@pytest.fixture
def test_db():
    """Create a temporary database for testing"""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    yield path
    # Cleanup
    if os.path.exists(path):
        os.unlink(path)


class TestInitDatabase:
    """Test database initialization"""

    def test_init_creates_database(self, test_db):
        """Test that init_database creates a new database file"""
        # Remove the temp file first
        os.unlink(test_db)
        assert not os.path.exists(test_db)

        init_database(test_db)
        assert os.path.exists(test_db)

    def test_init_creates_metadata_table(self, test_db):
        """Test that _metadata table is created"""
        init_database(test_db)
        conn = sqlite3.connect(test_db)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='_metadata'
        """)
        result = cursor.fetchone()
        conn.close()
        assert result is not None

    def test_init_creates_col_stats_table(self, test_db):
        """Test that _col_stats table is created"""
        init_database(test_db)
        conn = sqlite3.connect(test_db)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='_col_stats'
        """)
        result = cursor.fetchone()
        conn.close()
        assert result is not None


class TestRowCountOperations:
    """Test row count table operations"""

    def test_init_creates_row_counts_table(self, test_db):
        """Test that _row_counts table is created on init"""
        init_database(test_db)
        conn = sqlite3.connect(test_db)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='_row_counts'
        """)
        result = cursor.fetchone()
        conn.close()
        assert result is not None

    def test_insert_row_counts(self, test_db):
        """Test inserting row counts"""
        init_database(test_db)

        data = [
            ("2022-01-01", 100),
            ("2022-01-02", 150),
        ]
        insert_row_counts(test_db, "test_table", data)

        rows = get_row_counts(test_db, "test_table")
        assert len(rows) == 2
        assert rows[0] == ("2022-01-01", 100)
        assert rows[1] == ("2022-01-02", 150)

    def test_upsert_row_counts(self, test_db):
        """Test upserting row counts (update existing, insert new)"""
        init_database(test_db)

        # Initial insert
        data = [("2022-01-01", 100)]
        insert_row_counts(test_db, "test_table", data)

        # Upsert: update existing and add new
        new_data = [
            ("2022-01-01", 200),  # Update
            ("2022-01-02", 150),  # Insert
        ]
        upsert_row_counts(test_db, "test_table", new_data)

        rows = get_row_counts(test_db, "test_table")
        assert len(rows) == 2
        assert rows[0] == ("2022-01-01", 200)
        assert rows[1] == ("2022-01-02", 150)


class TestColStatsOperations:
    """Test column statistics operations"""

    def test_insert_col_stats(self, test_db):
        """Test inserting column statistics"""
        init_database(test_db)

        stats = {
            "source_table": "test_table",
            "column_name": "amount",
            "dt": "2022-01-01",
            "col_type": "numeric",
            "n_total": 100,
            "n_missing": 5,
            "n_unique": 80,
            "mean": 1500.50,
            "std": 200.25,
            "min_val": "100.00",
            "max_val": "5000.00",
            "top_10": None,
        }
        insert_col_stats(test_db, [stats])

        results = get_col_stats(test_db, "test_table")
        assert len(results) == 1
        assert results[0]["column_name"] == "amount"
        assert float(results[0]["mean"]) == 1500.50

    def test_insert_categorical_col_stats(self, test_db):
        """Test inserting categorical column statistics"""
        init_database(test_db)

        stats = {
            "source_table": "test_table",
            "column_name": "status",
            "dt": "2022-01-01",
            "col_type": "categorical",
            "n_total": 100,
            "n_missing": 0,
            "n_unique": 3,
            "mean": None,
            "std": None,
            "min_val": "ACTIVE",
            "max_val": "PENDING",
            "top_10": '[{"value": "ACTIVE", "count": 80}]',
        }
        insert_col_stats(test_db, [stats])

        results = get_col_stats(test_db, "test_table")
        assert len(results) == 1
        assert results[0]["column_name"] == "status"
        assert results[0]["col_type"] == "categorical"


class TestMetadataOperations:
    """Test metadata operations"""

    def test_update_and_get_metadata(self, test_db):
        """Test updating and retrieving metadata"""
        init_database(test_db)

        metadata = {
            "table_name": "test_table",
            "source": "aws",
            "db": "prod_db",
            "source_table": "schema.customer_daily",
            "date_var": "eff_dt",
            "source_file": "test.csv",
            "row_count_total": 1000,
            "load_mode": "upsert",
            "vintage": "day",
            "data_type": "row",
        }
        update_metadata(test_db, metadata)

        result = get_metadata(test_db, "test_table")
        assert result is not None
        assert result["source"] == "aws"
        assert result["db"] == "prod_db"
        assert result["row_count_total"] == 1000

    def test_list_tables(self, test_db):
        """Test listing all tables with metadata"""
        init_database(test_db)
        upsert_row_counts(test_db, "table1", [("2025-01-01", 100)])
        upsert_row_counts(test_db, "table2", [("2025-01-01", 200)])

        update_metadata(test_db, {
            "table_name": "table1",
            "source": "aws",
            "data_type": "row",
        })
        update_metadata(test_db, {
            "table_name": "table2",
            "source": "pcds",
            "data_type": "row",
        })

        tables = list_tables(test_db)
        table_names = [t["table_name"] for t in tables]
        assert "table1" in table_names
        assert "table2" in table_names
        assert "_metadata" not in table_names
        assert "_col_stats" not in table_names
        assert "_row_counts" not in table_names


class TestTablePairOperations:
    """Test table pair registration and retrieval"""

    def test_register_table_pair(self, test_db):
        """Test registering a table pair"""
        init_database(test_db)

        col_mappings = {
            "AMT": "amount",
            "CUST_STATUS": "customer_status"
        }

        register_table_pair(
            test_db,
            pair_name="customer_daily",
            table_left="customer_daily_oracle",
            table_right="customer_daily_aws",
            source_left="oracle",
            source_right="aws",
            col_mappings=col_mappings
        )

        # Retrieve and verify
        pair = get_table_pair(test_db, "customer_daily")
        assert pair is not None
        assert pair["pair_name"] == "customer_daily"
        assert pair["table_left"] == "customer_daily_oracle"
        assert pair["table_right"] == "customer_daily_aws"
        assert pair["source_left"] == "oracle"
        assert pair["source_right"] == "aws"
        assert pair["col_mappings"]["AMT"] == "amount"

    def test_register_pair_without_mappings(self, test_db):
        """Test registering a pair without column mappings"""
        init_database(test_db)

        register_table_pair(
            test_db,
            pair_name="test_pair",
            table_left="table1",
            table_right="table2"
        )

        pair = get_table_pair(test_db, "test_pair")
        assert pair is not None
        assert pair["col_mappings"] == {}

    def test_get_nonexistent_pair(self, test_db):
        """Test getting a pair that doesn't exist"""
        init_database(test_db)
        pair = get_table_pair(test_db, "nonexistent")
        assert pair is None

    def test_list_table_pairs(self, test_db):
        """Test listing all table pairs"""
        init_database(test_db)

        register_table_pair(
            test_db,
            pair_name="pair1",
            table_left="t1",
            table_right="t2",
            col_mappings={"A": "a"}
        )

        register_table_pair(
            test_db,
            pair_name="pair2",
            table_left="t3",
            table_right="t4"
        )

        pairs = list_table_pairs(test_db)
        assert len(pairs) == 2

        pair_names = [p["pair_name"] for p in pairs]
        assert "pair1" in pair_names
        assert "pair2" in pair_names

    def test_update_existing_pair(self, test_db):
        """Test that registering same pair name updates it"""
        init_database(test_db)

        # Register initial
        register_table_pair(
            test_db,
            pair_name="test",
            table_left="t1",
            table_right="t2"
        )

        # Update with new mapping
        register_table_pair(
            test_db,
            pair_name="test",
            table_left="t1_new",
            table_right="t2_new",
            col_mappings={"X": "x"}
        )

        # Should only have one pair
        pairs = list_table_pairs(test_db)
        assert len(pairs) == 1

        # Should have updated values
        pair = get_table_pair(test_db, "test")
        assert pair["table_left"] == "t1_new"
        assert pair["col_mappings"]["X"] == "x"


class TestColumnMetaOperations:
    """Test column metadata operations"""

    def test_insert_and_get_column_meta(self, test_db):
        """Test inserting and retrieving column metadata"""
        init_database(test_db)

        columns = {"AMT": "NUMBER", "STATUS": "VARCHAR2", "EFF_DT": "DATE"}
        count = insert_column_meta(test_db, "customer_daily", columns, source="pcds")

        assert count == 3
        meta = get_column_meta(test_db, "customer_daily")
        assert len(meta) == 3
        names = {m["column_name"] for m in meta}
        assert names == {"AMT", "STATUS", "EFF_DT"}
        assert meta[0]["source"] == "pcds"

    def test_upsert_column_meta(self, test_db):
        """Test that inserting same column updates it"""
        init_database(test_db)

        insert_column_meta(test_db, "tbl", {"COL1": "INT"}, source="pcds")
        insert_column_meta(test_db, "tbl", {"COL1": "BIGINT"}, source="aws")

        meta = get_column_meta(test_db, "tbl")
        assert len(meta) == 1
        assert meta[0]["data_type"] == "BIGINT"
        assert meta[0]["source"] == "aws"

    def test_get_column_meta_empty(self, test_db):
        """Test getting columns for nonexistent table"""
        init_database(test_db)
        meta = get_column_meta(test_db, "nonexistent")
        assert meta == []


class TestGenericUpsertDelete:
    """Test generic upsert and delete operations"""

    def test_generic_upsert_insert(self, test_db):
        """Test inserting a new row via generic_upsert"""
        init_database(test_db)
        count = generic_upsert(test_db, "_row_counts", {
            "source_table": "tbl1", "dt": "2025-01-01", "row_count": "500"
        })
        assert count == 1
        rows = get_row_counts(test_db, "tbl1")
        assert len(rows) == 1
        assert rows[0] == ("2025-01-01", 500)

    def test_generic_upsert_update(self, test_db):
        """Test updating an existing row via generic_upsert"""
        init_database(test_db)
        insert_row_counts(test_db, "tbl1", [("2025-01-01", 100)])
        generic_upsert(test_db, "_row_counts", {
            "source_table": "tbl1", "dt": "2025-01-01", "row_count": "999"
        })
        rows = get_row_counts(test_db, "tbl1")
        assert len(rows) == 1
        assert rows[0] == ("2025-01-01", 999)

    def test_generic_delete(self, test_db):
        """Test deleting rows via generic_delete"""
        init_database(test_db)
        insert_row_counts(test_db, "tbl1", [("2025-01-01", 100), ("2025-01-02", 200)])
        count = generic_delete(test_db, "_row_counts", {"source_table": "tbl1", "dt": "2025-01-01"})
        assert count == 1
        rows = get_row_counts(test_db, "tbl1")
        assert len(rows) == 1
        assert rows[0] == ("2025-01-02", 200)

    def test_generic_upsert_invalid_table(self, test_db):
        """Test that upserting to nonexistent table raises error"""
        init_database(test_db)
        with pytest.raises(ValueError, match="does not exist"):
            generic_upsert(test_db, "no_such_table", {"col": "val"})

    def test_generic_delete_invalid_table(self, test_db):
        """Test that deleting from nonexistent table raises error"""
        init_database(test_db)
        with pytest.raises(ValueError, match="does not exist"):
            generic_delete(test_db, "no_such_table", {"col": "val"})

    def test_generic_upsert_metadata(self, test_db):
        """Test upserting into _metadata table"""
        init_database(test_db)
        generic_upsert(test_db, "_metadata", {
            "table_name": "foo", "source": "oracle", "db": "prod"
        })
        meta = get_metadata(test_db, "foo")
        assert meta is not None
        assert meta["source"] == "oracle"
        assert meta["db"] == "prod"

    def test_generic_upsert_update_preserves_columns(self, test_db):
        """Test that updating only touches provided non-PK columns"""
        init_database(test_db)
        # Insert full record
        generic_upsert(test_db, "_metadata", {
            "table_name": "foo", "source": "oracle", "db": "prod"
        })
        # Update only source — db should stay "prod"
        generic_upsert(test_db, "_metadata", {
            "table_name": "foo", "source": "aws"
        })
        meta = get_metadata(test_db, "foo")
        assert meta["source"] == "aws"
        assert meta["db"] == "prod"

    def test_generic_upsert_bad_column(self, test_db):
        """Test that unknown column names raise error"""
        init_database(test_db)
        with pytest.raises(ValueError, match="Unknown column"):
            generic_upsert(test_db, "_metadata", {
                "table_name": "foo", "nonexistent_col": "bar"
            })

    def test_generic_upsert_missing_pk(self, test_db):
        """Test that missing PK columns raise error"""
        init_database(test_db)
        with pytest.raises(ValueError, match="Missing primary key"):
            generic_upsert(test_db, "_metadata", {"source": "oracle"})

    def test_generic_delete_bad_column(self, test_db):
        """Test that unknown column in delete raises error"""
        init_database(test_db)
        with pytest.raises(ValueError, match="Unknown column"):
            generic_delete(test_db, "_metadata", {"bad_col": "val"})

    def test_generic_update_explicit(self, test_db):
        """Test explicit update with WHERE + SET separation"""
        init_database(test_db)
        insert_row_counts(test_db, "tbl1", [("2025-01-01", 100), ("2025-01-02", 200)])
        count = generic_update(
            test_db, "_row_counts",
            where={"source_table": "tbl1", "dt": "2025-01-01"},
            updates={"row_count": "999"},
        )
        assert count == 1
        rows = get_row_counts(test_db, "tbl1")
        assert rows[0] == ("2025-01-01", 999)
        assert rows[1] == ("2025-01-02", 200)

    def test_generic_update_multiple_rows(self, test_db):
        """Test update matching multiple rows"""
        init_database(test_db)
        insert_row_counts(test_db, "tbl1", [("2025-01-01", 100), ("2025-01-02", 200)])
        count = generic_update(
            test_db, "_row_counts",
            where={"source_table": "tbl1"},
            updates={"row_count": "0"},
        )
        assert count == 2
        rows = get_row_counts(test_db, "tbl1")
        assert all(c == 0 for _, c in rows)

    def test_generic_update_bad_column(self, test_db):
        """Test that unknown column in update raises error"""
        init_database(test_db)
        with pytest.raises(ValueError, match="Unknown column"):
            generic_update(test_db, "_metadata", {"table_name": "foo"}, {"bad": "val"})

    def test_delete_with_like_operator(self, test_db):
        """Test delete with LIKE pattern"""
        init_database(test_db)
        insert_row_counts(test_db, "pcds_cust", [("2025-01-01", 100)])
        insert_row_counts(test_db, "aws_cust", [("2025-01-01", 200)])
        insert_row_counts(test_db, "other", [("2025-01-01", 300)])
        count = generic_delete(test_db, "_row_counts", {"source_table": "~=%_cust"})
        assert count == 2
        rows = get_row_counts(test_db, "other")
        assert len(rows) == 1

    def test_delete_with_in_operator(self, test_db):
        """Test delete with IN (comma-separated values)"""
        init_database(test_db)
        insert_row_counts(test_db, "tbl1", [("2025-01-01", 100)])
        insert_row_counts(test_db, "tbl2", [("2025-01-01", 200)])
        insert_row_counts(test_db, "tbl3", [("2025-01-01", 300)])
        count = generic_delete(test_db, "_row_counts", {"source_table": "tbl1,tbl3"})
        assert count == 2
        rows = get_row_counts(test_db, "tbl2")
        assert len(rows) == 1

    def test_delete_with_not_equal(self, test_db):
        """Test delete with != operator"""
        init_database(test_db)
        insert_row_counts(test_db, "keep", [("2025-01-01", 100)])
        insert_row_counts(test_db, "drop1", [("2025-01-01", 200)])
        insert_row_counts(test_db, "drop2", [("2025-01-01", 300)])
        count = generic_delete(test_db, "_row_counts", {"source_table": "!=keep"})
        assert count == 2
        rows = get_row_counts(test_db, "keep")
        assert len(rows) == 1

    def test_update_with_like_operator(self, test_db):
        """Test update with LIKE pattern in WHERE"""
        init_database(test_db)
        insert_row_counts(test_db, "pcds_a", [("2025-01-01", 100)])
        insert_row_counts(test_db, "pcds_b", [("2025-01-01", 200)])
        insert_row_counts(test_db, "aws_c", [("2025-01-01", 300)])
        count = generic_update(
            test_db, "_row_counts",
            where={"source_table": "~=pcds_%"},
            updates={"row_count": "0"},
        )
        assert count == 2
        assert get_row_counts(test_db, "aws_c")[0] == ("2025-01-01", 300)


class TestParseWhereClause:
    """Test WHERE clause parsing with operators"""

    def test_exact_match(self):
        sql, params = parse_where_clause({"col": "val"})
        assert sql == "col = ?"
        assert params == ["val"]

    def test_not_equal(self):
        sql, params = parse_where_clause({"col": "!=val"})
        assert sql == "col != ?"
        assert params == ["val"]

    def test_like(self):
        sql, params = parse_where_clause({"col": "~=%pattern%"})
        assert sql == "col LIKE ?"
        assert params == ["%pattern%"]

    def test_not_like(self):
        sql, params = parse_where_clause({"col": "!~=test%"})
        assert sql == "col NOT LIKE ?"
        assert params == ["test%"]

    def test_in_list(self):
        sql, params = parse_where_clause({"col": "a,b,c"})
        assert sql == "col IN (?, ?, ?)"
        assert params == ["a", "b", "c"]

    def test_combined(self):
        sql, params = parse_where_clause({"a": "val", "b": "~=%x%"})
        assert "a = ?" in sql
        assert "b LIKE ?" in sql
        assert params == ["val", "%x%"]

    def test_auto_like_from_percent(self):
        sql, params = parse_where_clause({"col": "%_cust_daily"})
        assert sql == "col LIKE ?"
        assert params == ["%_cust_daily"]


    def test_init_creates_column_meta_table(self, test_db):
        """Test that _column_meta table is created on init"""
        init_database(test_db)
        conn = sqlite3.connect(test_db)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='_column_meta'
        """)
        result = cursor.fetchone()
        conn.close()
        assert result is not None
