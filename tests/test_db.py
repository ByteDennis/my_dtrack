"""Tests for database operations"""

import pytest
import sqlite3
import tempfile
import os
from pathlib import Path
from dtrack.db import (
    init_database,
    create_row_count_table,
    insert_row_counts,
    upsert_row_counts,
    get_row_counts,
    insert_col_stats,
    get_col_stats,
    update_metadata,
    get_metadata,
    list_tables,
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

    def test_create_row_count_table(self, test_db):
        """Test creating a row count table"""
        init_database(test_db)
        create_row_count_table(test_db, "test_table")

        conn = sqlite3.connect(test_db)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='test_table'
        """)
        result = cursor.fetchone()
        conn.close()
        assert result is not None

    def test_insert_row_counts(self, test_db):
        """Test inserting row counts"""
        init_database(test_db)
        create_row_count_table(test_db, "test_table")

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
        create_row_count_table(test_db, "test_table")

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
        assert results[0]["mean"] == 1500.50

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
        create_row_count_table(test_db, "table1")
        create_row_count_table(test_db, "table2")

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
        # Should not include _metadata and _col_stats
        table_names = [t["table_name"] for t in tables]
        assert "table1" in table_names
        assert "table2" in table_names
        assert "_metadata" not in table_names
        assert "_col_stats" not in table_names
