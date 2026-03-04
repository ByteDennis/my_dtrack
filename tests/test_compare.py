"""Tests for comparison functionality"""

import pytest
import tempfile
import os
from dtrack.db import (
    init_database,
    create_row_count_table,
    upsert_row_counts,
    register_table_pair,
    insert_col_stats,
)
from dtrack.compare import (
    compare_row_counts,
    compare_column_stats,
    get_column_mapping,
)


@pytest.fixture
def test_db():
    """Create a temporary database for testing"""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    init_database(path)
    yield path
    if os.path.exists(path):
        os.unlink(path)


class TestCompareRowCounts:
    """Test row count comparison"""

    def test_compare_identical_tables(self, test_db):
        """Test comparing two identical tables"""
        # Create two tables with same data
        create_row_count_table(test_db, "table_a")
        create_row_count_table(test_db, "table_b")

        data = [
            ("2025-01-01", 100),
            ("2025-01-02", 150),
            ("2025-01-03", 200),
        ]

        upsert_row_counts(test_db, "table_a", data)
        upsert_row_counts(test_db, "table_b", data)

        result = compare_row_counts(test_db, "table_a", "table_b")

        assert len(result["only_left"]) == 0
        assert len(result["only_right"]) == 0
        assert len(result["matching"]) == 3
        assert len(result["mismatched"]) == 0

    def test_compare_with_only_left(self, test_db):
        """Test comparison with dates only in left table"""
        create_row_count_table(test_db, "table_a")
        create_row_count_table(test_db, "table_b")

        upsert_row_counts(test_db, "table_a", [
            ("2025-01-01", 100),
            ("2025-01-02", 150),
            ("2025-01-03", 200),
        ])

        upsert_row_counts(test_db, "table_b", [
            ("2025-01-02", 150),
        ])

        result = compare_row_counts(test_db, "table_a", "table_b")

        assert len(result["only_left"]) == 2
        assert "2025-01-01" in [d for d, _ in result["only_left"]]
        assert "2025-01-03" in [d for d, _ in result["only_left"]]
        assert len(result["matching"]) == 1

    def test_compare_with_mismatched(self, test_db):
        """Test comparison with different counts"""
        create_row_count_table(test_db, "table_a")
        create_row_count_table(test_db, "table_b")

        upsert_row_counts(test_db, "table_a", [
            ("2025-01-01", 100),
            ("2025-01-02", 150),
        ])

        upsert_row_counts(test_db, "table_b", [
            ("2025-01-01", 100),
            ("2025-01-02", 155),  # Different count
        ])

        result = compare_row_counts(test_db, "table_a", "table_b")

        assert len(result["matching"]) == 1
        assert len(result["mismatched"]) == 1

        # Check mismatch details
        mismatch = result["mismatched"][0]
        assert mismatch[0] == "2025-01-02"
        assert mismatch[1] == 150
        assert mismatch[2] == 155


class TestCompareColumnStats:
    """Test column statistics comparison"""

    def test_compare_numeric_columns(self, test_db):
        """Test comparing numeric column statistics"""
        # Insert stats for table_a
        stats_a = [
            {
                "source_table": "table_a",
                "column_name": "amount",
                "dt": "2025-01-01",
                "col_type": "numeric",
                "n_total": 100,
                "n_missing": 5,
                "n_unique": 80,
                "mean": 1500.0,
                "std": 200.0,
                "min_val": "100",
                "max_val": "5000",
                "top_10": None,
            }
        ]

        # Insert stats for table_b (identical)
        stats_b = [
            {
                "source_table": "table_b",
                "column_name": "amount",
                "dt": "2025-01-01",
                "col_type": "numeric",
                "n_total": 100,
                "n_missing": 5,
                "n_unique": 80,
                "mean": 1500.0,
                "std": 200.0,
                "min_val": "100",
                "max_val": "5000",
                "top_10": None,
            }
        ]

        insert_col_stats(test_db, stats_a)
        insert_col_stats(test_db, stats_b)

        result = compare_column_stats(
            test_db,
            "table_a",
            "table_b",
            columns=["amount"]
        )

        assert "amount" in result
        assert len(result["amount"]) > 0
        # Should have matching stats
        comparison = result["amount"][0]
        assert comparison["dt"] == "2025-01-01"
        assert comparison["n_total_diff"] == 0
        assert comparison["mean_diff"] == 0.0

    def test_compare_with_differences(self, test_db):
        """Test comparison with statistical differences"""
        stats_a = [
            {
                "source_table": "table_a",
                "column_name": "amount",
                "dt": "2025-01-01",
                "col_type": "numeric",
                "n_total": 100,
                "n_missing": 5,
                "n_unique": 80,
                "mean": 1500.0,
                "std": 200.0,
                "min_val": "100",
                "max_val": "5000",
                "top_10": None,
            }
        ]

        stats_b = [
            {
                "source_table": "table_b",
                "column_name": "amount",
                "dt": "2025-01-01",
                "col_type": "numeric",
                "n_total": 95,  # Different
                "n_missing": 10,  # Different
                "n_unique": 80,
                "mean": 1520.0,  # Different
                "std": 210.0,  # Different
                "min_val": "100",
                "max_val": "5000",
                "top_10": None,
            }
        ]

        insert_col_stats(test_db, stats_a)
        insert_col_stats(test_db, stats_b)

        result = compare_column_stats(
            test_db,
            "table_a",
            "table_b",
            columns=["amount"]
        )

        comparison = result["amount"][0]
        assert comparison["n_total_diff"] == -5
        assert comparison["n_missing_diff"] == 5
        assert comparison["mean_diff"] == 20.0


class TestGetColumnMapping:
    """Test column mapping resolution"""

    def test_get_mapping_from_pair(self, test_db):
        """Test getting column mapping from registered pair"""
        col_mappings = {
            "AMT": "amount",
            "CUST_STATUS": "customer_status"
        }

        register_table_pair(
            test_db,
            pair_name="test_pair",
            table_left="table_a",
            table_right="table_b",
            col_mappings=col_mappings
        )

        mapping = get_column_mapping(
            test_db,
            "table_a",
            "table_b",
            pair_name="test_pair"
        )

        assert mapping == col_mappings

    def test_get_mapping_with_override(self, test_db):
        """Test that col_map override takes precedence"""
        # Register pair with one mapping
        register_table_pair(
            test_db,
            pair_name="test_pair",
            table_left="table_a",
            table_right="table_b",
            col_mappings={"OLD": "old"}
        )

        # Override with different mapping
        override = {"NEW": "new"}
        mapping = get_column_mapping(
            test_db,
            "table_a",
            "table_b",
            col_map_override=override
        )

        assert mapping == override

    def test_get_mapping_auto_detect(self, test_db):
        """Test auto-detection of pair by table names"""
        col_mappings = {"A": "a"}

        register_table_pair(
            test_db,
            pair_name="auto_pair",
            table_left="table_a",
            table_right="table_b",
            col_mappings=col_mappings
        )

        # Don't specify pair_name, should auto-detect
        mapping = get_column_mapping(
            test_db,
            "table_a",
            "table_b"
        )

        assert mapping == col_mappings

    def test_get_mapping_no_pair(self, test_db):
        """Test when no pair exists, returns empty dict"""
        mapping = get_column_mapping(
            test_db,
            "table_a",
            "table_b"
        )

        assert mapping == {}
