"""Tests for statistics computation"""

import pytest
from dtrack.stats import (
    detect_column_type,
    compute_numeric_stats,
    compute_categorical_stats,
    compute_column_stats,
)


class TestDetectColumnType:
    """Test column type detection"""

    def test_detect_numeric_all_integers(self):
        """Test detection of integer column"""
        values = ["1", "2", "3", "4", "5"]
        assert detect_column_type(values) == "numeric"

    def test_detect_numeric_all_floats(self):
        """Test detection of float column"""
        values = ["1.5", "2.3", "3.7", "4.2"]
        assert detect_column_type(values) == "numeric"

    def test_detect_numeric_with_missing(self):
        """Test numeric detection with missing values"""
        values = ["1.5", "", "3.7", None, "4.2"]
        assert detect_column_type(values) == "numeric"

    def test_detect_categorical_strings(self):
        """Test detection of categorical column"""
        values = ["ACTIVE", "PENDING", "CLOSED", "ACTIVE"]
        assert detect_column_type(values) == "categorical"

    def test_detect_categorical_mixed(self):
        """Test detection when <90% numeric"""
        values = ["1", "2", "three", "4", "5"]
        assert detect_column_type(values) == "categorical"

    def test_detect_numeric_threshold(self):
        """Test 90% threshold for numeric detection"""
        # 9 numeric, 1 categorical = 90% numeric -> categorical
        values = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "text"]
        assert detect_column_type(values) == "categorical"

        # 10 numeric, 1 categorical = 91% numeric -> numeric
        values = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "text"]
        assert detect_column_type(values) == "numeric"


class TestComputeNumericStats:
    """Test numeric statistics computation"""

    def test_compute_numeric_stats_basic(self):
        """Test basic numeric stats"""
        values = ["10", "20", "30", "40", "50"]
        stats = compute_numeric_stats(values)

        assert stats["n_total"] == 5
        assert stats["n_missing"] == 0
        assert stats["n_unique"] == 5
        assert stats["mean"] == 30.0
        assert stats["min_val"] == "10.0"
        assert stats["max_val"] == "50.0"
        assert stats["std"] is not None
        assert stats["top_10"] is None

    def test_compute_numeric_stats_with_missing(self):
        """Test numeric stats with missing values"""
        values = ["10", "", "30", None, "50"]
        stats = compute_numeric_stats(values)

        assert stats["n_total"] == 5
        assert stats["n_missing"] == 2
        assert stats["n_unique"] == 3
        assert round(stats["mean"], 2) == 30.0

    def test_compute_numeric_stats_floats(self):
        """Test numeric stats with floats"""
        values = ["10.5", "20.3", "30.7"]
        stats = compute_numeric_stats(values)

        assert stats["n_total"] == 3
        assert round(stats["mean"], 2) == 20.5


class TestComputeCategoricalStats:
    """Test categorical statistics computation"""

    def test_compute_categorical_stats_basic(self):
        """Test basic categorical stats"""
        values = ["ACTIVE", "PENDING", "ACTIVE", "CLOSED", "ACTIVE"]
        stats = compute_categorical_stats(values)

        assert stats["n_total"] == 5
        assert stats["n_missing"] == 0
        assert stats["n_unique"] == 3
        assert stats["min_val"] == "ACTIVE"
        assert stats["max_val"] == "PENDING"
        assert stats["mean"] is None
        assert stats["std"] is None
        assert stats["top_10"] is not None
        assert "ACTIVE" in stats["top_10"]

    def test_compute_categorical_stats_with_missing(self):
        """Test categorical stats with missing values"""
        values = ["ACTIVE", "", "ACTIVE", None, "PENDING"]
        stats = compute_categorical_stats(values)

        assert stats["n_total"] == 5
        assert stats["n_missing"] == 2
        assert stats["n_unique"] == 2

    def test_compute_categorical_top_10(self):
        """Test top 10 frequency computation"""
        values = ["A"] * 10 + ["B"] * 5 + ["C"] * 3 + ["D"] * 1
        stats = compute_categorical_stats(values)

        import json
        top_10 = json.loads(stats["top_10"])
        assert len(top_10) == 4
        assert top_10[0]["value"] == "A"
        assert top_10[0]["count"] == 10


class TestComputeColumnStats:
    """Test integrated column stats computation"""

    def test_compute_column_stats_dataframe(self):
        """Test computing stats for a dataframe"""
        import pandas as pd

        df = pd.DataFrame({
            "dt": ["2025-01-01", "2025-01-01", "2025-01-01", "2025-01-02", "2025-01-02"],
            "amount": [100, 200, 150, 175, 225],
            "status": ["ACTIVE", "PENDING", "ACTIVE", "ACTIVE", "CLOSED"],
        })

        stats = compute_column_stats(
            df=df,
            source_table="test_table",
            date_col="dt",
            columns=["amount", "status"]
        )

        # Should have 2 dates * 2 columns = 4 stat records
        assert len(stats) == 4

        # Check amount stats for 2025-01-01
        amount_stats = [s for s in stats if s["dt"] == "2025-01-01" and s["column_name"] == "amount"][0]
        assert amount_stats["col_type"] == "numeric"
        assert amount_stats["n_total"] == 3
        assert amount_stats["mean"] == 150.0

        # Check status stats for 2025-01-01
        status_stats = [s for s in stats if s["dt"] == "2025-01-01" and s["column_name"] == "status"][0]
        assert status_stats["col_type"] == "categorical"
        assert status_stats["n_total"] == 3
        assert status_stats["n_unique"] == 2
