"""Tests for value normalization in categorical columns"""

import pytest
from dtrack.stats import normalize_value, compute_categorical_stats


class TestNormalizeValue:
    """Test value normalization function"""

    def test_whitespace_trimming(self):
        """Test trimming whitespace"""
        assert normalize_value("xxx ") == "xxx"
        assert normalize_value(" xxx") == "xxx"
        assert normalize_value("  xxx  ") == "xxx"
        assert normalize_value("") == ""
        assert normalize_value("   ") == ""

    def test_date_normalization(self):
        """Test date format normalization"""
        # SAS datetime format
        assert normalize_value("04MAR2026:00:00:00") == "2026-03-04"

        # ISO format (already normalized)
        assert normalize_value("2026-03-04") == "2026-03-04"

        # YYYYMM format (passes through unchanged)
        assert normalize_value("202603") == "202603"

        # US format
        assert normalize_value("03/04/2026") == "2026-03-04"

    def test_numeric_normalization(self):
        """Test numeric format normalization"""
        # Trailing zeros
        assert normalize_value("0.440") == "0.44"
        assert normalize_value("1.000") == "1"
        assert normalize_value("5.0") == "5"

        # Already normalized
        assert normalize_value("0.44") == "0.44"
        assert normalize_value("123") == "123"

        # Scientific notation
        assert normalize_value("1.5e2") == "150"

    def test_plain_string(self):
        """Test plain string values"""
        # Non-date, non-numeric strings pass through
        assert normalize_value("ACTIVE") == "ACTIVE"
        assert normalize_value("PENDING") == "PENDING"
        assert normalize_value("xyz123") == "xyz123"

    def test_edge_cases(self):
        """Test edge cases"""
        assert normalize_value(None) == ""
        assert normalize_value("") == ""
        assert normalize_value("0") == "0"
        assert normalize_value("0.0") == "0"


class TestCategoricalStatsWithNormalization:
    """Test categorical stats with normalization"""

    def test_normalized_frequency_count(self):
        """Test that different formats of same value are counted together"""
        values = [
            "04MAR2026:00:00:00",
            "2026-03-04",
            "04MAR2026:00:00:00",
            "ACTIVE",
            "ACTIVE ",  # with trailing space
            " ACTIVE",  # with leading space
            "0.440",
            "0.44",
            "0.4400",
        ]

        stats = compute_categorical_stats(values)

        assert stats["n_total"] == 9
        assert stats["n_missing"] == 0
        assert stats["n_unique"] == 3  # "2026-03-04", "ACTIVE", "0.44"

        # Parse top_10
        import json
        top_10 = json.loads(stats["top_10"])

        # Find counts
        counts = {item["value"]: item["count"] for item in top_10}

        # All date formats should be counted as "2026-03-04"
        assert counts.get("2026-03-04") == 3

        # All "ACTIVE" variants should be counted together
        assert counts.get("ACTIVE") == 3

        # All numeric formats should be counted as "0.44"
        assert counts.get("0.44") == 3

    def test_empty_and_whitespace(self):
        """Test handling of empty and whitespace values"""
        values = [
            "ACTIVE",
            "",
            "   ",
            "ACTIVE",
            None,
            "PENDING",
        ]

        stats = compute_categorical_stats(values)

        assert stats["n_total"] == 6
        assert stats["n_missing"] == 3  # empty, whitespace-only, and None
        assert stats["n_unique"] == 2  # "ACTIVE" and "PENDING"
