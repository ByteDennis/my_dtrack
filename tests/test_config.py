"""Tests for JSON config parsing"""

import pytest
import json
import tempfile
import os
from dtrack.config import parse_pairs_config, validate_pair_config


class TestParsePairsConfig:
    """Test parsing JSON configuration for table pairs"""

    def test_parse_valid_config(self):
        """Test parsing a valid configuration"""
        config = {
            "pairs": [
                {
                    "name": "customer_daily",
                    "tables": {
                        "oracle": {
                            "file": "data/oracle.csv",
                            "source": "oracle",
                            "db": "prod_db",
                            "table_name": "customer_daily_oracle",
                            "date_col": "eff_dt",
                            "vintage": "day"
                        },
                        "aws": {
                            "file": "data/aws.csv",
                            "source": "aws",
                            "db": "analytics_db",
                            "table_name": "customer_daily_aws",
                            "date_col": "run_date",
                            "vintage": "day"
                        }
                    },
                    "col_map": {
                        "AMT": "amount",
                        "CUST_STATUS": "customer_status"
                    }
                }
            ]
        }

        pairs = parse_pairs_config(config)
        assert len(pairs) == 1

        pair = pairs[0]
        assert pair["name"] == "customer_daily"
        assert "oracle" in pair["tables"]
        assert "aws" in pair["tables"]
        assert pair["tables"]["oracle"]["file"] == "data/oracle.csv"
        assert pair["col_map"]["AMT"] == "amount"

    def test_parse_config_with_defaults(self):
        """Test that defaults are applied when optional fields missing"""
        config = {
            "pairs": [
                {
                    "name": "test_pair",
                    "tables": {
                        "oracle": {
                            "file": "data/oracle.csv",
                            "table_name": "test_oracle"
                        },
                        "aws": {
                            "file": "data/aws.csv",
                            "table_name": "test_aws"
                        }
                    }
                }
            ]
        }

        pairs = parse_pairs_config(config)
        pair = pairs[0]

        # Check defaults
        assert pair["tables"]["oracle"].get("vintage", "day") == "day"
        assert pair.get("col_map", {}) == {}

    def test_parse_config_from_file(self, tmp_path):
        """Test parsing config from JSON file"""
        config_file = tmp_path / "config.json"
        config = {
            "pairs": [
                {
                    "name": "test",
                    "tables": {
                        "oracle": {"file": "a.csv", "table_name": "t1"},
                        "aws": {"file": "b.csv", "table_name": "t2"}
                    }
                }
            ]
        }

        with open(config_file, 'w') as f:
            json.dump(config, f)

        from dtrack.config import load_pairs_config_from_file
        pairs = load_pairs_config_from_file(str(config_file))
        assert len(pairs) == 1
        assert pairs[0]["name"] == "test"


class TestValidatePairConfig:
    """Test validation of pair configurations"""

    def test_validate_valid_config(self):
        """Test validation of a valid config"""
        pair = {
            "name": "test",
            "tables": {
                "oracle": {"file": "a.csv", "table_name": "t1"},
                "aws": {"file": "b.csv", "table_name": "t2"}
            }
        }

        # Should not raise
        validate_pair_config(pair)

    def test_validate_missing_name(self):
        """Test validation fails for missing name"""
        pair = {
            "tables": {
                "oracle": {"file": "a.csv", "table_name": "t1"},
                "aws": {"file": "b.csv", "table_name": "t2"}
            }
        }

        with pytest.raises(ValueError, match="name"):
            validate_pair_config(pair)

    def test_validate_missing_tables(self):
        """Test validation fails for missing tables"""
        pair = {
            "name": "test"
        }

        with pytest.raises(ValueError, match="tables"):
            validate_pair_config(pair)

    def test_validate_single_table(self):
        """Test validation fails if only one table in pair"""
        pair = {
            "name": "test",
            "tables": {
                "oracle": {"file": "a.csv", "table_name": "t1"}
            }
        }

        with pytest.raises(ValueError, match="at least two tables"):
            validate_pair_config(pair)

    def test_validate_file_optional(self):
        """Test validation passes when file is omitted (data loaded separately)"""
        pair = {
            "name": "test",
            "tables": {
                "oracle": {"table_name": "t1"},
                "aws": {"table_name": "t2"}
            }
        }
        validate_pair_config(pair)  # Should not raise

    def test_validate_missing_table_name(self):
        """Test validation fails for missing table_name"""
        pair = {
            "name": "test",
            "tables": {
                "oracle": {"file": "a.csv"},  # Missing table_name
                "aws": {"file": "b.csv", "table_name": "t2"}
            }
        }

        with pytest.raises(ValueError, match="table_name"):
            validate_pair_config(pair)
