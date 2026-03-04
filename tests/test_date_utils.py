"""Tests for date parsing and vintage bucketing"""

import pytest
from datetime import datetime
from dtrack.date_utils import parse_date, bucket_date


class TestParseDate:
    """Test date parsing from various formats"""

    def test_parse_iso_date(self):
        """Test parsing ISO format YYYY-MM-DD"""
        assert parse_date("2022-03-03") == "2022-03-03"

    def test_parse_iso_datetime(self):
        """Test parsing ISO datetime, truncate to date"""
        assert parse_date("2022-03-03 14:30:00") == "2022-03-03"

    def test_parse_sas_datetime(self):
        """Test parsing SAS datetime format DDMONYYYY:HH:MM:SS"""
        assert parse_date("03MAR2022:00:00:00") == "2022-03-03"
        assert parse_date("15JAN2024:14:30:00") == "2024-01-15"

    def test_parse_yyyymm(self):
        """Test parsing YYYYMM to first of month"""
        assert parse_date("201805") == "2018-05-01"
        assert parse_date("202412") == "2024-12-01"

    def test_parse_us_date(self):
        """Test parsing US format MM/DD/YYYY"""
        assert parse_date("03/03/2022") == "2022-03-03"
        assert parse_date("12/31/2024") == "2024-12-31"

    def test_parse_invalid_date(self):
        """Test that invalid dates raise ValueError"""
        with pytest.raises(ValueError):
            parse_date("not-a-date")
        with pytest.raises(ValueError):
            parse_date("2022-13-01")  # Invalid month


class TestBucketDate:
    """Test vintage bucketing"""

    def test_bucket_day(self):
        """Day vintage should return unchanged"""
        assert bucket_date("2022-03-15", "day") == "2022-03-15"

    def test_bucket_week(self):
        """Week vintage should return Monday of ISO week"""
        # 2022-03-15 is Tuesday, should bucket to 2022-03-14 (Monday)
        assert bucket_date("2022-03-15", "week") == "2022-03-14"
        # 2022-03-14 is Monday, should stay same
        assert bucket_date("2022-03-14", "week") == "2022-03-14"
        # 2022-03-20 is Sunday, should bucket to 2022-03-14 (Monday)
        assert bucket_date("2022-03-20", "week") == "2022-03-14"

    def test_bucket_month(self):
        """Month vintage should return first of month"""
        assert bucket_date("2022-03-15", "month") == "2022-03-01"
        assert bucket_date("2022-03-01", "month") == "2022-03-01"
        assert bucket_date("2022-03-31", "month") == "2022-03-01"

    def test_bucket_quarter(self):
        """Quarter vintage should return first of quarter"""
        assert bucket_date("2022-03-15", "quarter") == "2022-01-01"
        assert bucket_date("2022-05-20", "quarter") == "2022-04-01"
        assert bucket_date("2022-08-10", "quarter") == "2022-07-01"
        assert bucket_date("2022-11-25", "quarter") == "2022-10-01"

    def test_bucket_year(self):
        """Year vintage should return first of year"""
        assert bucket_date("2022-03-15", "year") == "2022-01-01"
        assert bucket_date("2022-12-31", "year") == "2022-01-01"

    def test_bucket_invalid_vintage(self):
        """Test that invalid vintage raises ValueError"""
        with pytest.raises(ValueError):
            bucket_date("2022-03-15", "invalid")
