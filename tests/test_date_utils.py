"""Tests for date parsing, format detection, DateConverter, and vintage bucketing"""

import pytest
from datetime import datetime
from dtrack.date_utils import (
    parse_date, bucket_date, detect_format,
    DateConverter, format_vintage_label,
)


class TestParseDate:
    """Test date parsing from various formats"""

    def test_parse_iso_date(self):
        assert parse_date("2022-03-03") == "2022-03-03"

    def test_parse_iso_datetime(self):
        assert parse_date("2022-03-03 14:30:00") == "2022-03-03"

    def test_parse_iso_datetime_fractional(self):
        assert parse_date("2022-03-03 14:30:00.123456") == "2022-03-03"

    def test_parse_iso8601_t_separator(self):
        assert parse_date("2022-03-03T14:30:00") == "2022-03-03"
        assert parse_date("2022-03-03T14:30:00.123456") == "2022-03-03"
        assert parse_date("2022-03-03T14:30:00.123456Z") == "2022-03-03"

    def test_parse_sas_datetime(self):
        assert parse_date("03MAR2022:00:00:00") == "2022-03-03"
        assert parse_date("15JAN2024:14:30:00") == "2024-01-15"

    def test_parse_sas_datetime_fractional(self):
        assert parse_date("03MAR2022:00:00:00.000000") == "2022-03-03"

    def test_parse_sas_date_only(self):
        assert parse_date("03MAR2022") == "2022-03-03"
        assert parse_date("15jan2024") == "2024-01-15"

    def test_parse_sas_short_year(self):
        assert parse_date("03MAR22") == "2022-03-03"

    def test_parse_oracle_default(self):
        assert parse_date("03-MAR-2022") == "2022-03-03"
        assert parse_date("15-JAN-2024") == "2024-01-15"

    def test_parse_oracle_datetime(self):
        assert parse_date("03-MAR-2022 14:30:00") == "2022-03-03"

    def test_parse_oracle_short_year(self):
        assert parse_date("03-MAR-22") == "2022-03-03"

    def test_parse_slash_variant(self):
        assert parse_date("2022/03/03") == "2022-03-03"
        assert parse_date("2022/03/03 14:30:00") == "2022-03-03"

    def test_parse_yyyymmdd(self):
        assert parse_date("20220303") == "2022-03-03"

    def test_parse_yyyymm(self):
        assert parse_date("201805") == "201805"
        assert parse_date("202412") == "202412"

    def test_parse_us_date(self):
        assert parse_date("03/03/2022") == "2022-03-03"
        assert parse_date("12/31/2024") == "2024-12-31"

    def test_parse_invalid_date(self):
        with pytest.raises(ValueError):
            parse_date("not-a-date")
        with pytest.raises(ValueError):
            parse_date("2022-13-01")


class TestDetectFormat:
    """Test date format detection"""

    def test_detect_iso(self):
        assert detect_format("2022-03-03") == "YYYY-MM-DD"

    def test_detect_sas(self):
        assert detect_format("03MAR2022:00:00:00") == "DDMONYYYY:HH:MM:SS"

    def test_detect_sas_date_only(self):
        assert detect_format("03MAR2022") == "DDMONYYYY"

    def test_detect_oracle(self):
        assert detect_format("03-MAR-2022") == "DD-MON-YYYY"

    def test_detect_yyyymmdd(self):
        assert detect_format("20220303") == "YYYYMMDD"

    def test_detect_yyyymm(self):
        assert detect_format("202203") == "YYYYMM"


class TestDateConverter:
    """Test bidirectional date conversion"""

    def test_learn_and_convert_sas(self):
        dc = DateConverter()
        dc.learn("03MAR2022:00:00:00")
        assert dc.format_label == "DDMONYYYY:HH:MM:SS"
        assert dc.to_canonical("15JAN2024:00:00:00") == "2024-01-15"
        assert dc.to_original("2024-01-15") == "15JAN2024:00:00:00"

    def test_learn_and_convert_yyyymmdd(self):
        dc = DateConverter()
        dc.learn("20220303")
        assert dc.to_canonical("20240115") == "2024-01-15"
        assert dc.to_original("2024-01-15") == "20240115"

    def test_learn_and_convert_oracle(self):
        dc = DateConverter()
        dc.learn("03-MAR-2022")
        assert dc.to_canonical("15-JAN-2024") == "2024-01-15"
        assert dc.to_original("2024-01-15") == "15-JAN-2024"

    def test_from_values(self):
        dc = DateConverter.from_values(["20220303", "20220304", "20220305"])
        assert dc.format_label == "YYYYMMDD"
        assert dc.to_original("2022-03-03") == "20220303"

    def test_canonical_passthrough(self):
        dc = DateConverter()
        dc.learn("2022-03-03")
        assert dc.to_original("2022-03-03") == "2022-03-03"

    def test_sas_date_only_roundtrip(self):
        dc = DateConverter()
        dc.learn("03MAR2022")
        assert dc.to_original("2022-03-03") == "03MAR2022"


class TestFormatVintageLabel:
    """Test human-readable vintage labels"""

    def test_day(self):
        label = format_vintage_label("2025-03-15", "day")
        assert "Mar" in label and "15" in label

    def test_week(self):
        assert format_vintage_label("2025-03-03", "week") == "2025-W10"

    def test_month(self):
        assert format_vintage_label("2025-03-01", "month") == "Mar 2025"

    def test_quarter(self):
        assert format_vintage_label("2025-01-01", "quarter") == "Q1 2025"
        assert format_vintage_label("2025-04-01", "quarter") == "Q2 2025"

    def test_year(self):
        assert format_vintage_label("2025-01-01", "year") == "2025"

    def test_yyyymm(self):
        assert format_vintage_label("202503", "month") == "Mar 2025"


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
