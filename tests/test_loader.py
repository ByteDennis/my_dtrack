"""Tests for data loading functionality"""

import pytest
import tempfile
import os
import csv
from pathlib import Path
from dtrack.loader import (
    detect_date_column,
    detect_count_column,
    load_row_count_csv,
    load_row_counts,
    load_column_data_csv,
    load_column_data,
)
from dtrack.db import init_database, get_row_counts, get_col_stats


@pytest.fixture
def test_db():
    """Create a temporary database for testing"""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    init_database(path)
    yield path
    if os.path.exists(path):
        os.unlink(path)


@pytest.fixture
def row_count_csv():
    """Create a temporary CSV with row counts"""
    fd, path = tempfile.mkstemp(suffix=".csv")
    with open(path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['eff_dt', 'cnt'])
        writer.writerow(['201805', '3400'])
        writer.writerow(['201806', '3500'])
        writer.writerow(['201807', '3600'])
    os.close(fd)
    yield path
    if os.path.exists(path):
        os.unlink(path)


@pytest.fixture
def col_data_csv():
    """Create a temporary CSV with column data"""
    fd, path = tempfile.mkstemp(suffix=".csv")
    with open(path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['dt', 'amount', 'status'])
        writer.writerow(['2025-01-01', '100.50', 'ACTIVE'])
        writer.writerow(['2025-01-01', '200.75', 'PENDING'])
        writer.writerow(['2025-01-02', '150.25', 'ACTIVE'])
    os.close(fd)
    yield path
    if os.path.exists(path):
        os.unlink(path)


class TestColumnDetection:
    """Test automatic column detection"""

    def test_detect_date_column(self):
        """Test detecting date column from headers"""
        assert detect_date_column(['eff_dt', 'cnt']) == 'eff_dt'
        assert detect_date_column(['RPG_DT', 'row_count']) == 'RPG_DT'
        assert detect_date_column(['date', 'value']) == 'date'
        assert detect_date_column(['run_date', 'count']) == 'run_date'

    def test_detect_count_column(self):
        """Test detecting count column from headers"""
        assert detect_count_column(['eff_dt', 'cnt']) == 'cnt'
        assert detect_count_column(['date', 'ROW_COUNT']) == 'ROW_COUNT'
        assert detect_count_column(['dt', 'count']) == 'count'
        assert detect_count_column(['date', 'rows']) == 'rows'


class TestLoadRowCountCSV:
    """Test loading row counts from CSV"""

    def test_load_row_count_csv_basic(self, row_count_csv):
        """Test loading a basic row count CSV"""
        data = load_row_count_csv(row_count_csv)

        assert len(data) == 3
        assert data[0] == ("201805", 3400)
        assert data[1] == ("201806", 3500)
        assert data[2] == ("201807", 3600)


class TestLoadRowCounts:
    """Test load_row_counts integration"""

    def test_load_row_counts_single_csv(self, test_db, row_count_csv):
        """Test loading row counts from a single CSV"""
        load_row_counts(
            db_path=test_db,
            file_or_folder=row_count_csv,
            table_name="test_table",
            mode="upsert",
            vintage="day",
            source="pcds",
            db_name="prod_db",
            source_table="schema.customer_daily",
        )

        # Check data was loaded
        rows = get_row_counts(test_db, "test_table")
        assert len(rows) == 3
        assert rows[0][1] == 3400  # row_count

    def test_load_row_counts_folder(self, test_db, tmpdir):
        """Test loading row counts from a folder"""
        # Create two CSV files in a temp folder
        csv1 = tmpdir / "file1.csv"
        csv2 = tmpdir / "file2.csv"

        with open(csv1, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['dt', 'count'])
            writer.writerow(['2025-01-01', '100'])

        with open(csv2, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['dt', 'count'])
            writer.writerow(['2025-01-02', '200'])

        # Load folder
        load_row_counts(
            db_path=test_db,
            file_or_folder=str(tmpdir),
            table_name="test_table",
            mode="upsert",
            vintage="day",
        )

        # Should have combined data from both files
        rows = get_row_counts(test_db, "test_table")
        assert len(rows) == 2


class TestLoadColumnDataCSV:
    """Test loading column data from CSV"""

    def test_load_column_data_csv_basic(self, col_data_csv):
        """Test loading column data CSV"""
        import pandas as pd
        df = load_column_data_csv(
            csv_path=col_data_csv,
            date_col="dt",
            vintage="day",
        )

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert "dt" in df.columns
        assert "amount" in df.columns
        assert "status" in df.columns

    def test_load_column_data_csv_with_date_filter(self, col_data_csv):
        """Test loading with date range filter"""
        df = load_column_data_csv(
            csv_path=col_data_csv,
            date_col="dt",
            vintage="day",
            from_date="2025-01-01",
            to_date="2025-01-01",
        )

        # Should only have rows from 2025-01-01
        assert len(df) == 2


class TestLoadColumnData:
    """Test load_column_data integration"""

    def test_load_column_data(self, test_db, col_data_csv):
        """Test loading column data"""
        load_column_data(
            db_path=test_db,
            file_path=col_data_csv,
            source_table="test_table",
            date_col="dt",
            columns=["amount", "status"],
            mode="upsert",
            vintage="day",
            source="aws",
            db_name="prod_db",
        )

        # Check stats were computed and stored
        stats = get_col_stats(test_db, "test_table")

        # 2 dates * 2 columns = 4 stat records
        assert len(stats) == 4

        # Check that amount column is numeric
        amount_stats = [s for s in stats if s["column_name"] == "amount"]
        assert len(amount_stats) == 2
        assert amount_stats[0]["col_type"] == "numeric"
