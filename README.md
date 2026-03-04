# dtrack - Data Tracking CLI

A Python CLI tool for tracking row counts and column-level statistics in SQLite databases. Built with test-driven development (TDD) and zero external dependencies (except pandas/numpy for statistics).

## Features

- ✅ **Row Count Tracking**: Load and track row counts from CSV files
- ✅ **Column Statistics**: Compute and track per-column statistics (mean, std, min, max, top_10, etc.)
- ✅ **Vintage Bucketing**: Aggregate data by day/week/month/quarter/year
- ✅ **Multiple Data Sources**: Track data from AWS, PCDS, Oracle, etc.
- ✅ **Date Format Support**: Handles ISO, SAS datetime, YYYYMM, US date formats
- ✅ **SQLite Storage**: Persistent storage with metadata tracking

## Installation

```bash
# Install in development mode
pip install -e .

# Or install from PyPI (once published)
pip install dtrack
```

## Quick Start

```bash
# 1. Initialize a new project database
dtrack init myproject.db

# 2. Load row count data
dtrack load-row myproject.db data/customer_daily.csv \
  --source pcds \
  --db prod_db \
  --vintage day

# 3. Load column statistics
dtrack load-col myproject.db data/customer_data.csv \
  --date-col dt \
  --columns amount,status,region \
  --source aws \
  --db analytics_db \
  --vintage week

# 4. List all tables
dtrack list myproject.db

# 5. Show row counts
dtrack show myproject.db customer_daily

# 6. Show column statistics
dtrack show-stats myproject.db customer_daily --column amount
```

## Commands

### `init` - Initialize Database

Create a new dtrack database.

```bash
dtrack init <project.db> [--force]
```

### `load-row` - Load Row Count Data

Load row counts from CSV file(s).

```bash
dtrack load-row <project.db> <file_or_folder> [options]

Options:
  --table-name NAME      Table name (defaults to filename)
  --mode MODE            Load mode: replace|append|upsert (default: upsert)
  --vintage GRANULARITY  Time granularity: day|week|month|quarter|year (default: day)
  --source SOURCE        Data source: aws|pcds|oracle
  --db DB                Database or service name
  --source-table TABLE   Original table name
  --date-var VAR         Date column name (auto-detected if not provided)
```

**CSV Format:**
```csv
eff_dt,cnt
201805,3400
201806,3500
```

### `load-col` - Load Column Statistics

Compute and store column statistics.

```bash
dtrack load-col <project.db> <file_path> --date-col <column> [options]

Options:
  --date-col COL         Name of the date column (required)
  --columns COL1,COL2    Comma-separated list of columns to analyze
  --mode MODE            Load mode: replace|upsert (default: upsert)
  --vintage GRANULARITY  Time granularity (default: day)
  --from-date DATE       Start date filter (YYYY-MM-DD)
  --to-date DATE         End date filter (YYYY-MM-DD)
  --source SOURCE        Data source
  --db DB                Database name
  --source-table TABLE   Source table name (defaults to filename)
```

**Statistics Computed:**

| Stat | Numeric | Categorical |
|------|---------|-------------|
| `n_total` | count of all rows | count of all rows |
| `n_missing` | count of NULL/empty/NaN | count of NULL/empty |
| `n_unique` | count of distinct values | count of distinct values |
| `mean` | arithmetic mean | — |
| `std` | standard deviation | — |
| `min` | minimum value | min value (lexicographic) |
| `max` | maximum value | max value |
| `top_10` | — | top 10 most frequent values as JSON |

### `list` - List Tables

List all tables in the database with metadata.

```bash
dtrack list <project.db>
```

### `show` - Show Row Counts

Display row count data from a table.

```bash
dtrack show <project.db> <table> [--limit N]
```

### `show-stats` - Show Column Statistics

Display column statistics.

```bash
dtrack show-stats <project.db> <table> [options]

Options:
  --column COL           Filter by column name
  --from-date DATE       Start date filter (YYYY-MM-DD)
  --to-date DATE         End date filter (YYYY-MM-DD)
  --limit N              Limit number of rows
```

## Development

### Running Tests

```bash
# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_date_utils.py -v

# Run with coverage
pytest tests/ --cov=dtrack --cov-report=html
```

### Project Structure

```
dtrack/
├── dtrack/
│   ├── __init__.py
│   ├── cli.py          # CLI entry point
│   ├── db.py           # Database operations
│   ├── loader.py       # CSV loading
│   ├── stats.py        # Statistics computation
│   └── date_utils.py   # Date parsing and bucketing
├── tests/
│   ├── test_date_utils.py
│   ├── test_db.py
│   ├── test_loader.py
│   └── test_stats.py
├── sample_data/
│   ├── customer_daily_pcds.csv
│   ├── customer_daily_aws.csv
│   └── customer_coldata.csv
├── pyproject.toml
└── README.md
```

## Date Format Support

The tool automatically detects and parses various date formats:

- **ISO Date**: `2022-03-03`
- **ISO DateTime**: `2022-03-03 14:30:00`
- **SAS DateTime**: `03MAR2022:00:00:00`
- **YYYYMM**: `201805` (converts to first of month)
- **US Date**: `03/03/2022`

## Vintage Bucketing

Control time granularity for aggregation:

- **day**: No bucketing (default)
- **week**: ISO week start (Monday)
- **month**: First of month
- **quarter**: First of quarter (Jan/Apr/Jul/Oct)
- **year**: First of year

## Database Schema

### Row Count Tables

```sql
CREATE TABLE <table_name> (
    dt TEXT PRIMARY KEY,
    row_count INTEGER NOT NULL
);
```

### Column Statistics Table

```sql
CREATE TABLE _col_stats (
    source_table TEXT NOT NULL,
    column_name TEXT NOT NULL,
    dt TEXT NOT NULL,
    col_type TEXT NOT NULL,  -- 'numeric' or 'categorical'
    n_total INTEGER,
    n_missing INTEGER,
    n_unique INTEGER,
    mean REAL,
    std REAL,
    min_val TEXT,
    max_val TEXT,
    top_10 TEXT,  -- JSON array for categorical
    PRIMARY KEY (source_table, column_name, dt)
);
```

### Metadata Table

```sql
CREATE TABLE _metadata (
    table_name TEXT PRIMARY KEY,
    source TEXT,
    db TEXT,
    source_table TEXT,
    date_var TEXT,
    source_file TEXT,
    loaded_at TEXT,
    last_updated TEXT,
    row_count_total INTEGER,
    load_mode TEXT,
    vintage TEXT,
    data_type TEXT  -- 'row' or 'col'
);
```

## Examples

### Example 1: Load Row Counts from Multiple Sources

```bash
# Load from PCDS
dtrack load-row myproject.db data/customer_daily_pcds.csv \
  --source pcds --db prod_db

# Load from AWS
dtrack load-row myproject.db data/customer_daily_aws.csv \
  --table-name customer_daily_aws --source aws --db analytics_db

# List tables
dtrack list myproject.db
```

### Example 2: Load Column Statistics with Weekly Vintage

```bash
dtrack load-col myproject.db data/customer_data.csv \
  --date-col dt \
  --columns amount,status,region \
  --vintage week \
  --source aws \
  --db analytics_db

# Show stats for amount column
dtrack show-stats myproject.db customer_data --column amount
```

### Example 3: Incremental Refresh

```bash
# Load only the latest week of data
dtrack load-col myproject.db data/latest_data.csv \
  --date-col dt \
  --from-date 2025-03-01 \
  --to-date 2025-03-07 \
  --mode upsert
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Write tests for your changes
4. Make your changes and ensure tests pass
5. Commit your changes (`git commit -m 'Add amazing feature'`)
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

## License

MIT License

## Author

Built with TDD principles and ❤️
