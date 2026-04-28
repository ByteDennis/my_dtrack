"""Constants for dtrack UI configuration."""

# Decimal precision for mean/std match keys in column comparison.
# Two values match iff `round(float(v), STAT_ROUND_DECIMALS)` is equal as a
# string with an "m_"/"s_" prefix — bypasses float-equality drift.
STAT_ROUND_DECIMALS = 4

# Data source types
DATA_SOURCES = [
    {"value": "oracle", "label": "SAS/Oracle"},
    {"value": "hadoop", "label": "SAS/Hadoop"},
    {"value": "sas", "label": "SAS Dataset"},
    {"value": "aws", "label": "AWS/Athena"},
    {"value": "csv", "label": "CSV"},
]

# Date column types — dropdown options shown in the pairs UI. When adding a
# new format, also add it to DATE_TYPE_FORMATS in platforms/base.py and to
# reformat_date()'s fmt_map there.
DATE_COLUMN_TYPES = [
    {"value": "date", "label": "Date"},
    {"value": "timestamp", "label": "Timestamp"},
    {"value": "datetime", "label": "DateTime"},
    {"value": "num", "label": "Number (YYYYMMDD)"},
    {"value": "num_yyyymm", "label": "Number (YYYYMM)"},
    {"value": "string_dash", "label": "String (YYYY-MM-DD)"},
    {"value": "string_compact", "label": "String (YYYYMMDD)"},
    {"value": "string_mon", "label": "String (DDMONYYYY)"},
    {"value": "string_mon_dash", "label": "String (DD-MON-YYYY)"},
    {"value": "string_us", "label": "String (MM/DD/YYYY)"},
]

# Date literal formatting by platform and date column type
# These transform the literal values in WHERE clauses (right side), not the column (left side)
# Example: WHERE date_col >= {literal} (not WHERE TRUNC(date_col) >= '2020-01-01')
#
# IMPORTANT: For datetime/timestamp types:
# - Lower bound (>=): Use date at 00:00:00
# - Upper bound (<): Use NEXT day at 00:00:00 (exclusive) to capture all records including 23:59:59.999
DATE_LITERAL_FORMATS = {
    "hadoop": {
        "date": {"from": "DATE '{value}'", "to": "DATE '{value}'"},
        "timestamp": {"from": "TIMESTAMP '{value} 00:00:00'", "to": "TIMESTAMP '{value} 23:59:59'"},
        "datetime": {"from": "TIMESTAMP '{value} 00:00:00'", "to": "TIMESTAMP '{value} 23:59:59'"},
        "num": {"from": "{sas_num}", "to": "{sas_num}"},
        "string_dash": {"from": "'{value}'", "to": "'{value}'"},
        "string_compact": {"from": "'{value_compact}'", "to": "'{value_compact}'"},
    },
    "oracle": {
        "date": {"from": "DATE '{value}'", "to": "DATE '{value}'"},
        "timestamp": {"from": "TIMESTAMP '{value} 00:00:00'", "to": "TIMESTAMP '{value} 23:59:59'"},
        "datetime": {"from": "TIMESTAMP '{value} 00:00:00'", "to": "TIMESTAMP '{value} 23:59:59'"},
        "num": {"from": "{sas_num}", "to": "{sas_num}"},
        "string_dash": {"from": "'{value}'", "to": "'{value}'"},
        "string_compact": {"from": "'{value_compact}'", "to": "'{value_compact}'"},
    },
    "aws": {
        "date": {"from": "DATE '{value}'", "to": "DATE '{value}'"},
        "timestamp": {"from": "TIMESTAMP '{value} 00:00:00'", "to": "TIMESTAMP '{value} 23:59:59'"},
        "datetime": {"from": "TIMESTAMP '{value} 00:00:00'", "to": "TIMESTAMP '{value} 23:59:59'"},
        "num": {"from": "{sas_num}", "to": "{sas_num}"},
        "string_dash": {"from": "'{value}'", "to": "'{value}'"},
        "string_compact": {"from": "'{value_compact}'", "to": "'{value_compact}'"},
    },
    "sas": {
        "date": {"from": "'{value_sas}'d", "to": "'{value_sas}'d"},
        "timestamp": {"from": "'{value_sas}:00:00:00'dt", "to": "'{value_sas}:23:59:59'dt"},
        "datetime": {"from": "'{value_sas}:00:00:00'dt", "to": "'{value_sas}:23:59:59'dt"},
        "num": {"from": "{sas_num}", "to": "{sas_num}"},
        "string_dash": {"from": "'{value}'", "to": "'{value}'"},
        "string_compact": {"from": "'{value_compact}'", "to": "'{value_compact}'"},
    },
}

# Operator to use with upper bound based on date type
# All use <= (inclusive) — timestamp/datetime use same day 23:59:59
DATE_UPPER_BOUND_OPERATORS = {
    "date": "<=",
    "timestamp": "<=",
    "datetime": "<=",
    "num": "<=",
    "string_dash": "<=",
    "string_compact": "<=",
}

# Connection macros by source type
CONNECTION_MACROS = {
    "oracle": ["pb23", "pb30"],
    "hadoop": ["hdp", "hadoop_prod"],
    "sas": ["work", "sasuser"],
    "aws": ["analytics_db", "warehouse_db", "mydb"],
    "csv": [],
}

# Vintage presets for different platforms
VINTAGE_PRESETS = {
    "hadoop": {
        "day": "{col}",
        "week": "TRUNC({col}, 'IW')",
        "month": "TRUNC({col}, 'MM')",
        "quarter": "TRUNC({col}, 'Q')",
        "year": "TRUNC({col}, 'YYYY')",
    },
    "oracle": {
        "day": "{col}",
        "week": "TRUNC({col}, 'IW')",
        "month": "TRUNC({col}, 'MM')",
        "quarter": "TRUNC({col}, 'Q')",
        "year": "TRUNC({col}, 'YYYY')",
    },
    "aws": {
        "day": "{col}",
        "week": "DATE_TRUNC('week', {col})",
        "month": "DATE_TRUNC('month', {col})",
        "quarter": "DATE_TRUNC('quarter', {col})",
        "year": "DATE_TRUNC('year', {col})",
    },
    "sas": {
        "day": "{col}",
        "week": "intnx('week.2', {col}, 0, 'b')",
        "month": "intnx('month', {col}, 0, 'b')",
        "quarter": "intnx('qtr', {col}, 0, 'b')",
        "year": "intnx('year', {col}, 0, 'b')",
    },
}
