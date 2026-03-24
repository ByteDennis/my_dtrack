"""Constants for dtrack UI configuration."""

# Data source types
DATA_SOURCES = [
    {"value": "pcds", "label": "SAS/Oracle"},
    {"value": "hadoop", "label": "SAS/Hadoop"},
    {"value": "oracle", "label": "Oracle Direct"},
    {"value": "sas", "label": "SAS Dataset"},
    {"value": "aws", "label": "AWS/Athena"},
    {"value": "csv", "label": "CSV"},
]

# Date column types
DATE_COLUMN_TYPES = [
    {"value": "date", "label": "Date"},
    {"value": "timestamp", "label": "Timestamp"},
    {"value": "datetime", "label": "DateTime"},
    {"value": "num", "label": "Number (SAS date)"},
    {"value": "string_dash", "label": "String (YYYY-MM-DD)"},
    {"value": "string_compact", "label": "String (YYYYMMDD)"},
]

# Date literal formatting by platform and date column type
# These transform the literal values in WHERE clauses (right side), not the column (left side)
# Example: WHERE date_col >= {literal} (not WHERE TRUNC(date_col) >= '2020-01-01')
#
# IMPORTANT: For datetime/timestamp types:
# - Lower bound (>=): Use date at 00:00:00
# - Upper bound (<): Use NEXT day at 00:00:00 (exclusive) to capture all records including 23:59:59.999
DATE_LITERAL_FORMATS = {
    "pcds": {
        "date": {"from": "DATE '{value}'", "to": "DATE '{value}'"},
        "timestamp": {"from": "TIMESTAMP '{value} 00:00:00'", "to": "TIMESTAMP '{value_next} 00:00:00'"},  # to uses NEXT day
        "datetime": {"from": "TIMESTAMP '{value} 00:00:00'", "to": "TIMESTAMP '{value_next} 00:00:00'"},  # to uses NEXT day
        "num": {"from": "{sas_num}", "to": "{sas_num}"},
        "string_dash": {"from": "'{value}'", "to": "'{value}'"},
        "string_compact": {"from": "'{value_compact}'", "to": "'{value_compact}'"},
    },
    "hadoop": {
        "date": {"from": "DATE '{value}'", "to": "DATE '{value}'"},
        "timestamp": {"from": "TIMESTAMP '{value} 00:00:00'", "to": "TIMESTAMP '{value_next} 00:00:00'"},
        "datetime": {"from": "TIMESTAMP '{value} 00:00:00'", "to": "TIMESTAMP '{value_next} 00:00:00'"},
        "num": {"from": "{sas_num}", "to": "{sas_num}"},
        "string_dash": {"from": "'{value}'", "to": "'{value}'"},
        "string_compact": {"from": "'{value_compact}'", "to": "'{value_compact}'"},
    },
    "oracle": {
        "date": {"from": "DATE '{value}'", "to": "DATE '{value}'"},
        "timestamp": {"from": "TIMESTAMP '{value} 00:00:00'", "to": "TIMESTAMP '{value_next} 00:00:00'"},
        "datetime": {"from": "TIMESTAMP '{value} 00:00:00'", "to": "TIMESTAMP '{value_next} 00:00:00'"},
        "num": {"from": "{sas_num}", "to": "{sas_num}"},
        "string_dash": {"from": "'{value}'", "to": "'{value}'"},
        "string_compact": {"from": "'{value_compact}'", "to": "'{value_compact}'"},
    },
    "aws": {
        "date": {"from": "DATE '{value}'", "to": "DATE '{value}'"},
        "timestamp": {"from": "TIMESTAMP '{value} 00:00:00'", "to": "TIMESTAMP '{value_next} 00:00:00'"},
        "datetime": {"from": "TIMESTAMP '{value} 00:00:00'", "to": "TIMESTAMP '{value_next} 00:00:00'"},
        "num": {"from": "{sas_num}", "to": "{sas_num}"},
        "string_dash": {"from": "'{value}'", "to": "'{value}'"},
        "string_compact": {"from": "'{value_compact}'", "to": "'{value_compact}'"},
    },
    "sas": {
        "date": {"from": "'{value_sas}'d", "to": "'{value_sas}'d"},
        "timestamp": {"from": "'{value_sas}:00:00:00'dt", "to": "'{value_next_sas}:00:00:00'dt"},
        "datetime": {"from": "'{value_sas}:00:00:00'dt", "to": "'{value_next_sas}:00:00:00'dt"},
        "num": {"from": "{sas_num}", "to": "{sas_num}"},
        "string_dash": {"from": "'{value}'", "to": "'{value}'"},
        "string_compact": {"from": "'{value_compact}'", "to": "'{value_compact}'"},
    },
}

# Operator to use with upper bound based on date type
# For datetime/timestamp, use < (less than) with next day
# For date/string/num, use <= (less than or equal) with same day
DATE_UPPER_BOUND_OPERATORS = {
    "date": "<=",
    "timestamp": "<",  # Exclusive upper bound (next day)
    "datetime": "<",   # Exclusive upper bound (next day)
    "num": "<=",
    "string_dash": "<=",
    "string_compact": "<=",
}

# Connection macros by source type
CONNECTION_MACROS = {
    "pcds": ["pcds", "pb23", "pb30"],
    "hadoop": ["hdp", "hadoop_prod"],
    "oracle": ["pcds", "pb23", "pb30"],
    "sas": ["work", "sasuser"],
    "aws": ["analytics_db", "warehouse_db", "mydb"],
    "csv": [],
}

# Vintage presets for different platforms
VINTAGE_PRESETS = {
    "pcds": {
        "day": "{col}",
        "week": "TRUNC({col}, 'IW')",
        "month": "TRUNC({col}, 'MM')",
        "quarter": "TRUNC({col}, 'Q')",
        "year": "TRUNC({col}, 'YYYY')",
    },
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
