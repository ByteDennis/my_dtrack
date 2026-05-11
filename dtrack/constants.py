"""Constants for dtrack UI configuration."""

# Decimal precision for mean/std match keys in column comparison.
# Two values match iff `round(float(v), STAT_ROUND_DECIMALS)` is equal as a
# string with an "m_"/"s_" prefix — bypasses float-equality drift.
STAT_ROUND_DECIMALS = 4

# Sentinel values that should be coerced to NULL before computing stats.
# Phase-1 coarse comparison is meant to flag columns whose distribution
# truly drifts between platforms; if one side stores -999 for missing and
# the other stores NULL, min/max/mean/n_unique will diverge for reasons
# that have nothing to do with the underlying data. Translating these
# values to NULL up front collapses the difference and keeps the signal
# focused on real drift.
#
# Per-platform so each engine can use its native types/literals.
# Categories:
#   numeric  — sentinel ints/floats (-999, -99999999, etc.)
#   string   — sentinel strings ('NA', '#N/A', '.', etc.). Blank/empty
#              are handled separately by the missing_cond rule.
#   date     — sentinel date literals. "infinity" (e.g. 9999-12-31),
#              "missing" (e.g. 0001-01-01), and the "today-placeholder"
#              (a fixed far-future date some pipelines write when the
#              row's date is unknown).
#
# Edit this dict to add or remove sentinels for your environment.
NULL_SENTINELS = {
    "oracle": {
        "numeric": [-999, -9999, -99999999],
        "string":  ["NA", "N/A", "#N/A", ".", "NULL", "null"],
        "date":    ["9999-12-31", "4712-12-31", "0001-01-01", "1900-01-01"],
    },
    "hadoop": {
        "numeric": [-999, -9999, -99999999],
        "string":  ["NA", "N/A", "#N/A", ".", "NULL", "null"],
        "date":    ["9999-12-31", "0001-01-01", "1900-01-01"],
    },
    "aws": {
        "numeric": [-999, -9999, -99999999],
        "string":  ["NA", "N/A", "#N/A", ".", "NULL", "null"],
        "date":    ["9999-12-31", "0001-01-01", "1900-01-01"],
    },
    "sas": {
        "numeric": [-999, -9999, -99999999],
        "string":  ["NA", "N/A", "#N/A", ".", "NULL", "null"],
        "date":    ["9999-12-31", "0001-01-01", "1900-01-01"],
    },
}

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

# ==========================================================================
# DATE TYPE REGISTRY — single source of truth for how `date_type` (the
# user's semantic config in tbl_cfg.date_type) maps to:
#   1. the canonical strftime-like format label used by reformat_date()
#      to convert min/max date strings into the column's stored shape,
#   2. the canonical raw DB type that the WHERE-clause builders branch
#      on to choose between bare numerics, quoted strings, DATE literals,
#      etc.
#
# To add a new format:
#   1. Add an entry to DATE_TYPE_FORMATS (date_type -> format label).
#   2. Add a matching entry to DATE_TYPE_TO_CANONICAL_DTYPE.
#   3. Add a strftime pattern to reformat_date()'s fmt_map (platforms/base.py).
#   4. Add a dropdown entry to DATE_COLUMN_TYPES above.
#
# All platforms (SAS, Oracle, Hadoop, Athena) pick these up via
# resolve_date_format() — no per-platform changes needed.
# ==========================================================================
DATE_TYPE_FORMATS = {
    # String types
    'string_compact':  'YYYYMMDD',    # '20260115'
    'string_dash':     'YYYY-MM-DD',  # '2026-01-15'
    'string':          'YYYY-MM-DD',  # '2026-01-15'
    'string_mon':      'DDMONYYYY',   # '15JAN2026'
    'string_mon_dash': 'DD-MON-YYYY', # '15-JAN-2026'
    'string_us':       'MM/DD/YYYY',  # '01/15/2026'
    # Numeric types (no quotes in SQL)
    'num':             'YYYYMMDD',    # 20260115
    'integer':         'YYYYMMDD',    # 20260115
    'int':             'YYYYMMDD',    # 20260115
    'number':          'YYYYMMDD',    # 20260115
    'num_yyyymm':      'YYYYMM',      # 202601
    # Native date/timestamp
    'date':            'YYYY-MM-DD',  # DATE '2026-01-15'
    'timestamp':       'YYYY-MM-DD',  # TIMESTAMP '2026-01-15 ...'
    'datetime':        'YYYY-MM-DD',  # TIMESTAMP '2026-01-15 ...'
}

# Map the user's semantic date_type to a canonical RAW DB type label.
# Downstream WHERE-clause builders branch on this dtype to pick the
# literal form (number, quoted string, DATE 'X', etc.). When the user
# configures date_type explicitly, this overrides _column_meta even if
# the metadata says something different (stale / missing / wrong).
DATE_TYPE_TO_CANONICAL_DTYPE = {
    # Native date / timestamp -- TRUNC + DATE 'YYYY-MM-DD' literals.
    'date':            'DATE',
    'timestamp':       'TIMESTAMP',
    'datetime':        'TIMESTAMP',
    # Numeric: bare integer literals like 20260115, no quotes, no TRUNC.
    'num':             'NUMBER',
    'integer':         'NUMBER',
    'int':             'NUMBER',
    'number':          'NUMBER',
    'num_yyyymm':      'NUMBER',
    # String: quoted literals in the column's stored format
    # ('2026-01-15', '20260115', '15JAN2026', '15-JAN-2026', '01/15/2026').
    'string':          'VARCHAR2',
    'string_dash':     'VARCHAR2',
    'string_compact':  'VARCHAR2',
    'string_mon':      'VARCHAR2',
    'string_mon_dash': 'VARCHAR2',
    'string_us':       'VARCHAR2',
}

# Connection macros by source type
CONNECTION_MACROS = {
    "oracle": ["pb23", "pb30"],
    "hadoop": ["hdp", "hadoop_prod"],
    "sas": ["work", "sasuser"],
    "aws": ["analytics_db", "warehouse_db", "mydb"],
    "csv": [],
}

# Hadoop libname / PROC SQL CONNECT profiles. Each method gets its own dict
# because the two statements take different params on this user's box. The
# Playground SAS engine shows the appropriate dropdown depending on which
# method is selected.
#
# Add or rename profiles here without touching JS/HTML — the UI fetches
# both dicts via /api/constants on page load and switches based on method.
HADOOP_LIBNAME_PROFILES = {
    "default": {
        "server":      "hadoop.example.com",
        "host":        "REALM.EXAMPLE.COM",
        "schema":      "default",
        "port":        10000,
        "jar_path":    "/sas/bin/Software/Hadoop/OSH/Jars",
        "config_path": "/sas/bin/Software/Hadoop/OSH/PROD",
        # "truststore": "/sas/bin/Software/Hadoop/OSH/PROD/truststore.jks",
    },
}

HADOOP_PROCSQL_PROFILES = {
    "default": {
        "server":      "hadoop.example.com",
        "host":        "REALM.EXAMPLE.COM",
        "schema":      "default",
        "port":        10000,
        "jar_path":    "/sas/bin/Software/Hadoop/OSH/Jars",
        "config_path": "/sas/bin/Software/Hadoop/OSH/PROD",
    },
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
