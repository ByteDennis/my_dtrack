

COL_CSV_HEADERS = [
    'dt', 'column_name', 'col_type', 'n_total', 'n_missing',
    'n_unique', 'mean', 'std', 'min_val', 'max_val', 'top_10',
    'length_avg', 'length_max', 'hash_sum',
]

ROW_CSV_HEADERS = ['dt', 'row_count']

SQL_MARKER_CONN_MACRO  = '@conn_macro'
SQL_MARKER_DEGRADE_SQL = '@degrade_sql'
SQL_MARKER_END_DEGRADE = '@end_degrade'

STATS_PRESETS = {
    'quick': {
        'n_total':     True,
        'n_missing':   True,
        'n_unique':    False,
        'min_max':     True,
        'length':      True,
        'hash_sum':    False,
        'top_10':      False,
    },
    'standard': {
        'n_total':     True,
        'n_missing':   True,
        'n_unique':    True,
        'min_max':     True,
        'length':      True,
        'hash_sum':    True,
        'top_10':      False,
    },
    'detailed': {
        'n_total':     True,
        'n_missing':   True,
        'n_unique':    True,
        'min_max':     True,
        'length':      True,
        'hash_sum':    True,
        'top_10':      True,
    },
}

PLATFORM_RUNNER = {
    'oracle': {
        'worker_env':       'ORACLE_MAX_WORKERS',
        'worker_default':   2,
        'degraded_label':   'oracle_degraded',
        'lifeboat_errors':  ['ORA-01652', 'ORA-04030'],
    },
    'hadoop': {
        'worker_env':       'HADOOP_MAX_WORKERS',
        'worker_default':   1,
        'degraded_label':   'hadoop_degraded',
        'lifeboat_errors':  [
            'OutOfMemoryError',
            'memory limit',
            'GC overhead',
            'Container killed',
            'beyond physical memory',
            'beyond virtual memory',
            'Java heap space',
            'spill failed',
        ],
    },
    'athena': {
        'worker_env':       'ATHENA_MAX_WORKERS',
        'worker_default':   8,
        'degraded_label':   'athena_degraded',
        'lifeboat_errors':  ['Query exhausted resources'],
    },
    'databricks': {
        'worker_env':       'DATABRICKS_MAX_WORKERS',
        'worker_default':   4,
        'degraded_label':   'databricks_degraded',
        'lifeboat_errors':  [
            'Job aborted',
            'OutOfMemoryError',
            'memory limit',
            'GC overhead',
            'SparkOutOfMemoryError',
            'ExecutorLostFailure',
        ],
    },
    'csv': {
        'worker_env':       'CSV_MAX_WORKERS',
        'worker_default':   4,
        'degraded_label':   'csv_degraded',
        'lifeboat_errors':  ['Out of Memory', 'could not allocate', 'OutOfMemory'],
    },
}

STAT_ROUND_DECIMALS = 4

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
}

DATA_SOURCES = [
    {"value": "oracle", "label": "Oracle"},
    {"value": "hadoop", "label": "Hadoop/Hive"},
    {"value": "aws", "label": "AWS/Athena"},
    {"value": "databricks", "label": "AWS/Databricks"},
    {"value": "csv", "label": "CSV"},
]

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

DATE_TYPE_FORMATS = {
    'string_compact':  'YYYYMMDD',
    'string_dash':     'YYYY-MM-DD',
    'string':          'YYYY-MM-DD',
    'string_mon':      'DDMONYYYY',
    'string_mon_dash': 'DD-MON-YYYY',
    'string_us':       'MM/DD/YYYY',
    'num':             'YYYYMMDD',
    'integer':         'YYYYMMDD',
    'int':             'YYYYMMDD',
    'number':          'YYYYMMDD',
    'num_yyyymm':      'YYYYMM',
    'date':            'YYYY-MM-DD',
    'timestamp':       'YYYY-MM-DD',
    'datetime':        'YYYY-MM-DD',
}

DATE_TYPE_TO_CANONICAL_DTYPE = {
    'date':            'DATE',
    'timestamp':       'TIMESTAMP',
    'datetime':        'TIMESTAMP',
    'num':             'NUMBER',
    'integer':         'NUMBER',
    'int':             'NUMBER',
    'number':          'NUMBER',
    'num_yyyymm':      'NUMBER',
    'string':          'VARCHAR2',
    'string_dash':     'VARCHAR2',
    'string_compact':  'VARCHAR2',
    'string_mon':      'VARCHAR2',
    'string_mon_dash': 'VARCHAR2',
    'string_us':       'VARCHAR2',
}

CONNECTION_MACROS = {
    "oracle": ["pb23", "pb30"],
    "hadoop": ["hdp", "hadoop_prod"],
    "aws": ["analytics_db", "warehouse_db", "mydb"],
    "databricks": [],
    "csv": [],
}

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
}
