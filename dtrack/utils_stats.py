"""Statistical computation utilities for column analysis with categorical/continuous differentiation."""
from concurrent.futures import ProcessPoolExecutor, as_completed
from utils_config import proc_pcds, proc_aws
from loguru import logger
import multiprocessing as mp
import functools as ft
from tqdm import tqdm

#>>> Get statistics for single column (worker function) <<<#
def get_pcds_column_stats(args, lock=None):
    svc, table_name, col_name, col_type, where_clause = args
    try:
        sql = build_column_sql(table_name, col_name, col_type, where_clause, is_oracle=True)
        df = proc_pcds(sql, service_name=svc)
        if not df.empty:
            return parse_stats_row(df.iloc[0].to_dict())
        return None
    except Exception as e:
        logger.error(f"Error getting stats for {col_name}: {e}")
        return None


#>>> Get statistics for single column (worker function) <<<#
def get_aws_column_stats(args, lock=None):
    database, table_name, col_name, col_type, where_clause = args
    try:
        sql = build_column_sql(table_name, col_name, col_type, where_clause, is_oracle=False)
        df = proc_aws(sql, data_base=database)
        if not df.empty:
            return parse_stats_row(df.iloc[0].to_dict())
        return None
    except Exception as e:
        logger.error(f"Error getting stats for {col_name}: {e}")
        return None
    
#>>> Get statistics for all columns in a vintage <<<#
def get_vintage_stats(
        table_name, columns_with_types, vintage, 
        max_workers=1, svc=None, db=None
    ):
    where_clause = vintage.get('where_clause', '1=1')

    if svc is not None:
        args_list = [(svc, table_name, col, typ, where_clause) for col, typ in columns_with_types.items()]
        single_worker = get_pcds_column_stats
    elif db is not None:
        args_list = [(db, table_name, col, typ, where_clause) for col, typ in columns_with_types.items()]
        single_worker = get_aws_column_stats

    if max_workers == 1:
        return {
            col: single_worker(args) for col, args in tqdm(
            zip(columns_with_types.keys(), args_list),
            total=len(args_list), desc="Calculating column statistics"
        )}

    all_stats = {}
    executor_class = ft.partial( ProcessPoolExecutor, mp_context=mp.get_context('spawn'))
    locker = mp.Manager().Lock()
    with executor_class(max_workers=max_workers) as executor:
        futures = {}
        for col_name, args in tqdm(zip(columns_with_types.keys(), args_list)):
            futures[executor.submit(single_worker, args=args, lock=locker)] = col_name
        for future in tqdm( as_completed(futures),
            total=len(futures), desc='Processing ... '
        ):
            try:
                col_name = futures[future]
                all_stats[col_name] = future.result()
            except Exception as e:
                logger.error(f"Task failed: {e}")
        executor.shutdown()

    return all_stats

#>>> Determine if data type is numeric (continuous) vs categorical <<<#
def is_numeric_type(data_type, is_oracle=True):
    data_type = data_type.upper() if is_oracle else data_type.lower()
    if is_oracle:
        return any(t in data_type for t in ['NUMBER', 'FLOAT'])
    else:
        return any(t in data_type for t in ['int', 'double', 'decimal', 'float', 'bigint'])

def build_categorical_sql_oracle(
    table_name: str,
    col_name: str,
    col_type: str,
    where_clause: str,
    top_n: int = 10
) -> str:
    wc = where_clause.strip() or "(1=1)"
    # Use TRUNC for DATE/TIMESTAMP, otherwise group by raw column
    dt_up = (col_type or "").upper()
    column_ref = f"TRUNC({col_name})" if ("DATE" in dt_up or "TIMESTAMP" in dt_up) else col_name

    return (
        "WITH FreqTable_RAW AS ( "
        f"SELECT {column_ref} AS p_col, COUNT(*) AS value_freq "
        f"FROM {table_name} WHERE {wc} "
        f"GROUP BY {column_ref} "
        "), FreqTable AS ( "
        "SELECT p_col, value_freq, "
        "       ROW_NUMBER() OVER (ORDER BY value_freq DESC, p_col ASC) AS rn "
        "FROM FreqTable_RAW "
        "), AggStats AS ( "
        "SELECT "
        "  SUM(ft.value_freq) AS col_count, "
        "  COUNT(ft.value_freq) AS col_distinct, "
        "  MAX(ft.value_freq) AS col_max, "
        "  MIN(ft.value_freq) AS col_min, "
        "  AVG(ft.value_freq) AS col_avg, "
        "  STDDEV_SAMP(ft.value_freq) AS col_std, "
        "  SUM(ft.value_freq) AS col_sum, "
        "  SUM(ft.value_freq * ft.value_freq) AS col_sum_sq "
        "FROM FreqTable ft "
        ") "
        "SELECT "
        f"  '{col_type}' AS col_type, "
        "  ast.*, "
        "  (SELECT NVL(value_freq, 0) FROM FreqTable WHERE p_col IS NULL) AS col_missing, "
        f"  (SELECT LISTAGG(p_col || '(' || value_freq || ')', '; ') "
        f"     WITHIN GROUP (ORDER BY value_freq DESC, p_col ASC) "
        f"   FROM FreqTable WHERE rn <= {top_n}) AS col_freq "
        "FROM AggStats ast"
    )


def build_continuous_sql_oracle(table_name, col_name, col_type, where_clause):
    wc = where_clause.strip() or "(1=1)"
    return (
        "SELECT "
        f"'{col_type}' AS col_type, "
        f"COUNT({col_name}) AS col_count, "
        f"COUNT(DISTINCT {col_name}) AS col_distinct, "
        f"MAX({col_name}) AS col_max, "
        f"MIN({col_name}) AS col_min, "
        f"TO_CHAR(AVG({col_name})) AS col_avg, "
        f"TO_CHAR(STDDEV_SAMP({col_name})) AS col_std, "
        f"TO_CHAR(SUM({col_name})) AS col_sum, "
        f"TO_CHAR(SUM({col_name} * {col_name})) AS col_sum_sq, "  
        f"COUNT(*) - COUNT({col_name}) AS col_missing, "
        "CAST('' AS VARCHAR2(1)) AS col_freq "
        f"FROM {table_name} WHERE {wc}"
    )


#>>> Build SQL for continuous column (Athena) <<<#
def build_continuous_sql_athena(table_name, col_name, col_type, where_clause):
    wc = where_clause.strip() or "(1=1)"
    return f"""
SELECT
    '{col_type}' AS col_type,
    COUNT({col_name}) AS col_count,
    COUNT(DISTINCT {col_name}) AS col_distinct,
    MAX({col_name}) AS col_max,
    MIN({col_name}) AS col_min,
    CAST(AVG(CAST({col_name} AS DOUBLE)) AS VARCHAR) AS col_avg,
    CAST(STDDEV_SAMP(CAST({col_name} AS DOUBLE)) AS VARCHAR) AS col_std,
    CAST(SUM(CAST({col_name} AS DOUBLE)) AS VARCHAR) AS col_sum,
    CAST(SUM(CAST({col_name} AS DOUBLE) * CAST({col_name} AS DOUBLE)) AS VARCHAR) AS col_sum_sq,
    '' AS col_freq,
    COUNT(*) - COUNT({col_name}) AS col_missing
FROM {table_name}  WHERE {wc}
""".strip()

#>>> Build SQL for categorical column (Athena) <<<#
def build_categorical_sql_athena(table_name, col_name, col_type, where_clause, top_n: int = 10):
    return f"""
WITH FreqTable_RAW AS (
    SELECT
        {col_name} AS p_col,
        COUNT(*) AS value_freq
    FROM  {table_name} WHERE {where_clause}
    GROUP BY {col_name}
),FreqTable AS (
    SELECT
        p_col, value_freq, 
        ROW_NUMBER() OVER (ORDER BY value_freq DESC, p_col ASC) AS rn
    FROM FreqTable_RAW
)
SELECT
    '{col_type}' AS col_type,
    SUM(value_freq) AS col_count,
    COUNT(value_freq) AS col_distinct,
    MAX(value_freq) AS col_max,
    MIN(value_freq) AS col_min,
    AVG(CAST(value_freq AS DOUBLE)) AS col_avg,
    STDDEV_SAMP(CAST(value_freq AS DOUBLE)) AS col_std,
    SUM(value_freq) AS col_sum,
    SUM(value_freq * value_freq) AS col_sum_sq,
    (SELECT ARRAY_JOIN(ARRAY_AGG(COALESCE(CAST(p_col AS VARCHAR), '') || '(' || CAST(value_freq AS VARCHAR) || ')' ORDER BY value_freq DESC), '; ') FROM FreqTable WHERE rn <= {top_n}) AS col_freq, 
    (SELECT COALESCE(value_freq, 0) FROM FreqTable Where p_col is NULL) AS col_missing
FROM FreqTable
""".strip()

#>>> Build SQL for single column based on type <<<#
def build_column_sql(table_name, col_name, col_type, where_clause, is_oracle=True):
    is_continuous = is_numeric_type(col_type, is_oracle)
    if is_oracle:
        return build_continuous_sql_oracle(table_name, col_name, col_type, where_clause) if is_continuous \
            else build_categorical_sql_oracle(table_name, col_name, col_type, where_clause)
    else:
        return build_continuous_sql_athena(table_name, col_name, col_type, where_clause) if is_continuous \
            else build_categorical_sql_athena(table_name, col_name, col_type, where_clause)

#>>> Parse single row statistics result <<<#
def parse_stats_row(row):
    row = {k.lower(): v for k,v in row.items()}
    return {
        'col_type': row['col_type'],
        'count': row['col_count'] or 0,
        'distinct': row['col_distinct'] or 0,
        'missing': row['col_missing'] or 0,
        'max': row['col_max'],
        'min': row['col_min'],
        'avg': row['col_avg'],
        'std': row['col_std'],
        'sum': row['col_sum'],
        'sum_sq': row['col_sum_sq'],
        'col_freq': row['col_freq']
    }