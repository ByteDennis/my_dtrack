"""Data extraction: SAS code generation for Oracle, direct AWS Athena extraction"""

import csv
import json
import os
from pathlib import Path
from typing import List, Dict, Optional


# Oracle numeric types
ORACLE_NUMERIC_TYPES = {
    'NUMBER', 'FLOAT', 'BINARY_FLOAT', 'BINARY_DOUBLE',
    'INTEGER', 'INT', 'SMALLINT', 'DECIMAL', 'NUMERIC', 'DEC',
}

# Athena numeric types
ATHENA_NUMERIC_TYPES = {
    'int', 'integer', 'bigint', 'smallint', 'tinyint',
    'double', 'float', 'decimal', 'real',
}


def is_numeric_type(data_type, is_oracle=True):
    """Check if a data type string represents a numeric column."""
    dt_upper = data_type.upper().split('(')[0].strip()
    if is_oracle:
        return dt_upper in ORACLE_NUMERIC_TYPES
    else:
        return dt_upper.lower() in ATHENA_NUMERIC_TYPES


def build_continuous_sql_oracle(table, col, date_col, where=""):
    """Build Oracle SQL for continuous/numeric column statistics."""
    where_clause = f"AND {where}" if where else ""
    return f"""
SELECT
    {date_col} AS dt,
    '{col}' AS column_name,
    'numeric' AS col_type,
    COUNT(*) AS n_total,
    SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS n_missing,
    COUNT(DISTINCT {col}) AS n_unique,
    AVG({col}) AS mean,
    STDDEV({col}) AS std,
    MIN({col}) AS min_val,
    MAX({col}) AS max_val
FROM {table}
WHERE 1=1 {where_clause}
GROUP BY {date_col}
ORDER BY {date_col}""".strip()


def build_categorical_sql_oracle(table, col, date_col, where=""):
    """Build Oracle SQL for categorical column statistics."""
    where_clause = f"AND {where}" if where else ""
    return f"""
SELECT
    {date_col} AS dt,
    '{col}' AS column_name,
    'categorical' AS col_type,
    COUNT(*) AS n_total,
    SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS n_missing,
    COUNT(DISTINCT {col}) AS n_unique,
    NULL AS mean,
    NULL AS std,
    MIN({col}) AS min_val,
    MAX({col}) AS max_val
FROM {table}
WHERE 1=1 {where_clause}
GROUP BY {date_col}
ORDER BY {date_col}""".strip()


def build_continuous_sql_athena(table, col, date_col, where=""):
    """Build Athena SQL for continuous/numeric column statistics."""
    where_clause = f"AND {where}" if where else ""
    return f"""
SELECT
    {date_col} AS dt,
    '{col}' AS column_name,
    'numeric' AS col_type,
    COUNT(*) AS n_total,
    SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS n_missing,
    COUNT(DISTINCT {col}) AS n_unique,
    AVG(CAST({col} AS DOUBLE)) AS mean,
    STDDEV(CAST({col} AS DOUBLE)) AS std,
    MIN({col}) AS min_val,
    MAX({col}) AS max_val
FROM {table}
WHERE 1=1 {where_clause}
GROUP BY {date_col}
ORDER BY {date_col}"""


def build_categorical_sql_athena(table, col, date_col, where=""):
    """Build Athena SQL for categorical column statistics."""
    where_clause = f"AND {where}" if where else ""
    return f"""
SELECT
    {date_col} AS dt,
    '{col}' AS column_name,
    'categorical' AS col_type,
    COUNT(*) AS n_total,
    SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS n_missing,
    COUNT(DISTINCT {col}) AS n_unique,
    NULL AS mean,
    NULL AS std,
    MIN({col}) AS min_val,
    MAX({col}) AS max_val
FROM {table}
WHERE 1=1 {where_clause}
GROUP BY {date_col}
ORDER BY {date_col}"""


def build_top10_sql_oracle(table, col, date_col, where=""):
    """Build Oracle SQL for top 10 frequency values per date."""
    where_clause = f"AND {where}" if where else ""
    return f"""
SELECT dt, val, cnt FROM (
    SELECT {date_col} AS dt, {col} AS val, COUNT(*) AS cnt,
           ROW_NUMBER() OVER (PARTITION BY {date_col} ORDER BY COUNT(*) DESC) AS rn
    FROM {table}
    WHERE {col} IS NOT NULL {where_clause}
    GROUP BY {date_col}, {col}
) WHERE rn <= 10
ORDER BY dt, cnt DESC""".strip()


def build_top10_sql_athena(table, col, date_col, where=""):
    """Build Athena SQL for top 10 frequency values per date."""
    where_clause = f"AND {where}" if where else ""
    return f"""
SELECT dt, val, cnt FROM (
    SELECT {date_col} AS dt, {col} AS val, COUNT(*) AS cnt,
           ROW_NUMBER() OVER (PARTITION BY {date_col} ORDER BY COUNT(*) DESC) AS rn
    FROM {table}
    WHERE {col} IS NOT NULL {where_clause}
    GROUP BY {date_col}, {col}
) t WHERE rn <= 10
ORDER BY dt, cnt DESC"""


def parse_stats_row(row):
    """Normalize a query result row to standard dict format."""
    return {
        'dt': str(row['dt']),
        'column_name': row['column_name'],
        'col_type': row['col_type'],
        'n_total': int(row['n_total']) if row['n_total'] is not None else 0,
        'n_missing': int(row['n_missing']) if row['n_missing'] is not None else 0,
        'n_unique': int(row['n_unique']) if row['n_unique'] is not None else 0,
        'mean': float(row['mean']) if row['mean'] is not None else None,
        'std': float(row['std']) if row['std'] is not None else None,
        'min_val': str(row['min_val']) if row['min_val'] is not None else None,
        'max_val': str(row['max_val']) if row['max_val'] is not None else None,
    }


# ---------------------------------------------------------------------------
# SAS Generation
# ---------------------------------------------------------------------------

def _oracle_date_transform(date_col, transform):
    """Return Oracle SQL expression for date transformation (used inside passthrough)."""
    if transform == "datetime_to_date":
        return f"TRUNC({date_col})"
    elif transform == "to_char":
        return f"TO_CHAR({date_col}, 'YYYY-MM-DD')"
    return date_col


def _gen_sas_row(tbl_cfg):
    """Generate SAS macro for row count extraction."""
    table = tbl_cfg['table']
    date_col = tbl_cfg['date_col']
    name = tbl_cfg['name']
    conn_macro = tbl_cfg.get('conn_macro', 'oracle')
    where = tbl_cfg.get('where', '')
    transform = tbl_cfg.get('date_transform', '')

    date_expr = _oracle_date_transform(date_col, transform) if transform else date_col
    where_clause = f"WHERE {where}" if where else ""

    return f"""\
%macro get_rowcounts_{name}(outpath=);
    proc sql;
        connect using {conn_macro};
        create table _rc_{name} as
        select * from connection to {conn_macro} (
            SELECT {date_expr} AS date_value, COUNT(*) AS row_count
            FROM {table}
            {where_clause}
            GROUP BY {date_expr}
            ORDER BY 1
        );
        disconnect from {conn_macro};
    quit;

    proc export data=_rc_{name}
        outfile="&outpath./{name}_row.csv"
        dbms=csv replace;
    run;

    proc delete data=_rc_{name}; run;
%mend get_rowcounts_{name};
"""


def _gen_sas_col_discover(tbl_cfg):
    """Generate SAS code to discover columns from ALL_TAB_COLUMNS."""
    table = tbl_cfg['table']
    name = tbl_cfg['name']
    conn_macro = tbl_cfg.get('conn_macro', 'oracle')

    # Split SCHEMA.TABLE
    parts = table.split('.')
    if len(parts) == 2:
        owner, tname = parts
    else:
        owner = 'USER'
        tname = parts[0]

    return f"""\
    /* Discover columns for {name} */
    proc sql;
        connect using {conn_macro};
        create table _cols_{name} as
        select * from connection to {conn_macro} (
            SELECT COLUMN_NAME, DATA_TYPE
            FROM ALL_TAB_COLUMNS
            WHERE OWNER = '{owner}' AND TABLE_NAME = '{tname}'
            ORDER BY COLUMN_ID
        );
        disconnect from {conn_macro};
    quit;

    proc export data=_cols_{name}
        outfile="&outpath./{name}_columns.csv"
        dbms=csv replace;
    run;
"""


def _gen_sas_col(tbl_cfg):
    """Generate SAS macro for column statistics extraction.

    Uses a single Oracle connection with a %DO loop over columns.
    Each column runs its own short SQL query within the same connection,
    avoiding repeated connect/disconnect overhead and SQL length limits.
    """
    table = tbl_cfg['table']
    date_col = tbl_cfg['date_col']
    name = tbl_cfg['name']
    conn_macro = tbl_cfg.get('conn_macro', 'oracle')
    where = tbl_cfg.get('where', '')
    columns = tbl_cfg.get('columns', {})
    transform = tbl_cfg.get('date_transform', '')

    date_expr = _oracle_date_transform(date_col, transform) if transform else date_col

    lines = []
    lines.append(f"%macro get_colstats_{name}(outpath=);")

    if not columns:
        lines.append(_gen_sas_col_discover(tbl_cfg))
        lines.append(f"    /* NOTE: Run this first, then update config with discovered columns */")
        lines.append(f"    %put WARNING: No columns specified for {name}. Column list exported to &outpath./{name}_columns.csv;")
        lines.append(f"%mend get_colstats_{name};")
        return '\n'.join(lines)

    # Filter out date column
    col_list = [(col, dtype) for col, dtype in columns.items()
                if col.upper() != date_col.upper()]
    n_cols = len(col_list)

    if n_cols == 0:
        lines.append(f"    %put WARNING: No non-date columns to extract for {name};")
        lines.append(f"%mend get_colstats_{name};")
        return '\n'.join(lines)

    # Define macro arrays for column names and types
    for i, (col, dtype) in enumerate(col_list, 1):
        is_num = is_numeric_type(dtype, is_oracle=True)
        lines.append(f"    %let _col{i} = {col};")
        lines.append(f"    %let _ctype{i} = {'numeric' if is_num else 'categorical'};")

    lines.append(f"    %let _ncols = {n_cols};")
    lines.append("")

    # Build the where fragment for Oracle SQL
    where_fragment = f"AND ({where})" if where else ""

    # Single connection, loop over columns
    lines.append(f"    proc sql;")
    lines.append(f"        connect using {conn_macro};")
    lines.append("")
    lines.append(f"        %do _i = 1 %to &_ncols;")
    lines.append(f"            %let _c = &&_col&_i;")
    lines.append(f"            %let _t = &&_ctype&_i;")
    lines.append(f"            %put NOTE: Extracting column &_i of &_ncols: &_c (&_t);")
    lines.append("")

    # Numeric branch
    lines.append(f"            %if &_t = numeric %then %do;")
    lines.append(f"                create table _cs_{name}_&_i as")
    lines.append(f"                select * from connection to {conn_macro} (")
    lines.append(f"                    SELECT")
    lines.append(f"                        {date_expr} AS dt,")
    lines.append(f"                        '&_c' AS column_name,")
    lines.append(f"                        'numeric' AS col_type,")
    lines.append(f"                        COUNT(*) AS n_total,")
    lines.append(f"                        SUM(CASE WHEN &_c IS NULL THEN 1 ELSE 0 END) AS n_missing,")
    lines.append(f"                        COUNT(DISTINCT &_c) AS n_unique,")
    lines.append(f"                        AVG(&_c) AS mean,")
    lines.append(f"                        STDDEV(&_c) AS std,")
    lines.append(f"                        MIN(&_c) AS min_val,")
    lines.append(f"                        MAX(&_c) AS max_val")
    lines.append(f"                    FROM {table}")
    lines.append(f"                    WHERE 1=1 {where_fragment}")
    lines.append(f"                    GROUP BY {date_expr}")
    lines.append(f"                    ORDER BY {date_expr}")
    lines.append(f"                );")
    lines.append(f"            %end;")

    # Categorical branch
    lines.append(f"            %else %do;")
    lines.append(f"                create table _cs_{name}_&_i as")
    lines.append(f"                select * from connection to {conn_macro} (")
    lines.append(f"                    SELECT")
    lines.append(f"                        {date_expr} AS dt,")
    lines.append(f"                        '&_c' AS column_name,")
    lines.append(f"                        'categorical' AS col_type,")
    lines.append(f"                        COUNT(*) AS n_total,")
    lines.append(f"                        SUM(CASE WHEN &_c IS NULL THEN 1 ELSE 0 END) AS n_missing,")
    lines.append(f"                        COUNT(DISTINCT &_c) AS n_unique,")
    lines.append(f"                        NULL AS mean,")
    lines.append(f"                        NULL AS std,")
    lines.append(f"                        MIN(&_c) AS min_val,")
    lines.append(f"                        MAX(&_c) AS max_val")
    lines.append(f"                    FROM {table}")
    lines.append(f"                    WHERE 1=1 {where_fragment}")
    lines.append(f"                    GROUP BY {date_expr}")
    lines.append(f"                    ORDER BY {date_expr}")
    lines.append(f"                );")
    lines.append(f"            %end;")
    lines.append("")
    lines.append(f"        %end; /* end column loop */")
    lines.append("")
    lines.append(f"        disconnect from {conn_macro};")
    lines.append(f"    quit;")
    lines.append("")

    # Stack all per-column datasets
    lines.append(f"    data _colstats_{name};")
    lines.append(f"        set")
    lines.append(f"        %do _i = 1 %to &_ncols;")
    lines.append(f"            _cs_{name}_&_i")
    lines.append(f"        %end;")
    lines.append(f"        ;")
    lines.append(f"    run;")
    lines.append("")

    # Export
    lines.append(f"    proc export data=_colstats_{name}")
    lines.append(f'        outfile="&outpath./{name}_col.csv"')
    lines.append(f"        dbms=csv replace;")
    lines.append(f"    run;")
    lines.append("")

    # Cleanup
    lines.append(f"    proc datasets lib=work nolist;")
    lines.append(f"        delete _colstats_{name}")
    lines.append(f"        %do _i = 1 %to &_ncols;")
    lines.append(f"            _cs_{name}_&_i")
    lines.append(f"        %end;")
    lines.append(f"        ;")
    lines.append(f"    quit;")
    lines.append("")

    lines.append(f"%mend get_colstats_{name};")
    return '\n'.join(lines)


def gen_sas(config_path, outdir, types=None):
    """
    Generate SAS files for Oracle data extraction.

    Args:
        config_path: Path to extraction config JSON
        outdir: Directory to write .sas files
        types: List of types to generate ("row", "col"). Default: both.
    """
    if types is None:
        types = ["row", "col"]

    with open(config_path, 'r') as f:
        config = json.load(f)

    os.makedirs(outdir, exist_ok=True)

    pcds_tables = [t for t in config['tables'] if t.get('source', '').lower() in ('pcds', 'oracle')]

    if not pcds_tables:
        print("No PCDS/Oracle tables found in config")
        return

    for tbl in pcds_tables:
        name = tbl['name']
        sas_parts = []
        sas_parts.append(f"/* Generated SAS extraction for: {name} */")
        sas_parts.append(f"/* Table: {tbl['table']} */")
        sas_parts.append("")

        if "row" in types:
            sas_parts.append(_gen_sas_row(tbl))

        if "col" in types:
            sas_parts.append(_gen_sas_col(tbl))

        # Add invocation
        sas_parts.append(f"/* === Run macros === */")
        sas_parts.append(f'%let outpath = {outdir};')
        if "row" in types:
            sas_parts.append(f"%get_rowcounts_{name}(outpath=&outpath.);")
        if "col" in types:
            sas_parts.append(f"%get_colstats_{name}(outpath=&outpath.);")

        sas_path = os.path.join(outdir, f"{name}.sas")
        with open(sas_path, 'w') as f:
            f.write('\n'.join(sas_parts))

        print(f"  Generated: {sas_path}")

    print(f"Generated {len(pcds_tables)} SAS file(s) in {outdir}")


# ---------------------------------------------------------------------------
# AWS Athena Direct Extraction
# ---------------------------------------------------------------------------

def _discover_columns_athena(cursor, database, table):
    """Discover columns from information_schema."""
    sql = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = '{database}' AND table_name = '{table}'
        ORDER BY ordinal_position
    """
    cursor.execute(sql)
    return {row[0]: row[1] for row in cursor.fetchall()}


def _extract_row_athena(cursor, tbl_cfg, outdir):
    """Extract row counts from Athena table."""
    database = tbl_cfg['database']
    table = tbl_cfg['table']
    date_col = tbl_cfg['date_col']
    name = tbl_cfg['name']
    where = tbl_cfg.get('where', '')

    full_table = f"{database}.{table}"
    where_clause = f"WHERE {where}" if where else ""

    sql = f"""
        SELECT {date_col} AS date_value, COUNT(*) AS row_count
        FROM {full_table}
        {where_clause}
        GROUP BY {date_col}
        ORDER BY {date_col}
    """
    cursor.execute(sql)
    rows = cursor.fetchall()

    csv_path = os.path.join(outdir, f"{name}_row.csv")
    with open(csv_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['date_value', 'row_count'])
        for row in rows:
            writer.writerow([row[0], row[1]])

    print(f"  Row counts: {csv_path} ({len(rows)} dates)")
    return csv_path


def _extract_col_single(cursor, tbl_cfg, col, dtype, full_table):
    """Extract stats for a single column. Returns list of stat dicts."""
    date_col = tbl_cfg['date_col']
    where = tbl_cfg.get('where', '')
    numeric = is_numeric_type(dtype, is_oracle=False)

    if numeric:
        sql = build_continuous_sql_athena(full_table, col, date_col, where)
    else:
        sql = build_categorical_sql_athena(full_table, col, date_col, where)

    cursor.execute(sql)
    results = []
    col_descriptions = [desc[0] for desc in cursor.description]
    for row in cursor.fetchall():
        row_dict = dict(zip(col_descriptions, row))
        parsed = parse_stats_row(row_dict)
        results.append(parsed)

    # Fetch top 10 for categorical columns
    if not numeric:
        top10_sql = build_top10_sql_athena(full_table, col, date_col, where)
        cursor.execute(top10_sql)
        top10_by_dt = {}
        for r in cursor.fetchall():
            dt_key = str(r[0])
            if dt_key not in top10_by_dt:
                top10_by_dt[dt_key] = []
            top10_by_dt[dt_key].append({"value": str(r[1]), "count": int(r[2])})

        for stat in results:
            stat['top_10'] = json.dumps(top10_by_dt.get(stat['dt'], []))

    return results


def extract_aws(config_path, outdir, types=None, max_workers=4):
    """
    Extract data directly from AWS Athena.

    Args:
        config_path: Path to extraction config JSON
        outdir: Directory to write CSV files
        types: List of types ("row", "col"). Default: both.
        max_workers: Max parallel workers for column extraction
    """
    try:
        from pyathena import connect as athena_connect
    except ImportError:
        print("Error: pyathena is required for AWS extraction")
        print("Install with: pip install 'dtrack[aws]'")
        return

    if types is None:
        types = ["row", "col"]

    with open(config_path, 'r') as f:
        config = json.load(f)

    os.makedirs(outdir, exist_ok=True)

    aws_tables = [t for t in config['tables'] if t.get('source', '').lower() == 'aws']

    if not aws_tables:
        print("No AWS tables found in config")
        return

    conn = athena_connect()
    cursor = conn.cursor()

    for tbl_cfg in aws_tables:
        name = tbl_cfg['name']
        database = tbl_cfg['database']
        table = tbl_cfg['table']
        date_col = tbl_cfg['date_col']
        full_table = f"{database}.{table}"

        print(f"\nExtracting: {name} ({full_table})")

        if "row" in types:
            _extract_row_athena(cursor, tbl_cfg, outdir)

        if "col" in types:
            # Discover or use configured columns
            columns = tbl_cfg.get('columns', {})
            if not columns:
                print(f"  Discovering columns from information_schema...")
                columns = _discover_columns_athena(cursor, database, table)
                # Remove date column
                columns.pop(date_col, None)
                print(f"  Found {len(columns)} columns")

            # Extract column stats (parallel)
            from concurrent.futures import ThreadPoolExecutor, as_completed

            all_stats = []

            def _extract_one(col_dtype):
                col, dtype = col_dtype
                if col.lower() == date_col.lower():
                    return []
                # Each thread needs its own cursor
                thread_cursor = conn.cursor()
                try:
                    return _extract_col_single(thread_cursor, tbl_cfg, col, dtype, full_table)
                finally:
                    thread_cursor.close()

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(_extract_one, (c, d)): c for c, d in columns.items()}
                for future in as_completed(futures):
                    col_name = futures[future]
                    try:
                        stats = future.result()
                        all_stats.extend(stats)
                    except Exception as e:
                        print(f"  Warning: Failed to extract {col_name}: {e}")

            # Write CSV
            if all_stats:
                csv_path = os.path.join(outdir, f"{name}_col.csv")
                fieldnames = [
                    'column_name', 'dt', 'col_type', 'n_total', 'n_missing',
                    'n_unique', 'mean', 'std', 'min_val', 'max_val', 'top_10',
                ]
                with open(csv_path, 'w', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    for stat in sorted(all_stats, key=lambda x: (x['column_name'], x['dt'])):
                        writer.writerow({k: stat.get(k, '') for k in fieldnames})

                print(f"  Column stats: {csv_path} ({len(all_stats)} rows)")

        # Write columns CSV when doing col extraction
        if "col" in types:
            cols_csv_path = os.path.join(outdir, f"{name}_columns.csv")
            with open(cols_csv_path, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['COLUMN_NAME', 'DATA_TYPE'])
                for col_name, col_type in sorted(columns.items()):
                    writer.writerow([col_name, col_type])
            print(f"  Column metadata: {cols_csv_path} ({len(columns)} columns)")

    cursor.close()
    conn.close()
    print(f"\nExtraction complete. Output in: {outdir}")


def discover_aws_columns(config_path, outdir):
    """
    Discover column metadata from AWS Athena tables (columns only, no data).

    Args:
        config_path: Path to extraction config JSON
        outdir: Directory to write column CSV files
    """
    try:
        from pyathena import connect as athena_connect
    except ImportError:
        print("Error: pyathena is required for AWS extraction")
        print("Install with: pip install 'dtrack[aws]'")
        return

    with open(config_path, 'r') as f:
        config = json.load(f)

    os.makedirs(outdir, exist_ok=True)

    aws_tables = [t for t in config['tables'] if t.get('source', '').lower() == 'aws']

    if not aws_tables:
        print("No AWS tables found in config")
        return

    conn = athena_connect()
    cursor = conn.cursor()

    for tbl_cfg in aws_tables:
        name = tbl_cfg['name']
        database = tbl_cfg['database']
        table = tbl_cfg['table']

        print(f"\nDiscovering columns: {name} ({database}.{table})")
        columns = _discover_columns_athena(cursor, database, table)

        csv_path = os.path.join(outdir, f"{name}_columns.csv")
        with open(csv_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['COLUMN_NAME', 'DATA_TYPE'])
            for col_name, col_type in sorted(columns.items()):
                writer.writerow([col_name, col_type])

        print(f"  {csv_path} ({len(columns)} columns)")

    cursor.close()
    conn.close()
    print(f"\nDiscovery complete. Output in: {outdir}")


def match_columns(pcds_csv, aws_csv, outfile=None):
    """
    Compare two column-metadata CSVs and match by case-insensitive name.

    Each CSV must have COLUMN_NAME and DATA_TYPE columns.

    Args:
        pcds_csv: Path to PCDS/Oracle columns CSV
        aws_csv: Path to AWS columns CSV
        outfile: Optional path to write JSON output
    """
    def _read_columns(path):
        cols = {}
        with open(path, 'r', newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                name = row.get('COLUMN_NAME') or row.get('column_name', '')
                dtype = row.get('DATA_TYPE') or row.get('data_type', '')
                if name:
                    cols[name] = dtype
        return cols

    pcds_cols = _read_columns(pcds_csv)
    aws_cols = _read_columns(aws_csv)

    # Build lookup: lowercase → original name
    pcds_lower = {k.lower(): k for k in pcds_cols}
    aws_lower = {k.lower(): k for k in aws_cols}

    matched = {}
    pcds_only = []
    aws_only = []

    # Match by case-insensitive name
    for lower_name, pcds_name in sorted(pcds_lower.items()):
        if lower_name in aws_lower:
            aws_name = aws_lower[lower_name]
            matched[pcds_name] = aws_name
        else:
            pcds_only.append({"name": pcds_name, "type": pcds_cols[pcds_name]})

    for lower_name, aws_name in sorted(aws_lower.items()):
        if lower_name not in pcds_lower:
            aws_only.append({"name": aws_name, "type": aws_cols[aws_name]})

    # Print results
    print(f"\nMatched ({len(matched)} columns):")
    for pcds_name, aws_name in sorted(matched.items()):
        pcds_type = pcds_cols[pcds_name]
        aws_type = aws_cols[aws_name]
        print(f"  {pcds_name} ({pcds_type}) <-> {aws_name} ({aws_type})")

    print(f"\nPCDS only ({len(pcds_only)}):")
    if pcds_only:
        for col in pcds_only:
            print(f"  {col['name']}  {col['type']}")
    else:
        print("  (none)")

    print(f"\nAWS only ({len(aws_only)}):")
    if aws_only:
        for col in aws_only:
            print(f"  {col['name']}  {col['type']}")
    else:
        print("  (none)")

    result = {
        "matched": matched,
        "pcds_only": pcds_only,
        "aws_only": aws_only,
        "manual_mapping": {},
    }

    if outfile:
        with open(outfile, 'w') as f:
            json.dump(result, f, indent=2)
        print(f"\nWritten to: {outfile}")

    return result
