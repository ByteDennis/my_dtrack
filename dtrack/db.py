"""Database operations for dtrack"""

import sqlite3
from datetime import datetime
from typing import List, Dict, Tuple, Optional


def init_database(db_path: str) -> None:
    """
    Initialize a new dtrack database with required tables.

    Creates:
    - _metadata table: stores metadata about loaded tables
    - _col_stats table: stores column-level statistics

    Args:
        db_path: Path to SQLite database file
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create metadata table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS _metadata (
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
            data_type TEXT
        )
    """)

    # Create column statistics table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS _col_stats (
            source_table TEXT NOT NULL,
            column_name TEXT NOT NULL,
            dt TEXT NOT NULL,
            col_type TEXT NOT NULL,
            n_total INTEGER,
            n_missing INTEGER,
            n_unique INTEGER,
            mean REAL,
            std REAL,
            min_val TEXT,
            max_val TEXT,
            top_10 TEXT,
            PRIMARY KEY (source_table, column_name, dt)
        )
    """)

    # Create table pairs table for storing comparison mappings
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS _table_pairs (
            pair_name TEXT PRIMARY KEY,
            table_left TEXT NOT NULL,
            table_right TEXT NOT NULL,
            source_left TEXT,
            source_right TEXT,
            col_mappings TEXT,
            created_at TEXT
        )
    """)

    conn.commit()
    conn.close()


def create_row_count_table(db_path: str, table_name: str) -> None:
    """
    Create a row count table.

    Args:
        db_path: Path to SQLite database file
        table_name: Name of the table to create
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            dt TEXT PRIMARY KEY,
            row_count INTEGER NOT NULL
        )
    """)

    conn.commit()
    conn.close()


def insert_row_counts(db_path: str, table_name: str, data: List[Tuple[str, int]]) -> None:
    """
    Insert row counts into a table.

    Args:
        db_path: Path to SQLite database file
        table_name: Name of the table
        data: List of (dt, row_count) tuples
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.executemany(
        f"INSERT INTO {table_name} (dt, row_count) VALUES (?, ?)",
        data
    )

    conn.commit()
    conn.close()


def upsert_row_counts(db_path: str, table_name: str, data: List[Tuple[str, int]]) -> None:
    """
    Upsert (INSERT OR REPLACE) row counts into a table.

    Args:
        db_path: Path to SQLite database file
        table_name: Name of the table
        data: List of (dt, row_count) tuples
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.executemany(
        f"INSERT OR REPLACE INTO {table_name} (dt, row_count) VALUES (?, ?)",
        data
    )

    conn.commit()
    conn.close()


def get_row_counts(
    db_path: str,
    table_name: str,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    limit: Optional[int] = None
) -> List[Tuple[str, int]]:
    """
    Get row counts from a table.

    Args:
        db_path: Path to SQLite database file
        table_name: Name of the table
        from_date: Optional start date filter (YYYY-MM-DD)
        to_date: Optional end date filter (YYYY-MM-DD)
        limit: Optional limit on number of rows

    Returns:
        List of (dt, row_count) tuples
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    query = f"SELECT dt, row_count FROM {table_name} WHERE 1=1"
    params = []

    if from_date:
        query += " AND dt >= ?"
        params.append(from_date)
    if to_date:
        query += " AND dt <= ?"
        params.append(to_date)

    query += " ORDER BY dt"

    if limit:
        query += f" LIMIT {limit}"

    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()

    return rows


def insert_col_stats(db_path: str, stats: List[Dict]) -> None:
    """
    Insert column statistics into _col_stats table.

    Args:
        db_path: Path to SQLite database file
        stats: List of stat dictionaries with keys:
            source_table, column_name, dt, col_type, n_total, n_missing,
            n_unique, mean, std, min_val, max_val, top_10
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    for stat in stats:
        cursor.execute("""
            INSERT OR REPLACE INTO _col_stats (
                source_table, column_name, dt, col_type,
                n_total, n_missing, n_unique, mean, std,
                min_val, max_val, top_10
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            stat["source_table"],
            stat["column_name"],
            stat["dt"],
            stat["col_type"],
            stat["n_total"],
            stat["n_missing"],
            stat["n_unique"],
            stat["mean"],
            stat["std"],
            stat["min_val"],
            stat["max_val"],
            stat["top_10"],
        ))

    conn.commit()
    conn.close()


def get_col_stats(
    db_path: str,
    source_table: str,
    column_name: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    limit: Optional[int] = None
) -> List[Dict]:
    """
    Get column statistics from _col_stats table.

    Args:
        db_path: Path to SQLite database file
        source_table: Name of the source table
        column_name: Optional column name filter
        from_date: Optional start date filter (YYYY-MM-DD)
        to_date: Optional end date filter (YYYY-MM-DD)
        limit: Optional limit on number of rows

    Returns:
        List of stat dictionaries
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    query = "SELECT * FROM _col_stats WHERE source_table = ?"
    params = [source_table]

    if column_name:
        query += " AND column_name = ?"
        params.append(column_name)
    if from_date:
        query += " AND dt >= ?"
        params.append(from_date)
    if to_date:
        query += " AND dt <= ?"
        params.append(to_date)

    query += " ORDER BY dt, column_name"

    if limit:
        query += f" LIMIT {limit}"

    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()

    return [dict(row) for row in rows]


def update_metadata(db_path: str, metadata: Dict) -> None:
    """
    Update or insert metadata for a table.

    Args:
        db_path: Path to SQLite database file
        metadata: Dictionary with metadata fields
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Check if metadata exists
    cursor.execute(
        "SELECT loaded_at FROM _metadata WHERE table_name = ?",
        (metadata["table_name"],)
    )
    existing = cursor.fetchone()

    now = datetime.now().isoformat()

    if existing:
        # Update existing metadata
        loaded_at = existing[0]
        metadata["loaded_at"] = loaded_at
        metadata["last_updated"] = now
    else:
        # New metadata
        metadata["loaded_at"] = now
        metadata["last_updated"] = now

    # Set defaults for optional fields
    metadata.setdefault("source", None)
    metadata.setdefault("db", None)
    metadata.setdefault("source_table", None)
    metadata.setdefault("date_var", None)
    metadata.setdefault("source_file", None)
    metadata.setdefault("row_count_total", None)
    metadata.setdefault("load_mode", None)
    metadata.setdefault("vintage", None)
    metadata.setdefault("data_type", None)

    cursor.execute("""
        INSERT OR REPLACE INTO _metadata (
            table_name, source, db, source_table, date_var,
            source_file, loaded_at, last_updated, row_count_total,
            load_mode, vintage, data_type
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        metadata["table_name"],
        metadata["source"],
        metadata["db"],
        metadata["source_table"],
        metadata["date_var"],
        metadata["source_file"],
        metadata["loaded_at"],
        metadata["last_updated"],
        metadata["row_count_total"],
        metadata["load_mode"],
        metadata["vintage"],
        metadata["data_type"],
    ))

    conn.commit()
    conn.close()


def get_metadata(db_path: str, table_name: str) -> Optional[Dict]:
    """
    Get metadata for a table.

    Args:
        db_path: Path to SQLite database file
        table_name: Name of the table

    Returns:
        Dictionary with metadata or None if not found
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM _metadata WHERE table_name = ?", (table_name,))
    row = cursor.fetchone()
    conn.close()

    if row:
        return dict(row)
    return None


def register_table_pair(
    db_path: str,
    pair_name: str,
    table_left: str,
    table_right: str,
    source_left: Optional[str] = None,
    source_right: Optional[str] = None,
    col_mappings: Optional[Dict[str, str]] = None,
) -> None:
    """
    Register a table pair with column mappings.

    Args:
        db_path: Path to SQLite database file
        pair_name: Name of the pair
        table_left: Left table name (e.g., oracle table)
        table_right: Right table name (e.g., aws table)
        source_left: Source identifier for left table
        source_right: Source identifier for right table
        col_mappings: Dictionary mapping left column names to right column names
    """
    import json
    from datetime import datetime

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    col_mappings_json = json.dumps(col_mappings) if col_mappings else None
    created_at = datetime.now().isoformat()

    cursor.execute("""
        INSERT OR REPLACE INTO _table_pairs (
            pair_name, table_left, table_right, source_left, source_right,
            col_mappings, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        pair_name,
        table_left,
        table_right,
        source_left,
        source_right,
        col_mappings_json,
        created_at,
    ))

    conn.commit()
    conn.close()


def get_table_pair(db_path: str, pair_name: str) -> Optional[Dict]:
    """
    Get a table pair by name.

    Args:
        db_path: Path to SQLite database file
        pair_name: Name of the pair

    Returns:
        Dictionary with pair information or None if not found
    """
    import json

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM _table_pairs WHERE pair_name = ?", (pair_name,))
    row = cursor.fetchone()
    conn.close()

    if row:
        result = dict(row)
        # Parse col_mappings JSON
        if result['col_mappings']:
            result['col_mappings'] = json.loads(result['col_mappings'])
        else:
            result['col_mappings'] = {}
        return result
    return None


def list_table_pairs(db_path: str) -> List[Dict]:
    """
    List all registered table pairs.

    Args:
        db_path: Path to SQLite database file

    Returns:
        List of dictionaries with pair information
    """
    import json

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM _table_pairs ORDER BY pair_name")
    rows = cursor.fetchall()
    conn.close()

    result = []
    for row in rows:
        pair = dict(row)
        # Parse col_mappings JSON
        if pair['col_mappings']:
            pair['col_mappings'] = json.loads(pair['col_mappings'])
        else:
            pair['col_mappings'] = {}
        result.append(pair)

    return result


def list_tables(db_path: str) -> List[Dict]:
    """
    List all tables in the database with their metadata.

    Args:
        db_path: Path to SQLite database file

    Returns:
        List of dictionaries with table information
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Get all user tables (exclude system tables)
    cursor.execute(r"""
        SELECT name FROM sqlite_master
        WHERE type='table' AND name NOT LIKE '\_%' ESCAPE '\' AND name NOT LIKE 'sqlite_%'
        ORDER BY name
    """)
    table_names = [row[0] for row in cursor.fetchall()]

    # Get metadata for each table
    result = []
    for table_name in table_names:
        cursor.execute("SELECT * FROM _metadata WHERE table_name = ?", (table_name,))
        row = cursor.fetchone()
        if row:
            result.append(dict(row))
        else:
            # Table exists but no metadata
            result.append({"table_name": table_name})

    conn.close()
    return result
