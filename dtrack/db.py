"""Database operations for dtrack"""

import os
import sqlite3
from datetime import datetime
from typing import List, Dict, Tuple, Optional, Any

from .date_utils import parse_date


# Canonical schema: {table_name: [(col_name, col_type, constraint), ...]}
# constraint is '' for normal columns, 'PRIMARY KEY' etc. for special ones.
# This is the single source of truth for all table schemas.
_SCHEMA = {
    "_metadata": [
        ("table_name", "TEXT", "PRIMARY KEY"),
        ("source", "TEXT", ""),
        ("db", "TEXT", ""),
        ("source_table", "TEXT", ""),
        ("date_var", "TEXT", ""),
        ("source_file", "TEXT", ""),
        ("loaded_at", "TEXT", ""),
        ("last_updated", "TEXT", ""),
        ("row_count_total", "INTEGER", ""),
        ("load_mode", "TEXT", ""),
        ("vintage", "TEXT", ""),
        ("data_type", "TEXT", ""),
        ("where_clause", "TEXT", ""),
        ("date_format", "TEXT", ""),
        ("min_date_loaded", "TEXT", ""),
        ("max_date_loaded", "TEXT", ""),
    ],
    "_col_stats": [
        ("source_table", "TEXT", "NOT NULL"),
        ("column_name", "TEXT", "NOT NULL"),
        ("dt", "TEXT", "NOT NULL"),
        ("col_type", "TEXT", "NOT NULL"),
        ("n_total", "TEXT", ""),
        ("n_missing", "TEXT", ""),
        ("n_unique", "TEXT", ""),
        ("mean", "TEXT", ""),
        ("std", "TEXT", ""),
        ("min_val", "TEXT", ""),
        ("max_val", "TEXT", ""),
        ("top_10", "TEXT", ""),
        ("vintage_label", "TEXT", ""),
    ],
    "_column_meta": [
        ("source_table", "TEXT", "NOT NULL"),
        ("column_name", "TEXT", "NOT NULL"),
        ("data_type", "TEXT", ""),
        ("source", "TEXT", ""),
    ],
    "_table_pairs": [
        ("pair_name", "TEXT", "PRIMARY KEY"),
        ("table_left", "TEXT", "NOT NULL"),
        ("table_right", "TEXT", "NOT NULL"),
        ("source_left", "TEXT", ""),
        ("source_right", "TEXT", ""),
        ("col_mappings", "TEXT", ""),
        ("col_rules", "TEXT", ""),
        ("created_at", "TEXT", ""),
    ],
    "_row_counts": [
        ("source_table", "TEXT", "NOT NULL"),
        ("dt", "TEXT", "NOT NULL"),
        ("row_count", "INTEGER", "NOT NULL"),
    ],
    "_row_comparison": [
        ("pair_name", "TEXT", "PRIMARY KEY"),
        ("overlap_start", "TEXT", ""),
        ("overlap_end", "TEXT", ""),
        ("matching_dates", "TEXT", ""),
        ("excluded_dates", "TEXT", ""),
        ("created_at", "TEXT", ""),
        ("query_time", "TEXT", ""),
        ("where_left", "TEXT", ""),
        ("where_right", "TEXT", ""),
    ],
    "_col_comparison": [
        ("pair_name", "TEXT", "PRIMARY KEY"),
        ("columns_compared", "TEXT", ""),
        ("matched_columns", "TEXT", ""),
        ("diff_columns", "TEXT", ""),
        ("comparison_details", "TEXT", ""),
        ("created_at", "TEXT", ""),
    ],
    "_sample_date": [
        ("pair_name", "TEXT", "NOT NULL"),
        ("table_name", "TEXT", "NOT NULL"),
        ("samples", "TEXT", ""),
        ("sampled_at", "TEXT", "DEFAULT CURRENT_TIMESTAMP"),
    ],
}

# Composite primary keys (tables not using single-column PRIMARY KEY constraint)
_COMPOSITE_PKS = {
    "_col_stats": ["source_table", "column_name", "dt"],
    "_column_meta": ["source_table", "column_name"],
    "_row_counts": ["source_table", "dt"],
    "_sample_date": ["pair_name", "table_name"],
}


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
            data_type TEXT,
            where_clause TEXT,
            date_format TEXT,
            pair_side TEXT,
            min_date_loaded TEXT,
            max_date_loaded TEXT
        )
    """)

    # Create column statistics table (all TEXT for cross-source compatibility)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS _col_stats (
            source_table TEXT NOT NULL,
            column_name TEXT NOT NULL,
            dt TEXT NOT NULL,
            col_type TEXT NOT NULL,
            n_total TEXT,
            n_missing TEXT,
            n_unique TEXT,
            mean TEXT,
            std TEXT,
            min_val TEXT,
            max_val TEXT,
            top_10 TEXT,
            vintage_label TEXT,
            PRIMARY KEY (source_table, column_name, dt)
        )
    """)

    # Create column metadata table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS _column_meta (
            source_table TEXT NOT NULL,
            column_name TEXT NOT NULL,
            data_type TEXT,
            source TEXT,
            PRIMARY KEY (source_table, column_name)
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

    # Create unified row counts table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS _row_counts (
            source_table TEXT NOT NULL,
            dt TEXT NOT NULL,
            row_count INTEGER NOT NULL,
            PRIMARY KEY (source_table, dt)
        )
    """)

    # Create row comparison results table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS _row_comparison (
            pair_name TEXT PRIMARY KEY,
            overlap_start TEXT,
            overlap_end TEXT,
            matching_dates TEXT,
            excluded_dates TEXT,
            created_at TEXT,
            query_time TEXT,
            where_left TEXT,
            where_right TEXT
        )
    """)

    # Create column comparison results table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS _col_comparison (
            pair_name TEXT PRIMARY KEY,
            columns_compared TEXT,
            matched_columns TEXT,
            diff_columns TEXT,
            comparison_details TEXT,
            created_at TEXT
        )
    """)

    # Create sampled dates table (for vintage='sample')
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS _sample_date (
            pair_name TEXT NOT NULL,
            table_name TEXT NOT NULL,
            samples TEXT,
            sampled_at TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (pair_name, table_name)
        )
    """)

    conn.commit()
    conn.close()


def _build_create_sql(table_name: str) -> str:
    """Build CREATE TABLE SQL from _SCHEMA definition."""
    cols = _SCHEMA[table_name]
    parts = []
    for name, dtype, constraint in cols:
        part = f"{name} {dtype}"
        if constraint and constraint not in ("DEFAULT CURRENT_TIMESTAMP",):
            part += f" {constraint}"
        elif constraint == "DEFAULT CURRENT_TIMESTAMP":
            part += " DEFAULT CURRENT_TIMESTAMP"
        parts.append(part)
    if table_name in _COMPOSITE_PKS:
        pk_cols = ", ".join(_COMPOSITE_PKS[table_name])
        parts.append(f"PRIMARY KEY ({pk_cols})")
    return f"CREATE TABLE IF NOT EXISTS {table_name} (\n    " + ",\n    ".join(parts) + "\n)"


def refresh_database(db_path: str) -> Dict[str, str]:
    """Refresh database schema: add missing columns, recreate conflicting tables.

    For each table in _SCHEMA:
    - If table doesn't exist: create it
    - If table exists with matching columns: skip
    - If table exists but missing columns: ALTER TABLE ADD COLUMN
    - If table has type conflicts or extra constraints differ: DROP and recreate

    Args:
        db_path: Path to SQLite database file

    Returns:
        Dict mapping table names to action taken: 'created', 'ok', 'updated', 'recreated'
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    actions = {}

    for table_name, schema_cols in _SCHEMA.items():
        # Check if table exists
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,)
        )
        if not cursor.fetchone():
            cursor.execute(_build_create_sql(table_name))
            actions[table_name] = 'created'
            continue

        # Get existing columns: {name: type}
        cursor.execute(f"PRAGMA table_info({table_name})")
        existing = {row[1]: row[2] for row in cursor.fetchall()}

        expected = {name: dtype for name, dtype, _ in schema_cols}
        expected_names = [name for name, _, _ in schema_cols]

        # Check for type conflicts
        conflict = False
        for col_name, col_type in expected.items():
            if col_name in existing and existing[col_name].upper() != col_type.upper():
                conflict = True
                break

        if conflict:
            # Back up data, drop, recreate
            # Find overlapping columns to preserve data
            overlap_cols = [c for c in expected_names if c in existing]
            has_data = False
            if overlap_cols:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                has_data = cursor.fetchone()[0] > 0

            if has_data and overlap_cols:
                tmp = f"{table_name}__bak"
                cursor.execute(f"ALTER TABLE {table_name} RENAME TO {tmp}")
                cursor.execute(_build_create_sql(table_name))
                cols_str = ", ".join(overlap_cols)
                cursor.execute(f"INSERT INTO {table_name} ({cols_str}) SELECT {cols_str} FROM {tmp}")
                cursor.execute(f"DROP TABLE {tmp}")
            else:
                cursor.execute(f"DROP TABLE {table_name}")
                cursor.execute(_build_create_sql(table_name))
            actions[table_name] = 'recreated'
            continue

        # Check for missing columns
        missing = [name for name in expected_names if name not in existing]
        if missing:
            for col_name in missing:
                col_type = expected[col_name]
                # Find constraint
                constraint = next(c for n, t, c in schema_cols if n == col_name)
                col_def = col_type
                if constraint == "DEFAULT CURRENT_TIMESTAMP":
                    col_def += " DEFAULT CURRENT_TIMESTAMP"
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_def}")
            actions[table_name] = f'updated (+{", ".join(missing)})'
        else:
            actions[table_name] = 'ok'

    conn.commit()
    conn.close()
    return actions


def insert_row_counts(db_path: str, source_table: str, data: List[Tuple[str, int]]) -> None:
    """
    Insert row counts into the unified _row_counts table.

    Args:
        db_path: Path to SQLite database file
        source_table: Name of the source table
        data: List of (dt, row_count) tuples
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.executemany(
        "INSERT INTO _row_counts (source_table, dt, row_count) VALUES (?, ?, ?)",
        [(source_table, dt, count) for dt, count in data]
    )

    conn.commit()
    conn.close()


def upsert_row_counts(db_path: str, source_table: str, data: List[Tuple[str, int]]) -> None:
    """
    Upsert (INSERT OR REPLACE) row counts into the unified _row_counts table.

    Args:
        db_path: Path to SQLite database file
        source_table: Name of the source table
        data: List of (dt, row_count) tuples
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.executemany(
        "INSERT OR REPLACE INTO _row_counts (source_table, dt, row_count) VALUES (?, ?, ?)",
        [(source_table, dt, count) for dt, count in data]
    )

    conn.commit()
    conn.close()


def get_row_counts(
    db_path: str,
    source_table: str,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    limit: Optional[int] = None
) -> List[Tuple[str, int]]:
    """
    Get row counts from the unified _row_counts table.

    Args:
        db_path: Path to SQLite database file
        source_table: Name of the source table
        from_date: Optional start date filter (YYYY-MM-DD)
        to_date: Optional end date filter (YYYY-MM-DD)
        limit: Optional limit on number of rows

    Returns:
        List of (dt, row_count) tuples
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Fetch all rows for this table — date filtering is done in Python
    # because stored dt formats vary (ISO, YYYYMMDD, SAS numeric) and
    # SQL string comparison doesn't work across formats.
    query = "SELECT dt, row_count FROM _row_counts WHERE source_table = ?"
    params = [source_table]

    if limit and not from_date and not to_date:
        query += f" LIMIT {limit}"

    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()

    # Parse all dates to canonical format (YYYY-MM-DD or YYYYMM), then filter.
    # YYYYMM dates need comparison against YYYYMM-truncated from/to dates.
    parsed = [(parse_date(dt), count) for dt, count in rows]

    # Dedup: multiple raw formats can map to the same canonical date
    # (e.g. "20260321" and "21Mar2026" both → "2026-03-21").
    # Keep the higher count when duplicates exist.
    deduped = {}
    for dt, count in parsed:
        if dt not in deduped or count > deduped[dt]:
            deduped[dt] = count
    parsed = list(deduped.items())

    if from_date:
        from_ym = from_date[:4] + from_date[5:7]  # "2024-12-23" -> "202412"
        parsed = [(dt, c) for dt, c in parsed
                  if (dt >= from_date if len(dt) == 10 else dt >= from_ym)]
    if to_date:
        to_ym = to_date[:4] + to_date[5:7]
        parsed = [(dt, c) for dt, c in parsed
                  if (dt <= to_date if len(dt) == 10 else dt <= to_ym)]

    parsed.sort(key=lambda r: r[0])

    if limit and (from_date or to_date):
        parsed = parsed[:limit]

    return parsed


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

    # Ensure vintage_label column exists (for DBs created before this feature)
    try:
        cursor.execute("ALTER TABLE _col_stats ADD COLUMN vintage_label TEXT")
    except sqlite3.OperationalError:
        pass

    for stat in stats:
        cursor.execute("""
            INSERT OR REPLACE INTO _col_stats (
                source_table, column_name, dt, col_type,
                n_total, n_missing, n_unique, mean, std,
                min_val, max_val, top_10, vintage_label
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            stat.get("vintage_label"),
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

    # Fetch all rows for this table — date filtering is done in Python
    # because stored dt formats vary (ISO, YYYYMMDD, SAS datetime) and
    # SQL string comparison doesn't work across formats.
    query = "SELECT * FROM _col_stats WHERE source_table = ?"
    params = [source_table]

    if column_name:
        query += " AND column_name = ?"
        params.append(column_name)

    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()

    results = [dict(row) for row in rows]

    # Parse dates to canonical form for filtering and sorting
    if from_date or to_date:
        filtered = []
        for r in results:
            raw_dt = r.get("dt", "")
            if not raw_dt:
                continue
            try:
                canon = parse_date(raw_dt)
            except ValueError:
                canon = raw_dt
            if from_date and canon < from_date:
                continue
            if to_date and canon > to_date:
                continue
            filtered.append(r)
        results = filtered

    # Dedup: multiple raw formats can map to the same canonical date
    # (e.g. "23MAR2026:00:00:00" and "2026-03-23" for same column).
    # Keep the last entry seen per (column_name, canonical_dt).
    deduped = {}
    for r in results:
        raw = r.get("dt", "")
        try:
            canon = parse_date(raw)
        except ValueError:
            canon = raw
        key = (r.get("column_name", ""), canon)
        deduped[key] = r
    results = list(deduped.values())

    # Sort by parsed date, then column name
    def _sort_key(r):
        raw = r.get("dt", "")
        try:
            canon = parse_date(raw)
        except ValueError:
            canon = raw
        return (canon, r.get("column_name", ""))

    results.sort(key=_sort_key)

    if limit:
        results = results[:limit]

    return results


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
    metadata.setdefault("where_clause", None)
    metadata.setdefault("date_format", None)
    metadata.setdefault("pair_side", None)
    metadata.setdefault("min_date_loaded", None)
    metadata.setdefault("max_date_loaded", None)

    # Ensure newer columns exist (for DBs created before these features)
    for col in ('where_clause', 'date_format', 'pair_side', 'min_date_loaded', 'max_date_loaded'):
        try:
            cursor.execute(f"ALTER TABLE _metadata ADD COLUMN {col} TEXT")
        except sqlite3.OperationalError:
            pass  # column already exists

    cursor.execute("""
        INSERT OR REPLACE INTO _metadata (
            table_name, source, db, source_table, date_var,
            source_file, loaded_at, last_updated, row_count_total,
            load_mode, vintage, data_type, where_clause, date_format, pair_side,
            min_date_loaded, max_date_loaded
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        metadata["where_clause"],
        metadata["date_format"],
        metadata["pair_side"],
        metadata["min_date_loaded"],
        metadata["max_date_loaded"],
    ))

    conn.commit()
    conn.close()


def patch_metadata(db_path: str, table_name: str, **fields) -> None:
    """Update specific fields in _metadata without overwriting others."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM _metadata WHERE table_name = ?", (table_name,))
    if not cursor.fetchone():
        conn.close()
        return
    sets = ", ".join(f"{k} = ?" for k in fields)
    vals = list(fields.values()) + [table_name]
    cursor.execute(f"UPDATE _metadata SET {sets} WHERE table_name = ?", vals)
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

    # Update pair_side in _metadata for both tables
    for tbl_name, side in [(table_left, 'left'), (table_right, 'right')]:
        try:
            cursor.execute("ALTER TABLE _metadata ADD COLUMN pair_side TEXT")
        except sqlite3.OperationalError:
            pass
        cursor.execute(
            "UPDATE _metadata SET pair_side = ? WHERE table_name = ?",
            (side, tbl_name)
        )

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


def delete_pair(db_path: str, pair_name: str) -> Dict:
    """
    Delete a pair and all associated data from the database.

    Drops the left/right row-count tables, removes _metadata and _col_stats
    rows, and deletes the _table_pairs entry. All in one transaction.

    Args:
        db_path: Path to SQLite database file
        pair_name: Name of the pair to delete

    Returns:
        Dictionary with deleted pair info, or raises ValueError if not found
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("SELECT table_left, table_right FROM _table_pairs WHERE pair_name = ?",
                   (pair_name,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        raise ValueError(f"Pair '{pair_name}' not found in database")

    table_left, table_right = row["table_left"], row["table_right"]
    tables = [t for t in (table_left, table_right) if t]

    try:
        for t in tables:
            cursor.execute(f"DROP TABLE IF EXISTS [{t}]")
            cursor.execute("DELETE FROM _metadata WHERE table_name = ?", (t,))
            try:
                cursor.execute("DELETE FROM _col_stats WHERE table_name = ?", (t,))
            except sqlite3.OperationalError:
                pass  # _col_stats may not exist

        cursor.execute("DELETE FROM _table_pairs WHERE pair_name = ?", (pair_name,))
        conn.commit()
    finally:
        conn.close()

    return {"pair_name": pair_name, "table_left": table_left, "table_right": table_right}


def insert_column_meta(
    db_path: str,
    source_table: str,
    columns: Dict[str, str],
    source: Optional[str] = None,
) -> int:
    """
    Insert or update column metadata into _column_meta table.

    Args:
        db_path: Path to SQLite database file
        source_table: Table name these columns belong to
        columns: Dictionary mapping column names to data types
        source: Source identifier (e.g., 'oracle', 'aws')

    Returns:
        Number of columns inserted/updated
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Ensure table exists (for DBs created before this feature)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS _column_meta (
            source_table TEXT NOT NULL,
            column_name TEXT NOT NULL,
            data_type TEXT,
            source TEXT,
            PRIMARY KEY (source_table, column_name)
        )
    """)

    for col_name, data_type in columns.items():
        cursor.execute("""
            INSERT OR REPLACE INTO _column_meta
            (source_table, column_name, data_type, source)
            VALUES (?, ?, ?, ?)
        """, (source_table, col_name, data_type, source))

    conn.commit()
    conn.close()
    return len(columns)


def get_column_meta(db_path: str, source_table: str) -> List[Dict]:
    """
    Get column metadata for a table.

    Args:
        db_path: Path to SQLite database file
        source_table: Table name to get columns for

    Returns:
        List of dicts with column_name, data_type, source
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    try:
        cursor.execute(
            "SELECT * FROM _column_meta WHERE source_table = ? ORDER BY column_name",
            (source_table,)
        )
        rows = cursor.fetchall()
        return [dict(row) for row in rows]
    except sqlite3.OperationalError:
        return []
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Oracle connection helpers
# ---------------------------------------------------------------------------

MACRO2SVC = {
    "pb23": "pcbs_mkt_comnn",
    "pb30": "pcbs_mkt_comnn_30",
}

#>>> Solve LDAP DSN to get TNS connect string <<<#
def solve_ldap(ldap_dsn: str):
    import re
    from ldap3 import Server, Connection, ALL
    pattern = r"^ldap:\/\/(.+)\/(.+)\,(cn=OracleContext.*)$"
    x = re.match(pattern, ldap_dsn)
    if not x:
        return None
    else:
        ldap_server, db, ora_context = x.groups()
    server = Server(ldap_server, get_info=ALL)
    conn = Connection(server)
    conn.bind()
    conn.search(ora_context, f"(cn={db})", attributes=['orclNetDescString'])
    tns = conn.entries[0].orclNetDescString.value
    return tns



def oracle_connect(conn_macro: str):
    """Connect to Oracle using a connection macro name.

    Looks up the service name in MACRO2SVC, reads credentials from
    environment variables (PCDS_USR and {conn_macro}_pwd), and
    connects via oracledb.

    If DTRACK_ORACLE_MOCK is set, returns None (discover_columns
    will read from mock CSV files instead).

    Returns an oracledb connection object, or None when mocking.
    """
    if os.environ.get('DTRACK_MOCK') or os.environ.get('DTRACK_ORACLE_MOCK'):
        print(f"[mock] Skipping Oracle connection for '{conn_macro}'")
        return None

    import oracledb

    svc = MACRO2SVC.get(conn_macro)
    if not svc:
        raise ValueError(
            f"Unknown conn_macro '{conn_macro}'. "
            f"Known: {', '.join(MACRO2SVC.keys())}"
        )

    user = os.environ.get('PCDS_USR')
    pwd = os.environ.get(f'{conn_macro}_pwd')

    if not user:
        raise RuntimeError("PCDS_USR not set in environment")
    if not pwd:
        raise RuntimeError(f"{conn_macro}_pwd not set in environment")

    ldap_base = os.environ.get('LDAP_BASE', '')
    if ldap_base:
        ldap_dsn = f"{ldap_base}/cn={svc},cn=OracleContext"
        dsn = solve_ldap(ldap_dsn)
    else:
        dsn = svc

    return oracledb.connect(user=user, password=pwd, dsn=dsn)


def discover_columns(conn, table_name: str) -> Dict[str, str]:
    """Discover column metadata from Oracle ALL_TAB_COLUMNS.

    If DTRACK_ORACLE_MOCK is set to a directory path, reads columns from
    {mock_dir}/{table_name}_columns.csv instead of querying Oracle.

    Args:
        conn: An oracledb connection object (ignored when mocking)
        table_name: Oracle table name (case-sensitive as stored in catalog)

    Returns:
        Dict mapping column names to data types
    """
    import csv

    mock_dir = os.environ.get('DTRACK_MOCK') or os.environ.get('DTRACK_ORACLE_MOCK')
    if mock_dir:
        csv_path = os.path.join(mock_dir, f"{table_name}_columns.csv")
        if not os.path.exists(csv_path):
            print(f"[mock] File not found: {csv_path}")
            return {}
        columns = {}
        with open(csv_path, 'r', newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                name = row.get('COLUMN_NAME') or row.get('column_name', '')
                dtype = row.get('DATA_TYPE') or row.get('data_type', '')
                if name:
                    columns[name] = dtype
        print(f"[mock] Loaded {len(columns)} columns from {csv_path}")
        return columns

    cursor = conn.cursor()
    cursor.execute(
        "SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, "
        "DATA_SCALE, NULLABLE "
        "FROM ALL_TAB_COLUMNS WHERE TABLE_NAME = :tbl "
        "ORDER BY COLUMN_ID",
        {"tbl": table_name.upper()}
    )
    columns = {}
    for row in cursor.fetchall():
        columns[row[0]] = row[1]
    cursor.close()
    return columns


def save_row_comparison(
    db_path: str,
    pair_name: str,
    overlap_start: Optional[str],
    overlap_end: Optional[str],
    matching_dates: List[str],
    excluded_dates: List[str],
    query_time: Optional[str] = None,
    where_left: Optional[str] = None,
    where_right: Optional[str] = None,
) -> None:
    """Save row comparison results to _row_comparison table."""
    import json

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Ensure table exists with all columns
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS _row_comparison (
            pair_name TEXT PRIMARY KEY,
            overlap_start TEXT,
            overlap_end TEXT,
            matching_dates TEXT,
            excluded_dates TEXT,
            created_at TEXT,
            query_time TEXT,
            where_left TEXT,
            where_right TEXT
        )
    """)

    # Ensure where_left and where_right columns exist (for old databases)
    try:
        cursor.execute("ALTER TABLE _row_comparison ADD COLUMN where_left TEXT")
    except sqlite3.OperationalError:
        pass
    try:
        cursor.execute("ALTER TABLE _row_comparison ADD COLUMN where_right TEXT")
    except sqlite3.OperationalError:
        pass

    cursor.execute("""
        INSERT OR REPLACE INTO _row_comparison (
            pair_name, overlap_start, overlap_end,
            matching_dates, excluded_dates, created_at, query_time,
            where_left, where_right
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        pair_name,
        overlap_start,
        overlap_end,
        json.dumps(sorted(matching_dates)),
        json.dumps(sorted(excluded_dates)),
        datetime.now().isoformat(),
        query_time,
        where_left,
        where_right,
    ))

    conn.commit()
    conn.close()


def get_row_comparison(db_path: str, pair_name: str) -> Optional[Dict]:
    """Get row comparison results for a pair."""
    import json

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    try:
        cursor.execute(
            "SELECT * FROM _row_comparison WHERE pair_name = ?", (pair_name,)
        )
        row = cursor.fetchone()
    except sqlite3.OperationalError:
        return None
    finally:
        conn.close()

    if row:
        result = dict(row)
        result['matching_dates'] = json.loads(result['matching_dates']) if result['matching_dates'] else []
        result['excluded_dates'] = json.loads(result['excluded_dates']) if result['excluded_dates'] else []
        return result
    return None


def save_col_comparison(
    db_path: str,
    pair_name: str,
    columns_compared: List[str],
    matched_columns: List[str],
    diff_columns: List[str],
    comparison_details: Optional[Dict] = None,
) -> None:
    """Save column comparison results to _col_comparison table.

    Args:
        db_path: Path to database
        pair_name: Name of the table pair
        columns_compared: List of all columns compared
        matched_columns: List of columns with matching statistics
        diff_columns: List of columns with differing statistics
        comparison_details: Optional dict of full comparison results per column
    """
    import json

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Ensure table exists with all columns
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS _col_comparison (
            pair_name TEXT PRIMARY KEY,
            columns_compared TEXT,
            matched_columns TEXT,
            diff_columns TEXT,
            comparison_details TEXT,
            created_at TEXT
        )
    """)

    # Try to add comparison_details column if it doesn't exist (backward compatibility)
    try:
        cursor.execute("ALTER TABLE _col_comparison ADD COLUMN comparison_details TEXT")
    except sqlite3.OperationalError:
        pass  # Column already exists

    cursor.execute("""
        INSERT OR REPLACE INTO _col_comparison (
            pair_name, columns_compared, matched_columns, diff_columns, comparison_details, created_at
        ) VALUES (?, ?, ?, ?, ?, ?)
    """, (
        pair_name,
        json.dumps(sorted(columns_compared)),
        json.dumps(sorted(matched_columns)),
        json.dumps(sorted(diff_columns)),
        json.dumps(comparison_details) if comparison_details else None,
        datetime.now().isoformat(),
    ))

    conn.commit()
    conn.close()


def get_col_comparison(db_path: str, pair_name: str) -> Optional[Dict]:
    """Get column comparison results for a pair."""
    import json

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    try:
        cursor.execute(
            "SELECT * FROM _col_comparison WHERE pair_name = ?", (pair_name,)
        )
        row = cursor.fetchone()
    except sqlite3.OperationalError:
        return None
    finally:
        conn.close()

    if row:
        result = dict(row)
        result['columns_compared'] = json.loads(result['columns_compared']) if result['columns_compared'] else []
        result['matched_columns'] = json.loads(result['matched_columns']) if result['matched_columns'] else []
        result['diff_columns'] = json.loads(result['diff_columns']) if result['diff_columns'] else []
        result['comparison_details'] = json.loads(result['comparison_details']) if result.get('comparison_details') else None
        return result
    return None


def parse_where_clause(where: dict) -> Tuple[str, list]:
    """Parse a dict of conditions into SQL WHERE clause with parameters.

    Supports operators via value prefixes or auto-detection:
        col: val           → col = ?
        col: !=val         → col != ?
        col: ~=pattern     → col LIKE ?   (use % for wildcard)
        col: !~=pattern    → col NOT LIKE ?
        col: val1,val2     → col IN (?, ?)  (comma-separated, no spaces)
        col: %pattern%     → col LIKE ?   (auto-detected from % in value)

    Args:
        where: Dictionary of column=value conditions

    Returns:
        (sql_fragment, params) — e.g. ("col1=? AND col2 LIKE ?", [val1, val2])
    """
    parts = []
    params = []
    for col, val in where.items():
        if val.startswith('!~='):
            parts.append(f"{col} NOT LIKE ?")
            params.append(val[3:])
        elif val.startswith('~='):
            parts.append(f"{col} LIKE ?")
            params.append(val[2:])
        elif val.startswith('!='):
            parts.append(f"{col} != ?")
            params.append(val[2:])
        elif ',' in val and not val.startswith("'"):
            values = [v.strip() for v in val.split(',')]
            placeholders = ', '.join('?' for _ in values)
            parts.append(f"{col} IN ({placeholders})")
            params.extend(values)
        elif '%' in val:
            parts.append(f"{col} LIKE ?")
            params.append(val)
        else:
            parts.append(f"{col} = ?")
            params.append(val)
    return " AND ".join(parts), params


def _get_table_schema(cursor, table: str) -> Tuple[List[str], List[str]]:
    """Get column names and primary key columns for a table.

    Returns:
        (all_columns, pk_columns) — both as lists of column names
    """
    cursor.execute(f"PRAGMA table_info({table})")
    rows = cursor.fetchall()
    # PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
    all_cols = [r[1] for r in rows]
    pk_cols = [r[1] for r in rows if r[5] > 0]
    return all_cols, pk_cols


def generic_upsert(db_path: str, table: str, data: dict) -> int:
    """Smart upsert: UPDATE existing row or INSERT new row.

    Uses PRAGMA table_info to discover schema and primary key.
    - Validates all provided column names exist in the table
    - Requires all PK columns to be provided
    - If record exists (by PK): UPDATE only the provided non-PK columns
    - If record doesn't exist: INSERT (missing non-PK columns get defaults/NULL)

    Args:
        db_path: Path to SQLite database file
        table: Table name
        data: Dictionary of column=value assignments

    Returns:
        Number of affected rows
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Validate table exists
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,)
    )
    if not cursor.fetchone():
        conn.close()
        raise ValueError(f"Table '{table}' does not exist")

    all_cols, pk_cols = _get_table_schema(cursor, table)

    # Validate column names
    bad_cols = [c for c in data if c not in all_cols]
    if bad_cols:
        conn.close()
        raise ValueError(f"Unknown column(s) in '{table}': {', '.join(bad_cols)}")

    # Validate PK columns are provided
    missing_pk = [c for c in pk_cols if c not in data]
    if missing_pk:
        conn.close()
        raise ValueError(f"Missing primary key column(s): {', '.join(missing_pk)}")

    # Check if record already exists
    if pk_cols:
        where = " AND ".join(f"{c}=?" for c in pk_cols)
        cursor.execute(
            f"SELECT 1 FROM {table} WHERE {where}",
            [data[c] for c in pk_cols],
        )
        exists = cursor.fetchone() is not None
    else:
        exists = False

    if exists:
        # UPDATE only non-PK columns that were provided
        non_pk = {k: v for k, v in data.items() if k not in pk_cols}
        if non_pk:
            set_clause = ", ".join(f"{k}=?" for k in non_pk)
            where = " AND ".join(f"{c}=?" for c in pk_cols)
            sql = f"UPDATE {table} SET {set_clause} WHERE {where}"
            params = list(non_pk.values()) + [data[c] for c in pk_cols]
            cursor.execute(sql, params)
        # If only PK columns provided and record exists, nothing to update
    else:
        cols = ", ".join(data.keys())
        placeholders = ", ".join("?" for _ in data)
        sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
        cursor.execute(sql, list(data.values()))

    rowcount = cursor.rowcount
    conn.commit()
    conn.close()
    return rowcount


def generic_update(db_path: str, table: str, where: dict, updates: dict) -> int:
    """Update rows matching WHERE conditions with new values.

    Args:
        db_path: Path to SQLite database file
        table: Table name
        where: Dictionary of column=value conditions (ANDed) to find rows
        updates: Dictionary of column=value assignments to apply

    Returns:
        Number of updated rows
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Validate table exists
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,)
    )
    if not cursor.fetchone():
        conn.close()
        raise ValueError(f"Table '{table}' does not exist")

    all_cols, _ = _get_table_schema(cursor, table)
    bad_cols = [c for c in list(where) + list(updates) if c not in all_cols]
    if bad_cols:
        conn.close()
        raise ValueError(f"Unknown column(s) in '{table}': {', '.join(set(bad_cols))}")

    set_clause = ", ".join(f"{k}=?" for k in updates)
    where_sql, where_params = parse_where_clause(where)
    sql = f"UPDATE {table} SET {set_clause} WHERE {where_sql}"
    params = list(updates.values()) + where_params

    cursor.execute(sql, params)
    rowcount = cursor.rowcount
    conn.commit()
    conn.close()
    return rowcount


def generic_delete(db_path: str, table: str, where: dict) -> int:
    """Delete rows from any table matching WHERE conditions.

    Supports operators in values: !=, ~= (LIKE), !~= (NOT LIKE), comma (IN).

    Args:
        db_path: Path to SQLite database file
        table: Table name
        where: Dictionary of column=condition pairs

    Returns:
        Number of deleted rows
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Validate table exists
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,)
    )
    if not cursor.fetchone():
        conn.close()
        raise ValueError(f"Table '{table}' does not exist")

    all_cols, _ = _get_table_schema(cursor, table)
    bad_cols = [c for c in where if c not in all_cols]
    if bad_cols:
        conn.close()
        raise ValueError(f"Unknown column(s) in '{table}': {', '.join(bad_cols)}")

    where_sql, where_params = parse_where_clause(where)
    sql = f"DELETE FROM {table} WHERE {where_sql}"

    cursor.execute(sql, where_params)
    rowcount = cursor.rowcount
    conn.commit()
    conn.close()
    return rowcount


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

    # Get distinct source tables from _row_counts
    cursor.execute(
        "SELECT DISTINCT source_table FROM _row_counts ORDER BY source_table"
    )
    table_names = [row[0] for row in cursor.fetchall()]

    # Get metadata for each table
    result = []
    for table_name in table_names:
        cursor.execute("SELECT * FROM _metadata WHERE table_name = ?", (table_name,))
        row = cursor.fetchone()
        if row:
            result.append(dict(row))
        else:
            result.append({"table_name": table_name})

    conn.close()
    return result


def save_sampled_dates(db_path: str, pair_name: str, table_name: str, dates: List[str]) -> int:
    """
    Save sampled dates to _sample_date table as JSON array.

    Args:
        db_path: Path to database file
        pair_name: Name of the table pair
        table_name: Name of the specific table
        dates: List of date values that were sampled

    Returns:
        Number of dates saved
    """
    import json
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    now = datetime.now().isoformat()
    samples_json = json.dumps(dates)

    cursor.execute(
        "INSERT OR REPLACE INTO _sample_date (pair_name, table_name, samples, sampled_at) VALUES (?, ?, ?, ?)",
        (pair_name, table_name, samples_json, now)
    )

    conn.commit()
    count = len(dates)
    conn.close()
    return count


def get_sampled_dates(db_path: str, pair_name: str, table_name: str) -> List[str]:
    """
    Retrieve sampled dates from _sample_date table.

    Args:
        db_path: Path to database file
        pair_name: Name of the table pair
        table_name: Name of the specific table

    Returns:
        List of sampled date values (sorted)
    """
    import json
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute(
        "SELECT samples FROM _sample_date WHERE pair_name = ? AND table_name = ?",
        (pair_name, table_name)
    )

    row = cursor.fetchone()
    conn.close()

    if row and row[0]:
        return json.loads(row[0])
    return []


# ============================================================================
# CONFIG <-> DATABASE SYNC (UNIFIED FORMAT)
# ============================================================================

def update_pair_col_map(db_path: str, pair_name: str, col_map: Dict[str, str]) -> None:
    """
    Update col_mappings for a pair in _table_pairs.

    Args:
        db_path: Path to database file
        pair_name: Name of the pair
        col_map: Dictionary mapping left column names to right column names
    """
    import json

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    col_mappings_json = json.dumps(col_map) if col_map else None

    cursor.execute(
        "UPDATE _table_pairs SET col_mappings = ? WHERE pair_name = ?",
        (col_mappings_json, pair_name)
    )

    if cursor.rowcount == 0:
        conn.close()
        raise ValueError(f"Pair '{pair_name}' not found in database")

    conn.commit()
    conn.close()


def get_pair_col_map_from_db(db_path: str, pair_name: str) -> Dict[str, str]:
    """
    Get col_mappings for a pair from _table_pairs.

    Args:
        db_path: Path to database file
        pair_name: Name of the pair

    Returns:
        Dictionary mapping left column names to right column names
    """
    import json

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute(
        "SELECT col_mappings FROM _table_pairs WHERE pair_name = ?",
        (pair_name,)
    )

    row = cursor.fetchone()
    conn.close()

    if row and row[0]:
        return json.loads(row[0])
    return {}


def ensure_col_rules_column(db_path: str) -> None:
    """Add col_rules column to _table_pairs if it doesn't exist."""
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("ALTER TABLE _table_pairs ADD COLUMN col_rules TEXT")
    except sqlite3.OperationalError:
        pass  # column already exists
    conn.commit()
    conn.close()


def update_pair_col_rules(db_path: str, pair_name: str, rules_data: dict) -> None:
    """Save col_rules JSON for a pair in _table_pairs."""
    import json

    ensure_col_rules_column(db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    rules_json = json.dumps(rules_data) if rules_data else None
    cursor.execute(
        "UPDATE _table_pairs SET col_rules = ? WHERE pair_name = ?",
        (rules_json, pair_name)
    )

    if cursor.rowcount == 0:
        conn.close()
        raise ValueError(f"Pair '{pair_name}' not found in database")

    conn.commit()
    conn.close()


def get_pair_col_rules(db_path: str, pair_name: str) -> dict:
    """Load col_rules JSON for a pair from _table_pairs."""
    import json

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute(
            "SELECT col_rules FROM _table_pairs WHERE pair_name = ?",
            (pair_name,)
        )
    except sqlite3.OperationalError:
        conn.close()
        return {}

    row = cursor.fetchone()
    conn.close()

    if row and row[0]:
        return json.loads(row[0])
    return {}


def get_pair_where_map_from_db(db_path: str, pair_name: str) -> Optional[Dict[str, List[str]]]:
    """
    Get where_map (row comparison results) for a pair from _row_comparison.

    Args:
        db_path: Path to database file
        pair_name: Name of the pair

    Returns:
        Dictionary with matching_dates, left_only_dates, right_only_dates
        or None if comparison results not found
    """
    comp = get_row_comparison(db_path, pair_name)
    if not comp:
        return None

    # Convert excluded_dates format to left_only/right_only
    # Note: Current schema doesn't distinguish, we'll need to enhance this
    matching = comp.get('matching_dates', [])
    excluded = comp.get('excluded_dates', [])

    return {
        'matching_dates': matching,
        'left_only_dates': [],  # TODO: Split excluded into left/right
        'right_only_dates': excluded,  # For now, treat all excluded as right-only
    }


def sync_config_to_db(db_path: str, config: Dict[str, Any]) -> None:
    """
    Sync unified config to database (save col_map and where_map).

    For each pair in config:
    - Saves col_map to _table_pairs
    - Saves where_map to _row_comparison

    Args:
        db_path: Path to database file
        config: Unified configuration dictionary
    """
    import json

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    for pair_name, pair_config in config.get("pairs", {}).items():
        if pair_config.get("skip"):
            continue
        # Update col_map in _table_pairs
        col_map = pair_config.get("col_map", {})
        if col_map:
            col_mappings_json = json.dumps(col_map)
            cursor.execute(
                "UPDATE _table_pairs SET col_mappings = ? WHERE pair_name = ?",
                (col_mappings_json, pair_name)
            )

        # Update where_map and metadata in _row_comparison
        where_map = pair_config.get("where_map", {})
        metadata = pair_config.get("metadata", {})

        # Get query_time from metadata
        query_time = metadata.get("query_time")
        if query_time == "—":
            query_time = None

        # Update WHERE clauses if present (unified format)
        if where_map and ("left" in where_map or "right" in where_map):
            where_left = where_map.get("left", "")
            where_right = where_map.get("right", "")

            # Ensure columns exist
            try:
                cursor.execute("ALTER TABLE _row_comparison ADD COLUMN where_left TEXT")
            except sqlite3.OperationalError:
                pass
            try:
                cursor.execute("ALTER TABLE _row_comparison ADD COLUMN where_right TEXT")
            except sqlite3.OperationalError:
                pass

            # Update only where_left, where_right, and query_time (preserve date arrays)
            cursor.execute("""
                UPDATE _row_comparison
                SET where_left = ?, where_right = ?, query_time = ?
                WHERE pair_name = ?
            """, (where_left, where_right, query_time, pair_name))

    conn.commit()
    conn.close()


def sync_db_to_config(db_path: str, config: Dict[str, Any]) -> None:
    """
    Sync database state to unified config (load col_map and where_map).

    For each pair in config:
    - Loads col_map from _table_pairs
    - Loads where_map from _row_comparison
    - Loads query_time from _row_comparison

    Modifies config in place.

    Args:
        db_path: Path to database file
        config: Unified configuration dictionary
    """
    for pair_name in config.get("pairs", {}):
        # Load col_map from database
        col_map = get_pair_col_map_from_db(db_path, pair_name)
        if col_map:
            config["pairs"][pair_name]["col_map"] = col_map

        # Load where_map and query_time from database
        comp = get_row_comparison(db_path, pair_name)
        if comp:
            # Generate WHERE statements from date arrays (stored in DB)
            # WHERE statements are for display/editing in config
            matching = comp.get('matching_dates', [])
            excluded = comp.get('excluded_dates', [])

            # For now, store simple placeholder WHERE statements
            # TODO: Generate proper WHERE clauses from dates
            config["pairs"][pair_name]["where_map"] = {
                'left': f"— ({len(matching)} matching dates, {len(excluded)} excluded)",
                'right': f"— ({len(matching)} matching dates, {len(excluded)} excluded)",
            }

            # Load query_time into metadata
            if "metadata" not in config["pairs"][pair_name]:
                config["pairs"][pair_name]["metadata"] = {}
            if comp.get('query_time'):
                config["pairs"][pair_name]["metadata"]["query_time"] = comp['query_time']
