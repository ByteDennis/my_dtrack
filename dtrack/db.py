
import json
import os
import sqlite3
from datetime import datetime
from typing import List, Dict, Tuple, Optional, Any

from .date_utils import parse_date

_SCHEMA = {
    "_metadata": [
        ("table_name", "TEXT", "PRIMARY KEY"),
        ("source", "TEXT", ""),
        ("source_table", "TEXT", ""),
        ("date_var", "TEXT", ""),
        ("row_count_total", "INTEGER", ""),
        ("vintage", "TEXT", ""),
        ("data_type", "TEXT", ""),
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
        ("length_avg", "TEXT", ""),
        ("length_max", "TEXT", ""),
        ("hash_sum", "TEXT", ""),
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
        ("col_include", "TEXT", ""),
        ("col_exclude", "TEXT", ""),
        ("col_rejected", "TEXT", ""),
        ("col_type_overrides", "TEXT", ""),
        ("col_type_overrides_right", "TEXT", ""),
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
        ("non_matching_dates", "TEXT", ""),
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
    "_playground_history": [
        ("id", "INTEGER", "PRIMARY KEY AUTOINCREMENT"),
        ("engine", "TEXT", "NOT NULL"),
        ("conn", "TEXT", ""),
        ("sql", "TEXT", "NOT NULL"),
        ("elapsed_sec", "REAL", ""),
        ("n_rows", "INTEGER", ""),
        ("status", "TEXT", "NOT NULL"),
        ("error_msg", "TEXT", ""),
        ("note", "TEXT", "DEFAULT ''"),
        ("ts_utc", "TEXT", "NOT NULL"),
    ],
}

_COMPOSITE_PKS = {
    "_col_stats": ["source_table", "column_name", "dt"],
    "_column_meta": ["source_table", "column_name"],
    "_row_counts": ["source_table", "dt"],
    "_sample_date": ["pair_name", "table_name"],
}

# >>> Initialize a new dtrack database with required tables <<< #
def init_database(db_path: str) -> None:
    parent = os.path.dirname(os.path.abspath(db_path))
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS _metadata (
            table_name TEXT PRIMARY KEY,
            source TEXT,
            source_table TEXT,
            date_var TEXT,
            row_count_total INTEGER,
            vintage TEXT,
            data_type TEXT,
            date_format TEXT,
            min_date_loaded TEXT,
            max_date_loaded TEXT
        )
    """)

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

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS _column_meta (
            source_table TEXT NOT NULL,
            column_name TEXT NOT NULL,
            data_type TEXT,
            source TEXT,
            PRIMARY KEY (source_table, column_name)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS _table_pairs (
            pair_name TEXT PRIMARY KEY,
            table_left TEXT NOT NULL,
            table_right TEXT NOT NULL,
            source_left TEXT,
            source_right TEXT,
            col_mappings TEXT,
            col_rules TEXT,
            col_include TEXT,
            col_exclude TEXT,
            created_at TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS _row_counts (
            source_table TEXT NOT NULL,
            dt TEXT NOT NULL,
            row_count INTEGER NOT NULL,
            PRIMARY KEY (source_table, dt)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS _row_comparison (
            pair_name TEXT PRIMARY KEY,
            overlap_start TEXT,
            overlap_end TEXT,
            matching_dates TEXT,
            excluded_dates TEXT,
            non_matching_dates TEXT,
            created_at TEXT,
            query_time TEXT,
            where_left TEXT,
            where_right TEXT
        )
    """)

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

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS _playground_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            engine TEXT NOT NULL,
            conn TEXT,
            sql TEXT NOT NULL,
            elapsed_sec REAL,
            n_rows INTEGER,
            status TEXT NOT NULL,
            error_msg TEXT,
            note TEXT DEFAULT '',
            ts_utc TEXT NOT NULL
        )
    """)

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

# >>> Build CREATE TABLE SQL from _SCHEMA definition <<< #
def _build_create_sql(table_name: str) -> str:
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

# >>> Refresh database schema: add missing columns, recreate conflicting tables <<< #
def refresh_database(db_path: str) -> Dict[str, str]:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    actions = {}

    for table_name, schema_cols in _SCHEMA.items():
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,)
        )
        if not cursor.fetchone():
            cursor.execute(_build_create_sql(table_name))
            actions[table_name] = 'created'
            continue

        cursor.execute(f"PRAGMA table_info({table_name})")
        existing = {row[1]: row[2] for row in cursor.fetchall()}

        expected = {name: dtype for name, dtype, _ in schema_cols}
        expected_names = [name for name, _, _ in schema_cols]

        conflict = False
        for col_name, col_type in expected.items():
            if col_name in existing and existing[col_name].upper() != col_type.upper():
                conflict = True
                break

        extra_cols = [c for c in existing if c not in expected]
        needs_rebuild = conflict or bool(extra_cols)

        if needs_rebuild:
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

            detail = []
            if extra_cols:
                detail.append(f"-{', '.join(extra_cols)}")
            if conflict:
                detail.append("type fix")
            actions[table_name] = f'rebuilt ({"; ".join(detail)})'
            continue

        missing = [name for name in expected_names if name not in existing]
        if missing:
            for col_name in missing:
                col_type = expected[col_name]
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

# >>> Insert row counts into the unified _row_counts table <<< #
def insert_row_counts(db_path: str, source_table: str, data: List[Tuple[str, int]]) -> None:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.executemany(
        "INSERT INTO _row_counts (source_table, dt, row_count) VALUES (?, ?, ?)",
        [(source_table, dt, count) for dt, count in data]
    )

    conn.commit()
    conn.close()

# >>> Upsert (INSERT OR REPLACE) row counts into the unified _row_counts table <<< #
def upsert_row_counts(db_path: str, source_table: str, data: List[Tuple[str, int]]) -> None:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.executemany(
        "INSERT OR REPLACE INTO _row_counts (source_table, dt, row_count) VALUES (?, ?, ?)",
        [(source_table, dt, count) for dt, count in data]
    )

    conn.commit()
    conn.close()

# >>> Get row counts from the unified _row_counts table <<< #
def get_row_counts(
    db_path: str,
    source_table: str,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    limit: Optional[int] = None
) -> List[Tuple[str, int]]:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    query = "SELECT dt, row_count FROM _row_counts WHERE source_table = ?"
    params = [source_table]

    if limit and not from_date and not to_date:
        query += f" LIMIT {limit}"

    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()

    parsed = []
    for dt, count in rows:
        if dt is None or (isinstance(dt, str) and not dt.strip()):
            continue
        try:
            parsed.append((parse_date(dt), count))
        except (ValueError, AttributeError):
            continue

    deduped = {}
    for dt, count in parsed:
        if dt not in deduped or count > deduped[dt]:
            deduped[dt] = count
    parsed = list(deduped.items())

    if from_date:
        from_ym = from_date[:4] + from_date[5:7]
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

# >>> Insert column statistics into _col_stats table <<< #
def insert_col_stats(db_path: str, stats: List[Dict], mode: str = "fine") -> None:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute("ALTER TABLE _col_stats ADD COLUMN vintage_label TEXT")
    except sqlite3.OperationalError:
        pass
    for col in ("length_avg", "length_max", "hash_sum"):
        try:
            cursor.execute(f"ALTER TABLE _col_stats ADD COLUMN {col} TEXT")
        except sqlite3.OperationalError:
            pass

    if mode == "coarse_merge":
        sql = """
            INSERT INTO _col_stats (
                source_table, column_name, dt, col_type,
                n_total, n_missing, n_unique,
                min_val, max_val, vintage_label,
                length_avg, length_max, hash_sum
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_table, column_name, dt) DO UPDATE SET
                col_type     = excluded.col_type,
                n_total      = excluded.n_total,
                n_missing    = excluded.n_missing,
                n_unique     = excluded.n_unique,
                min_val      = excluded.min_val,
                max_val      = excluded.max_val,
                vintage_label= excluded.vintage_label,
                length_avg   = excluded.length_avg,
                length_max   = excluded.length_max,
                hash_sum     = excluded.hash_sum
        """
        for stat in stats:
            cursor.execute(sql, (
                stat["source_table"],
                stat["column_name"],
                stat["dt"],
                stat.get("col_type", "categorical"),
                stat.get("n_total", ""),
                stat.get("n_missing", ""),
                stat.get("n_unique", ""),
                stat.get("min_val", ""),
                stat.get("max_val", ""),
                stat.get("vintage_label"),
                stat.get("length_avg", ""),
                stat.get("length_max", ""),
                stat.get("hash_sum", ""),
            ))
    else:
        sql = """
            INSERT OR REPLACE INTO _col_stats (
                source_table, column_name, dt, col_type,
                n_total, n_missing, n_unique, mean, std,
                min_val, max_val, top_10, vintage_label,
                length_avg, length_max, hash_sum
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        for stat in stats:
            cursor.execute(sql, (
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
                stat.get("length_avg", ""),
                stat.get("length_max", ""),
                stat.get("hash_sum", ""),
            ))

    conn.commit()
    conn.close()

# >>> Get column statistics rows for one source table <<< #
def get_col_stats(
    db_path: str,
    source_table: str,
    column_name: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[Dict]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    query = "SELECT * FROM _col_stats WHERE source_table = ?"
    params = [source_table]

    if column_name:
        query += " AND column_name = ?"
        params.append(column_name)

    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()

    results = [dict(row) for row in rows]

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
            if canon not in ('all', 'sample'):
                if from_date and canon < from_date:
                    continue
                if to_date and canon > to_date:
                    continue
            filtered.append(r)
        results = filtered

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

# >>> Update or insert metadata for a table <<< #
def update_metadata(db_path: str, metadata: Dict) -> None:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    metadata.setdefault("source", None)
    metadata.setdefault("source_table", None)
    metadata.setdefault("date_var", None)
    metadata.setdefault("row_count_total", None)
    metadata.setdefault("vintage", None)
    metadata.setdefault("data_type", None)
    metadata.setdefault("date_format", None)
    metadata.setdefault("min_date_loaded", None)
    metadata.setdefault("max_date_loaded", None)

    cursor.execute("""
        INSERT OR REPLACE INTO _metadata (
            table_name, source, source_table, date_var,
            row_count_total, vintage, data_type, date_format,
            min_date_loaded, max_date_loaded
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        metadata["table_name"],
        metadata["source"],
        metadata["source_table"],
        metadata["date_var"],
        metadata["row_count_total"],
        metadata["vintage"],
        metadata["data_type"],
        metadata["date_format"],
        metadata["min_date_loaded"],
        metadata["max_date_loaded"],
    ))

    conn.commit()
    conn.close()

# >>> Update specific fields in _metadata without overwriting others <<< #
def patch_metadata(db_path: str, table_name: str, **fields) -> None:
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

# >>> Recompute NULL metadata fields from actual _row_counts and _col_stats data <<< #
def refresh_metadata_from_data(db_path: str) -> int:
    from .date_utils import parse_date

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM _metadata")
    meta_rows = [dict(r) for r in cursor.fetchall()]

    updated = 0
    for meta in meta_rows:
        tname = meta["table_name"]
        patches = {}

        cursor.execute(
            "SELECT dt, row_count FROM _row_counts WHERE source_table = ?", (tname,))
        row_data = cursor.fetchall()
        if row_data:
            canonical_dates = []
            total = 0
            for dt, count in row_data:
                total += count
                try:
                    canonical_dates.append(parse_date(dt))
                except ValueError:
                    canonical_dates.append(dt)
            canonical_dates.sort()

            if not meta.get("min_date_loaded") and canonical_dates:
                patches["min_date_loaded"] = canonical_dates[0]
            if not meta.get("max_date_loaded") and canonical_dates:
                patches["max_date_loaded"] = canonical_dates[-1]
            if not meta.get("row_count_total"):
                patches["row_count_total"] = total
            if not meta.get("data_type"):
                patches["data_type"] = "row"

        cursor.execute(
            "SELECT dt FROM _col_stats WHERE source_table = ? GROUP BY dt", (tname,))
        col_dates = cursor.fetchall()
        if col_dates:
            canonical = []
            for (dt,) in col_dates:
                try:
                    canonical.append(parse_date(dt))
                except ValueError:
                    canonical.append(dt)
            canonical.sort()

            if not meta.get("min_date_loaded") and "min_date_loaded" not in patches and canonical:
                patches["min_date_loaded"] = canonical[0]
            if not meta.get("max_date_loaded") and "max_date_loaded" not in patches and canonical:
                patches["max_date_loaded"] = canonical[-1]
            if not meta.get("data_type") and "data_type" not in patches:
                patches["data_type"] = "col"

        if patches:
            sets = ", ".join(f"{k} = ?" for k in patches)
            vals = list(patches.values()) + [tname]
            cursor.execute(f"UPDATE _metadata SET {sets} WHERE table_name = ?", vals)
            updated += 1

    conn.commit()
    conn.close()
    return updated

# >>> Get metadata for a table <<< #
def get_metadata(db_path: str, table_name: str) -> Optional[Dict]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM _metadata WHERE table_name = ?", (table_name,))
    row = cursor.fetchone()
    conn.close()

    if row:
        return dict(row)
    return None

# >>> Register a table pair with column mappings <<< #
def register_table_pair(
    db_path: str,
    pair_name: str,
    table_left: str,
    table_right: str,
    source_left: Optional[str] = None,
    source_right: Optional[str] = None,
    col_mappings: Optional[Dict[str, str]] = None,
) -> None:
    import json
    from datetime import datetime

    ensure_col_rules_column(db_path)
    ensure_col_filter_columns(db_path)
    ensure_coarse_stat_columns(db_path)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute(
        "SELECT col_mappings, col_rules, col_include, col_exclude "
        "FROM _table_pairs WHERE pair_name = ?",
        (pair_name,),
    )
    existing = cursor.fetchone() or (None, None, None, None)
    existing_map, existing_rules, existing_include, existing_exclude = existing

    if col_mappings is None:
        col_mappings_json = existing_map
    else:
        col_mappings_json = json.dumps(col_mappings) if col_mappings else None

    created_at = datetime.now().isoformat()

    cursor.execute("""
        INSERT OR REPLACE INTO _table_pairs (
            pair_name, table_left, table_right, source_left, source_right,
            col_mappings, col_rules, col_include, col_exclude, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        pair_name,
        table_left,
        table_right,
        source_left,
        source_right,
        col_mappings_json,
        existing_rules,
        existing_include,
        existing_exclude,
        created_at,
    ))

    conn.commit()
    conn.close()

# >>> Get a table pair by name <<< #
def get_table_pair(db_path: str, pair_name: str) -> Optional[Dict]:
    import json

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM _table_pairs WHERE pair_name = ?", (pair_name,))
    row = cursor.fetchone()
    conn.close()

    if row:
        result = dict(row)
        if result['col_mappings']:
            result['col_mappings'] = json.loads(result['col_mappings'])
        else:
            result['col_mappings'] = {}
        return result
    return None

# >>> List all registered table pairs <<< #
def list_table_pairs(db_path: str) -> List[Dict]:
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
        if pair['col_mappings']:
            pair['col_mappings'] = json.loads(pair['col_mappings'])
        else:
            pair['col_mappings'] = {}
        result.append(pair)

    return result

# >>> Delete a pair and all associated data from the database <<< #
def delete_pair(db_path: str, pair_name: str) -> Dict:
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
                pass

        cursor.execute("DELETE FROM _table_pairs WHERE pair_name = ?", (pair_name,))
        conn.commit()
    finally:
        conn.close()

    return {"pair_name": pair_name, "table_left": table_left, "table_right": table_right}

# >>> Insert or update column metadata into _column_meta table <<< #
def insert_column_meta(
    db_path: str,
    source_table: str,
    columns: Dict[str, str],
    source: Optional[str] = None,
) -> int:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

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

# >>> Get column metadata for a table <<< #
def get_column_meta(db_path: str, source_table: str) -> List[Dict]:
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

MACRO2SVC = {
    "pb23": "pcbs_mkt_comnn",
    "pb30": "pcbs_mkt_comnn_30",
}

# >>> Read additional Oracle macros from env without touching MACRO2SVC <<< #
def _load_macro_overrides() -> dict:
    extra: dict = {}
    blob = os.environ.get('DTRACK_ORACLE_MACROS', '').strip()
    if blob:
        for part in blob.split(','):
            if ':' in part:
                k, v = part.split(':', 1)
                k = k.strip()
                v = v.strip()
                if k and v:
                    extra[k] = v
    cfg = os.environ.get('DTRACK_ORACLE_MACROS_FILE')
    if cfg:
        try:
            from pathlib import Path as _P
            if _P(cfg).exists():
                extra.update(json.loads(_P(cfg).read_text()))
        except Exception:
            pass
    return extra

# >>> Look up a service name for `conn_macro` from MACRO2SVC + overrides <<< #
def resolve_oracle_macro(conn_macro: str) -> Optional[str]:
    return {**MACRO2SVC, **_load_macro_overrides()}.get(conn_macro)

#>>> Solve LDAP DSN to get TNS connect string <<<#
def solve_ldap(ldap_dsn: str):
    import re
    from ldap3 import Server, Connection, ALL
    pattern = r"^ldap:\/\/(.+)\/(.+)\,(cn=OracleContext.*)$"
    x = re.match(pattern, ldap_dsn)
    if not x:
        return None
    ldap_server, db, ora_context = x.groups()
    server = Server(ldap_server, get_info=ALL)
    conn = Connection(server)
    if not conn.bind():
        raise RuntimeError(
            f"LDAP bind to {ldap_server} failed: {conn.result!r}"
        )
    conn.search(ora_context, f"(cn={db})", attributes=['orclNetDescString'])
    if not conn.entries:
        raise RuntimeError(
            f"LDAP search found no entry for service '{db}' on {ldap_server} "
            f"(context={ora_context!r}). Check DTRACK_ORACLE_MACROS / MACRO2SVC "
            f"for the connection macro and that the service is registered in "
            f"the Oracle context."
        )
    entry = conn.entries[0]
    if not getattr(entry, 'orclNetDescString', None):
        raise RuntimeError(
            f"LDAP entry for '{db}' has no orclNetDescString attribute "
            f"(found: {[a.key for a in entry] if hasattr(entry, '__iter__') else '?'})."
        )
    return entry.orclNetDescString.value

# >>> Connect to Oracle using a connection macro name <<< #
def oracle_connect(conn_macro: str, *, user: Optional[str] = None,
                   password: Optional[str] = None,
                   service: Optional[str] = None,
                   ldap_dsn: Optional[str] = None):
    import oracledb

    svc = service or resolve_oracle_macro(conn_macro)
    if not svc:
        merged = {**MACRO2SVC, **_load_macro_overrides()}
        raise ValueError(
            f"Unknown conn_macro '{conn_macro}'. "
            f"Known: {', '.join(sorted(merged.keys())) or '<empty>'}. "
            f"Set DTRACK_ORACLE_MACROS=name:service to add more."
        )

    user = (user
            or os.environ.get(f'{conn_macro}_USR')
            or os.environ.get('PCDS_USR'))
    password = (password
                or os.environ.get(f'{conn_macro}_PWD')
                or os.environ.get(f'{conn_macro}_pwd'))
    if not user:
        raise RuntimeError(
            f"No Oracle user for '{conn_macro}': set {conn_macro}_USR or PCDS_USR")
    if not password:
        raise RuntimeError(
            f"No Oracle password for '{conn_macro}': set {conn_macro}_PWD or {conn_macro}_pwd")

    tmpl = ldap_dsn if ldap_dsn is not None else os.environ.get('LDAP_DSN', '')
    if tmpl:
        dsn = solve_ldap(tmpl.format(service=svc))
    else:
        dsn = svc

    return oracledb.connect(user=user, password=password, dsn=dsn)

# >>> Discover column metadata from Oracle ALL_TAB_COLUMNS <<< #
def discover_columns_oracle(conn, table_name: str) -> Dict[str, str]:
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

# >>> Save row comparison results to _row_comparison table <<< #
def save_row_comparison(
    db_path: str,
    pair_name: str,
    overlap_start: Optional[str],
    overlap_end: Optional[str],
    matching_dates: List[str],
    excluded_dates: List[str],
    non_matching_dates: Optional[List[str]] = None,
    query_time: Optional[str] = None,
    where_left: Optional[str] = None,
    where_right: Optional[str] = None,
) -> None:
    import json

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        INSERT OR REPLACE INTO _row_comparison (
            pair_name, overlap_start, overlap_end,
            matching_dates, excluded_dates, non_matching_dates,
            created_at, query_time, where_left, where_right
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        pair_name,
        overlap_start,
        overlap_end,
        json.dumps(sorted(matching_dates)),
        json.dumps(sorted(excluded_dates)),
        json.dumps(sorted(non_matching_dates)) if non_matching_dates else '[]',
        datetime.now().isoformat(),
        query_time,
        where_left,
        where_right,
    ))

    conn.commit()
    conn.close()

# >>> Get row comparison results for a pair <<< #
def get_row_comparison(db_path: str, pair_name: str) -> Optional[Dict]:
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

# >>> Save column comparison results to _col_comparison table <<< #
def save_col_comparison(
    db_path: str,
    pair_name: str,
    columns_compared: List[str],
    matched_columns: List[str],
    diff_columns: List[str],
    comparison_details: Optional[Dict] = None,
) -> None:
    import json

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

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

    try:
        cursor.execute("ALTER TABLE _col_comparison ADD COLUMN comparison_details TEXT")
    except sqlite3.OperationalError:
        pass

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

# >>> Get column comparison results for a pair <<< #
def get_col_comparison(db_path: str, pair_name: str) -> Optional[Dict]:
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

# >>> Parse a dict of conditions into SQL WHERE clause with parameters <<< #
def parse_where_clause(where: dict) -> Tuple[str, list]:
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

# >>> Get column names and primary key columns for a table <<< #
def _get_table_schema(cursor, table: str) -> Tuple[List[str], List[str]]:
    cursor.execute(f"PRAGMA table_info({table})")
    rows = cursor.fetchall()
    all_cols = [r[1] for r in rows]
    pk_cols = [r[1] for r in rows if r[5] > 0]
    return all_cols, pk_cols

# >>> Smart upsert: UPDATE existing row or INSERT new row <<< #
def generic_upsert(db_path: str, table: str, data: dict) -> int:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,)
    )
    if not cursor.fetchone():
        conn.close()
        raise ValueError(f"Table '{table}' does not exist")

    all_cols, pk_cols = _get_table_schema(cursor, table)

    bad_cols = [c for c in data if c not in all_cols]
    if bad_cols:
        conn.close()
        raise ValueError(f"Unknown column(s) in '{table}': {', '.join(bad_cols)}")

    missing_pk = [c for c in pk_cols if c not in data]
    if missing_pk:
        conn.close()
        raise ValueError(f"Missing primary key column(s): {', '.join(missing_pk)}")

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
        non_pk = {k: v for k, v in data.items() if k not in pk_cols}
        if non_pk:
            set_clause = ", ".join(f"{k}=?" for k in non_pk)
            where = " AND ".join(f"{c}=?" for c in pk_cols)
            sql = f"UPDATE {table} SET {set_clause} WHERE {where}"
            params = list(non_pk.values()) + [data[c] for c in pk_cols]
            cursor.execute(sql, params)
    else:
        cols = ", ".join(data.keys())
        placeholders = ", ".join("?" for _ in data)
        sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
        cursor.execute(sql, list(data.values()))

    rowcount = cursor.rowcount
    conn.commit()
    conn.close()
    return rowcount

# >>> Update rows matching WHERE conditions with new values <<< #
def generic_update(db_path: str, table: str, where: dict, updates: dict) -> int:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

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

# >>> Delete rows from any table matching WHERE conditions <<< #
def generic_delete(db_path: str, table: str, where: dict) -> int:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

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

# >>> List all tables in the database with their metadata <<< #
def list_tables(db_path: str) -> List[Dict]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute(
        "SELECT DISTINCT source_table FROM _row_counts ORDER BY source_table"
    )
    table_names = [row[0] for row in cursor.fetchall()]

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

# >>> Save sampled dates to _sample_date table as JSON array <<< #
def save_sampled_dates(db_path: str, pair_name: str, table_name: str, dates: List[str]) -> int:
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

# >>> Retrieve sampled dates from _sample_date table <<< #
def get_sampled_dates(db_path: str, pair_name: str, table_name: str) -> List[str]:
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

# >>> Update col_mappings for a pair in _table_pairs <<< #
def update_pair_col_map(db_path: str, pair_name: str, col_map: Dict[str, str]) -> None:
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

# >>> Get col_mappings for a pair from _table_pairs <<< #
def get_pair_col_map_from_db(db_path: str, pair_name: str) -> Dict[str, str]:
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

# >>> Add col_rules column to _table_pairs if it doesn't exist <<< #
def ensure_col_rules_column(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("ALTER TABLE _table_pairs ADD COLUMN col_rules TEXT")
    except sqlite3.OperationalError:
        pass
    conn.commit()
    conn.close()

# >>> Save col_rules JSON for a pair in _table_pairs <<< #
def update_pair_col_rules(db_path: str, pair_name: str, rules_data: dict) -> None:
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

# >>> Load col_rules JSON for a pair from _table_pairs <<< #
def get_pair_col_rules(db_path: str, pair_name: str) -> dict:
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

# >>> Add col_include / col_exclude / col_rejected / col_type_overrides <<< #
def ensure_col_filter_columns(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    for col in ("col_include", "col_exclude", "col_rejected",
                "col_type_overrides", "col_type_overrides_right"):
        try:
            conn.execute(f"ALTER TABLE _table_pairs ADD COLUMN {col} TEXT")
        except sqlite3.OperationalError:
            pass
    conn.commit()
    conn.close()

# >>> Add length_avg / length_max / hash_sum columns to _col_stats if missing <<< #
def ensure_coarse_stat_columns(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    for col in ("length_avg", "length_max", "hash_sum"):
        try:
            conn.execute(f"ALTER TABLE _col_stats ADD COLUMN {col} TEXT")
        except sqlite3.OperationalError:
            pass
    conn.commit()
    conn.close()

# >>> Save include / exclude glob pattern lists for a pair <<< #
def save_pair_col_filter(db_path: str, pair_name: str,
                         include: Optional[list] = None,
                         exclude: Optional[list] = None) -> None:
    import json
    ensure_col_filter_columns(db_path)

    include_json = json.dumps(list(include)) if include else None
    exclude_json = json.dumps(list(exclude)) if exclude else None

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE _table_pairs SET col_include = ?, col_exclude = ? WHERE pair_name = ?",
        (include_json, exclude_json, pair_name),
    )
    if cursor.rowcount == 0:
        conn.close()
        raise ValueError(f"Pair '{pair_name}' not found in database")
    conn.commit()
    conn.close()

# >>> Load {include: [...], exclude: [...]} for a pair from _table_pairs <<< #
def get_pair_col_filter(db_path: str, pair_name: str) -> dict:
    import json
    ensure_col_filter_columns(db_path)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT col_include, col_exclude FROM _table_pairs WHERE pair_name = ?",
        (pair_name,),
    )
    row = cursor.fetchone()
    conn.close()

    if not row:
        return {"include": [], "exclude": []}
    inc = json.loads(row[0]) if row[0] else []
    exc = json.loads(row[1]) if row[1] else []
    return {"include": inc, "exclude": exc}

# >>> Load the user-rejected (left, right) mapping pairs for `pair_name` <<< #
def get_pair_col_rejected(db_path: str, pair_name: str) -> list:
    import json
    ensure_col_filter_columns(db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT col_rejected FROM _table_pairs WHERE pair_name = ?", (pair_name,))
    row = cursor.fetchone()
    conn.close()
    if not row or not row[0]:
        return []
    try:
        return json.loads(row[0])
    except (json.JSONDecodeError, TypeError):
        return []

# >>> Replace the col_rejected list for `pair_name` <<< #
def save_pair_col_rejected(db_path: str, pair_name: str, rejected: list) -> None:
    import json
    ensure_col_filter_columns(db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    payload = json.dumps(rejected) if rejected else None
    cursor.execute(
        "UPDATE _table_pairs SET col_rejected = ? WHERE pair_name = ?",
        (payload, pair_name))
    conn.commit()
    conn.close()

# >>> Load {left_col: dtype_override} for `pair_name`. Empty when none <<< #
def get_pair_col_type_overrides(db_path: str, pair_name: str) -> dict:
    import json
    ensure_col_filter_columns(db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT col_type_overrides FROM _table_pairs WHERE pair_name = ?",
        (pair_name,))
    row = cursor.fetchone()
    conn.close()
    if not row or not row[0]:
        return {}
    try:
        return json.loads(row[0])
    except (json.JSONDecodeError, TypeError):
        return {}

# >>> Replace the per-column type overrides for `pair_name`. Empty clears <<< #
def save_pair_col_type_overrides(db_path: str, pair_name: str, overrides: dict) -> None:
    import json
    ensure_col_filter_columns(db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    payload = json.dumps(overrides) if overrides else None
    cursor.execute(
        "UPDATE _table_pairs SET col_type_overrides = ? WHERE pair_name = ?",
        (payload, pair_name))
    conn.commit()
    conn.close()

# >>> Load right-side column type overrides for `pair_name` <<< #
def get_pair_col_type_overrides_right(db_path: str, pair_name: str) -> dict:
    import json
    ensure_col_filter_columns(db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT col_type_overrides_right FROM _table_pairs WHERE pair_name = ?",
        (pair_name,))
    row = cursor.fetchone()
    conn.close()
    if not row or not row[0]:
        return {}
    try:
        return json.loads(row[0])
    except (json.JSONDecodeError, TypeError):
        return {}

# >>> Replace the right-side per-column type overrides. Empty clears <<< #
def save_pair_col_type_overrides_right(db_path: str, pair_name: str, overrides: dict) -> None:
    import json
    ensure_col_filter_columns(db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    payload = json.dumps(overrides) if overrides else None
    cursor.execute(
        "UPDATE _table_pairs SET col_type_overrides_right = ? WHERE pair_name = ?",
        (payload, pair_name))
    conn.commit()
    conn.close()

# >>> Get where_map (row comparison results) for a pair from _row_comparison <<< #
def get_pair_where_map_from_db(db_path: str, pair_name: str) -> Optional[Dict[str, List[str]]]:
    comp = get_row_comparison(db_path, pair_name)
    if not comp:
        return None

    matching = comp.get('matching_dates', [])
    excluded = comp.get('excluded_dates', [])

    return {
        'matching_dates': matching,
        'left_only_dates': [],
        'right_only_dates': excluded,
    }

# >>> Sync unified config to database (save col_map and where_map) <<< #
def sync_config_to_db(db_path: str, config: Dict[str, Any]) -> None:
    from .platforms.base import qualified_name

    for pair_name, pair_config in config.get("pairs", {}).items():
        if pair_config.get("skip"):
            continue
        col_map = pair_config.get("col_map", {})
        left = pair_config.get("left", {})
        right = pair_config.get("right", {})
        if col_map and left.get("name") and right.get("name"):
            register_table_pair(
                db_path, pair_name,
                qualified_name(left), qualified_name(right),
                source_left=left.get("source"), source_right=right.get("source"),
                col_mappings=col_map,
            )

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    for pair_name, pair_config in config.get("pairs", {}).items():
        if pair_config.get("skip"):
            continue

        where_map = pair_config.get("where_map", {})
        metadata = pair_config.get("metadata", {})

        query_time = metadata.get("query_time")
        if query_time == "—":
            query_time = None

        if where_map and ("left" in where_map or "right" in where_map):
            where_left = where_map.get("left", "")
            where_right = where_map.get("right", "")

            try:
                cursor.execute("ALTER TABLE _row_comparison ADD COLUMN where_left TEXT")
            except sqlite3.OperationalError:
                pass
            try:
                cursor.execute("ALTER TABLE _row_comparison ADD COLUMN where_right TEXT")
            except sqlite3.OperationalError:
                pass

            cursor.execute("""
                UPDATE _row_comparison
                SET where_left = ?, where_right = ?, query_time = ?
                WHERE pair_name = ?
            """, (where_left, where_right, query_time, pair_name))

    conn.commit()
    conn.close()

# >>> Sync database state to unified config (load col_map and where_map) <<< #
def sync_db_to_config(db_path: str, config: Dict[str, Any]) -> None:
    for pair_name in config.get("pairs", {}):
        col_map = get_pair_col_map_from_db(db_path, pair_name)
        if col_map:
            config["pairs"][pair_name]["col_map"] = col_map

        comp = get_row_comparison(db_path, pair_name)
        if comp:
            matching = comp.get('matching_dates', [])
            excluded = comp.get('excluded_dates', [])

            config["pairs"][pair_name]["where_map"] = {
                'left': f"— ({len(matching)} matching dates, {len(excluded)} excluded)",
                'right': f"— ({len(matching)} matching dates, {len(excluded)} excluded)",
            }

            if "metadata" not in config["pairs"][pair_name]:
                config["pairs"][pair_name]["metadata"] = {}
            if comp.get('query_time'):
                config["pairs"][pair_name]["metadata"]["query_time"] = comp['query_time']

# >>> Insert a playground history row. Returns the new row id <<< #
def insert_playground_run(db_path: str, *, engine: str, conn: Optional[str],
                          sql: str, elapsed_sec: Optional[float],
                          n_rows: Optional[int], status: str,
                          error_msg: Optional[str] = None,
                          note: str = '') -> int:
    from datetime import timezone
    ts = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    with sqlite3.connect(db_path) as c:
        cur = c.execute(
            """INSERT INTO _playground_history
               (engine, conn, sql, elapsed_sec, n_rows, status, error_msg, note, ts_utc)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (engine, conn, sql, elapsed_sec, n_rows, status, error_msg, note, ts),
        )
        return cur.lastrowid

# >>> Return up to `limit` most-recent playground runs (newest first) <<< #
def list_playground_runs(db_path: str, limit: int = 50) -> List[Dict]:
    with sqlite3.connect(db_path) as c:
        c.row_factory = sqlite3.Row
        rows = c.execute(
            """SELECT id, engine, conn, sql, elapsed_sec, n_rows, status,
                      error_msg, note, ts_utc
               FROM _playground_history
               ORDER BY id DESC
               LIMIT ?""",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]

# >>> Update arbitrary columns on a playground history row <<< #
def update_playground_run(db_path: str, run_id: int, **fields) -> bool:
    allowed = {"status", "elapsed_sec", "n_rows", "error_msg"}
    sets = {k: v for k, v in fields.items() if k in allowed}
    if not sets:
        return False
    with sqlite3.connect(db_path) as c:
        cur = c.execute(
            "UPDATE _playground_history "
            f"SET {', '.join(f'{k} = ?' for k in sets)} "
            "WHERE id = ?",
            (*sets.values(), run_id),
        )
        return cur.rowcount > 0

# >>> Update the note column on a single run. Returns True if a row changed <<< #
def update_playground_note(db_path: str, run_id: int, note: str) -> bool:
    with sqlite3.connect(db_path) as c:
        cur = c.execute(
            "UPDATE _playground_history SET note = ? WHERE id = ?",
            (note, run_id),
        )
        return cur.rowcount > 0

# >>> Delete one run. Returns True if a row was removed <<< #
def delete_playground_run(db_path: str, run_id: int) -> bool:
    with sqlite3.connect(db_path) as c:
        cur = c.execute(
            "DELETE FROM _playground_history WHERE id = ?",
            (run_id,),
        )
        return cur.rowcount > 0

def clear_playground_history(db_path: str) -> int:
    # >>> wipe all rows from _playground_history; returns count deleted <<< #
    with sqlite3.connect(db_path) as c:
        cur = c.execute("DELETE FROM _playground_history")
        return cur.rowcount

# >>> read the stored compare-csv run signature for a pair (None if unknown) <<< #
def get_compare_csv_sig(db_path: str, pair_name: str) -> Optional[str]:
    with sqlite3.connect(db_path) as c:
        c.execute("CREATE TABLE IF NOT EXISTS _compare_csv_state "
                  "(pair_name TEXT PRIMARY KEY, signature TEXT, created_at TEXT)")
        row = c.execute("SELECT signature FROM _compare_csv_state WHERE pair_name = ?",
                        (pair_name,)).fetchone()
    return row[0] if row else None

# >>> persist the compare-csv run signature for a pair <<< #
def set_compare_csv_sig(db_path: str, pair_name: str, signature: str) -> None:
    from datetime import datetime
    with sqlite3.connect(db_path) as c:
        c.execute("CREATE TABLE IF NOT EXISTS _compare_csv_state "
                  "(pair_name TEXT PRIMARY KEY, signature TEXT, created_at TEXT)")
        c.execute("INSERT OR REPLACE INTO _compare_csv_state "
                  "(pair_name, signature, created_at) VALUES (?, ?, ?)",
                  (pair_name, signature, datetime.now().isoformat()))

# >>> drop _col_stats rows for the given source tables (clean slate on refresh) <<< #
def delete_col_stats_for_tables(db_path: str, tables) -> int:
    n = 0
    with sqlite3.connect(db_path) as c:
        for t in tables:
            n += c.execute("DELETE FROM _col_stats WHERE source_table = ?", (t,)).rowcount
    return n
