#!/usr/bin/env python3

import argparse
import json
import os
import re
import sys

from .db import (
    init_database,
    get_row_counts,
    get_metadata,
    register_table_pair,
    insert_column_meta,
    get_column_meta,
    save_row_comparison,
    get_row_comparison,
    patch_metadata,
    save_col_comparison,
    sync_config_to_db,
)
from .loader import load_row_counts, load_precomputed_col_stats
from .compare import (
    compare_row_counts,
    compare_column_stats,
    match_columns_from_dicts,
    apply_column_rules,
    _has_col_differences,
)
from .config import (
    load_unified_config,
    save_unified_config,
    get_all_tables_from_unified,
    set_pair_where_map,
    set_pair_col_map,
    ensure_pair_defaults,
    mark_pair_skipped,
    add_ignored_rows,
    get_ignored_columns,
    get_col_type_overrides,
)
from .platforms.base import qualified_name, is_numeric_type
from .interact import prompt_skip_pair, prompt_ignore_items, save_and_pause

# >>> Load and return (config, config_path) <<< #
def _load_config(config_path):
    if not os.path.exists(config_path):
        print(f"Error: Config file not found: {config_path}")
        sys.exit(1)
    return load_unified_config(config_path)

# >>> parse comma-separated "n_unique=on,hash_sum=off" → {'n_unique': True, ...} <<< #
def _parse_stats_override(spec):
    if not spec:
        return None
    if isinstance(spec, dict):
        return {str(k).strip().lower(): bool(v) for k, v in spec.items()} or None
    truthy = {'1', 'on', 'true', 'yes', 'y', 't'}
    falsy = {'0', 'off', 'false', 'no', 'n', 'f'}
    out = {}
    for part in spec.split(','):
        part = part.strip()
        if not part:
            continue
        if '=' not in part:
            print(f"Error: --stats-override item '{part}' missing '='")
            sys.exit(1)
        k, v = part.split('=', 1)
        k = k.strip().lower()
        v = v.strip().lower()
        if v in truthy:
            out[k] = True
        elif v in falsy:
            out[k] = False
        else:
            print(f"Error: --stats-override value '{v}' is not on/off")
            sys.exit(1)
    return out or None

# >>> Exit if db doesn't exist <<< #
def _require_db(db_path):
    if not os.path.exists(db_path):
        print(f"Error: Database not found: {db_path}")
        print(f"Run: dtrack init {db_path}")
        sys.exit(1)

# >>> Initialize a new dtrack database <<< #
def cmd_init(args):
    db_path = args.project_db
    if args.refresh or getattr(args, "migrate", False):
        if not os.path.exists(db_path):
            print(f"Error: Database not found: {db_path}")
            sys.exit(1)
        from .db import refresh_database, ensure_col_filter_columns
        actions = refresh_database(db_path)
        ensure_col_filter_columns(db_path)
        verb = "Migrated" if getattr(args, "migrate", False) else "Refreshed"
        print(f"{verb} database: {db_path}")
        for table, action in sorted(actions.items()):
            print(f"  {table}: {action}")
        return

    if os.path.exists(db_path):
        if not args.force:
            print(f"Error: Database already exists: {db_path}")
            print("Use --force to overwrite")
            sys.exit(1)
        os.remove(db_path)
    init_database(db_path)
    print(f"Initialized database: {db_path}")

    config_path = args.config or os.path.join(os.path.dirname(os.path.abspath(db_path)) or '.', 'pairs.json')
    if os.path.exists(config_path):
        print(f"Config exists, leaving as-is: {config_path}")
    else:
        import json as _json
        os.makedirs(os.path.dirname(os.path.abspath(config_path)) or '.', exist_ok=True)
        with open(config_path, 'w') as f:
            _json.dump({"pairs": {}, "metadata": {}}, f, indent=2)
        print(f"Initialized config: {config_path}")

# >>> Load row count data from CSV <<< #
def cmd_load_row(args):
    _require_db(args.project_db)
    config = _load_config(args.config)
    tables = get_all_tables_from_unified(config)

    for tbl in tables:
        qname = qualified_name(tbl)
        csv_path = os.path.join(args.folder, f"{qname}_row.csv")
        if not os.path.exists(csv_path):
            print(f"WARNING: {csv_path} not found, skipping {qname}")
            continue
        print(f"\n--- {qname} ---")
        load_row_counts(
            db_path=args.project_db,
            file_or_folder=csv_path,
            table_name=qname,
            mode=args.mode,
            source=tbl.get('source'),
            db_name=args.db,
            source_table=tbl.get('table', ''),
            date_col=args.date_var,
            date_var_override=tbl.get('date_col'),
        )
        rows = get_row_counts(args.project_db, qname)
        total = sum(count for _, count in rows)
        print(f"  Loaded {len(rows)} date buckets, total: {total:,}")

# >>> Load column statistics from CSV <<< #
def cmd_load_col(args):
    _require_db(args.project_db)
    config = _load_config(args.config)
    tables = get_all_tables_from_unified(config)

    for tbl in tables:
        qname = qualified_name(tbl)
        csv_path = os.path.join(args.folder, f"{qname}_col.csv")
        if not os.path.exists(csv_path):
            print(f"WARNING: {csv_path} not found, skipping {qname}")
            continue
        print(f"\n--- {qname} ---")
        table_vintage = tbl.get('vintage')
        if not table_vintage:
            meta = get_metadata(args.project_db, qname)
            table_vintage = (meta.get('vintage') or 'day') if meta else 'day'
        count = load_precomputed_col_stats(
            db_path=args.project_db,
            csv_path=csv_path,
            table_name=qname,
            mode=args.mode,
            source=tbl.get('source'),
            db_name=tbl.get('conn_macro'),
            vintage=table_vintage,
        )
        print(f"  Loaded {count} stat rows")

# >>> Load column metadata from CSV or live discovery <<< #
def cmd_load_columns(args):
    _require_db(args.project_db)
    config = _load_config(args.config)
    tables = get_all_tables_from_unified(config)

    import csv as csv_mod
    csv_files = getattr(args, 'csv_files', [])
    csv_lookup = {}
    if csv_files:
        for csv_path in csv_files:
            if not os.path.exists(csv_path):
                print(f"Error: CSV file not found: {csv_path}")
                continue
            with open(csv_path, 'r', newline='') as f:
                reader = csv_mod.DictReader(f)
                for row in reader:
                    row = {k.strip(): v for k, v in row.items()}
                    src = row.get('source') or row.get('SOURCE', '')
                    tbl = row.get('table') or row.get('TABLE', '')
                    col = row.get('column_name') or row.get('COLUMN_NAME', '')
                    dt = row.get('data_type') or row.get('DATA_TYPE', '')
                    if tbl and col:
                        key = (src.lower(), tbl.lower())
                        csv_lookup.setdefault(key, {})[col] = dt

    cols_dir = getattr(args, 'columns_dir', None)
    from .platforms.base import resolve_config_columns
    columns_file_qnames = set()

    for tbl in tables:
        source = tbl.get('source', '')
        qname = qualified_name(tbl)
        raw_table = tbl.get('table', '') or ''
        name = tbl.get('name', '')

        csv_key = (source.lower(), raw_table.lower())
        csv_key_name = (source.lower(), name.lower())
        matched_key = csv_key if csv_key in csv_lookup else (csv_key_name if csv_key_name in csv_lookup else None)

        if matched_key:
            columns = csv_lookup[matched_key]
            count = insert_column_meta(args.project_db, qname, columns, source=source)
            print(f"{qname}: loaded {count} columns from CSV")
            continue

        cfg_cols = resolve_config_columns(tbl)
        if cfg_cols:
            count = insert_column_meta(args.project_db, qname, cfg_cols, source=source)
            origin = 'columns_file' if tbl.get('columns_file') else 'inline columns'
            print(f"{qname}: loaded {count} columns from config ({origin})")
            if tbl.get('columns_file'):
                columns_file_qnames.add(qname)
            continue

        existing = _read_sidecar_columns(cols_dir, qname)
        if existing:
            count = insert_column_meta(args.project_db, qname, existing, source=source)
            print(f"{qname}: loaded {count} columns from existing "
                  f"{qname}_columns.csv (discovery skipped)")
            continue

        if tbl.get('processed'):
            columns = _read_sidecar_columns(cols_dir, qname)
            if columns:
                count = insert_column_meta(args.project_db, qname, columns, source=source)
                print(f"{qname}: loaded {count} columns from {qname}_columns.csv "
                      f"(processed/CTE — discovery skipped)")
            else:
                print(f"WARNING: {qname}: 'processed' CTE — automatic discovery skipped. "
                      f"Provide {qname}_columns.csv (column_name,data_type) in "
                      f"{cols_dir or 'the columns dir (--columns-dir)'}")
            continue

        if source.lower() == 'csv':
            from .platforms.csv_duckdb import discover_csv_columns
            path = tbl.get('conn_macro', '')
            if not path or not os.path.exists(path):
                print(f"{qname}: conn_macro (csv path) missing or not found ({path!r}); skipping")
                continue
            columns = discover_csv_columns(path)
            if not columns:
                print(f"{qname}: no columns discovered from {path}")
                continue
            count = insert_column_meta(args.project_db, qname, columns, source=source)
            print(f"{qname}: discovered {count} columns from {path}")
            continue

        conn_macro = tbl.get('conn_macro', '')
        print(f"{qname}: discovering columns from {source} ({conn_macro}: {raw_table})")
        _load_columns_entry(args.project_db, qname, raw_table, source, conn_macro)

    out_dir = cols_dir
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
        for tbl in tables:
            qname = qualified_name(tbl)
            if qname in columns_file_qnames:
                print(f"{qname}: columns_file defined; skipping {qname}_columns.csv")
                continue
            meta = sorted(get_column_meta(args.project_db, qname) or [],
                          key=lambda m: m['column_name'])
            if not meta:
                continue
            col_csv = os.path.join(out_dir, f"{qname}_columns.csv")
            if os.path.exists(col_csv):
                print(f"Kept existing column metadata (not overwritten): {col_csv}")
                continue
            with open(col_csv, 'w', newline='') as f:
                w = csv_mod.writer(f)
                w.writerow(['COLUMN_NAME', 'DATA_TYPE'])
                for m in meta:
                    w.writerow([m['column_name'], m.get('data_type', '')])
            print(f"Wrote column metadata: {col_csv}")

# >>> read a user-supplied {qname}_columns.csv (column_name/variable -> data_type) <<< #
def _read_sidecar_columns(columns_dir, qname):
    from .loader import read_columns_sidecar
    if not columns_dir:
        return None
    cols = read_columns_sidecar(os.path.join(columns_dir, f"{qname}_columns.csv"))
    return cols or None

# >>> Discover and load columns for one table entry <<< #
def _load_columns_entry(project_db, store_name, raw_table, source, conn_macro):
    if conn_macro and source.lower() == 'aws':
        from .platforms.athena import _discover_columns_athena, athena_connect, aws_creds_renew
        print(f"Connecting to Athena ({conn_macro})...")
        aws_creds_renew()
        conn = athena_connect(data_base=conn_macro)
        cursor = conn.cursor()
        print(f"Discovering columns for: {conn_macro}.{raw_table}")
        columns = _discover_columns_athena(cursor, conn_macro, raw_table)
        cursor.close()
        conn.close()

        if not columns:
            print(f"WARNING: No columns found for {conn_macro}.{raw_table}")
            return
    elif source.lower() == 'databricks':
        if conn_macro and '.' in conn_macro:
            from .platforms.databricks import databricks_connect, _discover_columns_databricks
            catalog, schema = conn_macro.split('.', 1)
            print(f"Connecting to Databricks ({conn_macro})...")
            conn = databricks_connect(conn_macro)
            cursor = conn.cursor()
            print(f"Discovering columns for: {conn_macro}.{raw_table}")
            columns = _discover_columns_databricks(cursor, catalog, schema, raw_table)
            cursor.close()
            conn.close()
        else:
            print(f"WARNING: {store_name}: databricks conn_macro must be catalog.schema, got '{conn_macro}'")
            return
        if not columns:
            print(f"WARNING: No columns found for {conn_macro}.{raw_table}")
            return
    elif source.lower() == 'hadoop':
        from .platforms.hadoop import _discover_columns_hadoop
        print(f"Discovering Hadoop columns for: {conn_macro}.{raw_table}")
        columns = _discover_columns_hadoop(conn_macro, raw_table)
        if not columns:
            print(f"WARNING: No columns found for {conn_macro}.{raw_table}")
            return
    elif conn_macro:
        from .db import oracle_connect, discover_columns_oracle
        print(f"Connecting to Oracle via '{conn_macro}'...")
        conn = oracle_connect(conn_macro)
        print(f"Discovering columns for: {raw_table}")
        columns = discover_columns_oracle(conn, raw_table)
        if conn is not None:
            conn.close()
        if not columns:
            print(f"WARNING: No columns found for {raw_table}")
            return
        print(f"Discovered {len(columns)} columns")
    else:
        print(f"WARNING: No conn_macro for {store_name}, skipping")
        return

    count = insert_column_meta(project_db, store_name, columns, source=source)
    print(f"Loaded {count} columns into _column_meta for table: {store_name}")

# >>> Run and print row count comparison for a single pair <<< #
def _print_row_comparison(db_path, table_left, table_right, source_left, source_right,
                          from_date=None, to_date=None, pair_name=None,
                          date_col_left=None, date_col_right=None):
    source_left = source_left or "left"
    source_right = source_right or "right"

    print(f"\nComparing row counts: {table_left} vs {table_right}")
    print("=" * 70)

    if not date_col_left or not date_col_right:
        meta_l = get_metadata(db_path, table_left)
        meta_r = get_metadata(db_path, table_right)
        date_col_left = date_col_left or ((meta_l.get('date_var') or '') if meta_l else '')
        date_col_right = date_col_right or ((meta_r.get('date_var') or '') if meta_r else '')
    if date_col_left or date_col_right:
        dc_str = date_col_left if date_col_left == date_col_right else f"{date_col_left} / {date_col_right}"
        print(f"Date column: {dc_str}")

    result = compare_row_counts(db_path, table_left, table_right, from_date=from_date, to_date=to_date)
    summary = result["summary"]

    if summary["count_left"] == 0 or summary["count_right"] == 0:
        empty_side = source_left if summary["count_left"] == 0 else source_right
        empty_tbl = table_left if summary["count_left"] == 0 else table_right
        print(f"ERROR: pair '{pair_name}': {empty_side} side ({empty_tbl}) has no "
              f"loaded rows -- skipping (run load-row for it first)")
        return None
    dr_left = summary['date_range_left']
    dr_right = summary['date_range_right']

    print(f"\n{source_left}: {dr_left[0]} to {dr_left[1]} | {summary['count_left']} dates | total: {summary['total_left']:,}")
    print(f"{source_right}: {dr_right[0]} to {dr_right[1]} | {summary['count_right']} dates | total: {summary['total_right']:,}")

    has_overlap = dr_left[0] and dr_right[0]
    overlap_start = overlap_end = None
    if has_overlap:
        overlap_start = max(dr_left[0], dr_right[0])
        overlap_end = min(dr_left[1], dr_right[1])
        if overlap_start <= overlap_end:
            print(f"\nOverlap range: {overlap_start} to {overlap_end}")
        else:
            has_overlap = False

    def _in_overlap(dt):
        return has_overlap and overlap_start <= dt <= overlap_end

    misload_left = [(dt, c) for dt, c in result['only_left'] if _in_overlap(dt)]
    misload_right = [(dt, c) for dt, c in result['only_right'] if _in_overlap(dt)]

    if result["mismatched"]:
        print(f"\nMismatched ({len(result['mismatched'])} dates):")
        for dt, cl, cr in result["mismatched"]:
            print(f"  {dt}: {source_left}={cl:,}, {source_right}={cr:,}, diff={cr-cl:+,}")
    else:
        print("\nMismatched: (none)")

    if misload_left or misload_right:
        print(f"\nMisload ({len(misload_left) + len(misload_right)} dates):")
        for dt, count in misload_left:
            print(f"  {dt}: {source_left}={count:,}, {source_right}=0")
        for dt, count in misload_right:
            print(f"  {dt}: {source_left}=0, {source_right}={count:,}")

    n_matching = len(result['matching'])
    print(f"\nMatching: {n_matching} dates")

    if pair_name:
        matching_dates = [dt for dt, _ in result['matching']]
        excluded_dates = [dt for dt, _, _ in result['mismatched']]
        excluded_dates += [dt for dt, _ in misload_left]
        excluded_dates += [dt for dt, _ in misload_right]
        save_row_comparison(db_path, pair_name, overlap_start, overlap_end, matching_dates, excluded_dates)

    return result

# >>> Build WHERE clause from matching and excluded dates <<< #
def _build_where_from_dates(table_cfg, matching_dates, excluded_dates, db_path=None):
    date_col = table_cfg.get('date_col', 'dt')

    date_dtype = ''
    date_format = ''
    if db_path:
        qn = qualified_name(table_cfg)
        col_meta = get_column_meta(db_path, qn)
        for cm in col_meta:
            if cm['column_name'].upper() == date_col.upper():
                date_dtype = (cm.get('data_type') or '').upper()
                break
        meta = get_metadata(db_path, qn)
        if meta:
            date_format = meta.get('date_format') or ''

    is_datetime = ('TIMESTAMP' in date_dtype or 'DATETIME' in date_dtype)
    source = (table_cfg.get('source') or '').lower()

    if is_datetime:
        if source == 'aws':
            col_expr = f"CAST({date_col} AS DATE)"
            def fmt_date(d):
                return f"'{d}'" if re.match(r'^\d{6}$', str(d)) else f"DATE '{str(d).split()[0]}'"
        else:
            col_expr = f"TRUNC({date_col})"
            def fmt_date(d):
                return f"'{d}'" if re.match(r'^\d{6}$', str(d)) else f"DATE '{str(d).split()[0]}'"
    else:
        col_expr = date_col
        from .date_utils import DateConverter
        dc = DateConverter()
        if date_format:
            dc.format_label = date_format

        def fmt_date(d):
            if re.match(r'^\d{6}$', str(d)):
                if date_dtype.startswith(('NUMBER', 'INTEGER', 'INT', 'BIGINT')):
                    return str(d)
                return f"'{d}'"
            original = dc.to_original(str(d)) if date_format else str(d)
            if date_dtype.startswith(('VARCHAR', 'CHAR', 'STRING', 'TEXT')):
                return f"'{original}'"
            elif date_dtype.startswith(('DATE', 'TIMESTAMP')):
                return f"DATE '{str(original).split()[0]}'"
            return f"'{original}'"

    parts = []
    orig_where = table_cfg.get('where', '').strip()
    if orig_where:
        parts.append(f"({orig_where})")
    if matching_dates:
        min_d, max_d = min(matching_dates), max(matching_dates)
        parts.append(f"{col_expr} >= {fmt_date(min_d)}")
        parts.append(f"{col_expr} <= {fmt_date(max_d)}")
        if excluded_dates:
            parts.append(f"{col_expr} NOT IN ({', '.join(fmt_date(d) for d in excluded_dates)})")
    return " AND ".join(parts) if parts else ""

# >>> Compare row counts between paired tables <<< #
def cmd_compare_row(args):
    _require_db(args.project_db)
    config = _load_config(args.config)
    yes = getattr(args, 'yes', False)
    html_path = getattr(args, 'html', None)

    if not html_path:
        output_dir = os.path.join(os.getcwd(), 'output')
        os.makedirs(output_dir, exist_ok=True)
        html_path = os.path.join(output_dir, 'compare_row.html')

    html_entries = []

    for pair_name, pair_config in config["pairs"].items():
        if pair_config.get("skip"):
            print(f"  Skipping pair: {pair_name}")
            continue
        left = pair_config["left"]
        right = pair_config["right"]
        table_left = qualified_name(left)
        table_right = qualified_name(right)
        source_left = left.get("source", "left")
        source_right = right.get("source", "right")
        col_map = pair_config.get("col_map", {})
        dc_left = left.get('date_col', '')
        dc_right = right.get('date_col', '')

        register_table_pair(
            args.project_db, pair_name, table_left, table_right,
            source_left=source_left, source_right=source_right,
            col_mappings=col_map if col_map else None,
        )
        if dc_left:
            patch_metadata(args.project_db, table_left, date_var=dc_left)
        if dc_right:
            patch_metadata(args.project_db, table_right, date_var=dc_right)

        result = _print_row_comparison(
            args.project_db, table_left, table_right,
            source_left, source_right,
            from_date=args.from_date, to_date=args.to_date,
            pair_name=pair_name,
            date_col_left=dc_left, date_col_right=dc_right,
        )
        if result is None:
            continue

        if not yes:
            n_mismatch = len(result['mismatched']) + len(result['only_left']) + len(result['only_right'])
            if n_mismatch > 0:
                action = prompt_skip_pair(pair_name, f"{n_mismatch} mismatched/missing dates")
                if action == 's':
                    mark_pair_skipped(config, pair_name)
                    save_unified_config(config, args.config)
                    print(f"  Pair '{pair_name}' marked as skipped")
                    continue
                elif action == 'i':
                    mismatch_dates = [dt for dt, _, _ in result['mismatched']]
                    ignored = prompt_ignore_items(mismatch_dates, "mismatched dates")
                    if ignored:
                        add_ignored_rows(config, pair_name, ignored)
                        save_unified_config(config, args.config)
                        print(f"  Added {len(ignored)} dates to ignore_rows")

        html_entries.append((pair_name, source_left, source_right, table_left, table_right, result, {}))

    for pair_name, sl, sr, tl, tr, result, _ in html_entries:
        comp = get_row_comparison(args.project_db, pair_name)
        if comp:
            matching = comp.get('matching_dates', [])
            excluded = comp.get('excluded_dates', [])
            left_cfg = config['pairs'][pair_name]['left']
            right_cfg = config['pairs'][pair_name]['right']
            where_left = _build_where_from_dates(left_cfg, matching, excluded, db_path=args.project_db)
            where_right = _build_where_from_dates(right_cfg, matching, excluded, db_path=args.project_db)
            set_pair_where_map(config, pair_name, where_left, where_right)
            ensure_pair_defaults(config, pair_name)

    if not yes:
        config = save_and_pause(config, args.config, [
            "Review where_map",
            "Add time_map values (check ./csv/_timing.csv)",
            "Add comment_map notes (shown as tooltips in HTML)",
        ])
    else:
        save_unified_config(config, args.config)

    sync_config_to_db(args.project_db, config)
    print(f"Synced to database: {args.project_db}")

    if html_path and html_entries:
        from .html_export import generate_row_count_html, create_row_count_table, wrap_html_document
        row_sections = []
        for pair_name, sl, sr, tl, tr, comp_result, wm in html_entries:
            meta_l = get_metadata(args.project_db, tl)
            meta_r = get_metadata(args.project_db, tr)
            pair_cfg = config['pairs'].get(pair_name, {})
            time_map = pair_cfg.get('time_map', {}).get('row', {})
            cm = pair_cfg.get('comment_map', {}).get('row', {})
            section = generate_row_count_html(
                pair_name, sl, sr, tl, tr, comp_result,
                metadata_left=meta_l, metadata_right=meta_r,
                where_map=wm, time_map=time_map,
                comment_left=cm.get('left', ''), comment_right=cm.get('right', ''),
                left_cfg=pair_cfg.get('left'), right_cfg=pair_cfg.get('right'),
                description=pair_cfg.get('description', ''),
            )
            row_sections.append(section)
        gm = config.get('metadata', {})
        table_html = create_row_count_table(row_sections)
        doc = wrap_html_document(
            getattr(args, 'title', None) or gm.get('title', 'Row Count Comparison'),
            [table_html],
            subtitle=getattr(args, 'subtitle', None) or gm.get('subtitle', ''),
        )
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(doc)
        print(f"HTML report: {html_path}")

        excel_path = os.path.splitext(html_path)[0] + '.xlsx'
        from .excel import build_row_comparison_workbook
        wb = build_row_comparison_workbook(
            args.project_db, args.config, args.from_date, args.to_date)
        os.makedirs(os.path.dirname(excel_path) or '.', exist_ok=True)
        wb.save(excel_path)
        print(f"Excel report: {excel_path}")

# >>> Compare column statistics between paired tables <<< #
def cmd_compare_col(args):
    _require_db(args.project_db)
    config = _load_config(args.config)
    html_path = getattr(args, 'html', None)

    if not html_path:
        output_dir = os.path.join(os.getcwd(), 'output')
        os.makedirs(output_dir, exist_ok=True)
        html_path = os.path.join(output_dir, 'compare_col.html')

    html_entries = []

    for pair_name, pair_config in config["pairs"].items():
        if pair_config.get("skip"):
            print(f"  Skipping pair: {pair_name}")
            continue
        left = pair_config["left"]
        right = pair_config["right"]
        table_left = qualified_name(left)
        table_right = qualified_name(right)
        source_left = left.get("source", "left")
        source_right = right.get("source", "right")
        col_mappings = pair_config.get("col_map", {})
        col_type_overrides = get_col_type_overrides(config, pair_name)
        ignore_columns = get_ignored_columns(config, pair_name)

        register_table_pair(
            args.project_db, pair_name, table_left, table_right,
            source_left=source_left, source_right=source_right,
            col_mappings=col_mappings if col_mappings else None,
        )

        dc_left = left.get('date_col', '')
        dc_right = right.get('date_col', '')
        if dc_left:
            patch_metadata(args.project_db, table_left, date_var=dc_left)
        if dc_right:
            patch_metadata(args.project_db, table_right, date_var=dc_right)

        print(f"\n{'='*70}")
        print(f"Comparing column stats: {pair_name}")
        print(f"  {source_left}: {table_left}")
        print(f"  {source_right}: {table_right}")
        print(f"{'='*70}")

        matched_dates = None
        if not args.no_date_filter:
            row_result = compare_row_counts(
                args.project_db, table_left, table_right,
                from_date=args.from_date, to_date=args.to_date,
            )
            matched_dates = {dt for dt, _ in row_result['matching']}
            if matched_dates:
                print(f"Filtering to {len(matched_dates)} matching dates")

        result = compare_column_stats(
            args.project_db, table_left, table_right,
            col_mappings=col_mappings,
            from_date=args.from_date, to_date=args.to_date,
            matched_dates=matched_dates,
            col_type_overrides=col_type_overrides,
        )

        if result:
            matched_cols = []
            diff_cols = []
            for col_name, comparisons in result.items():
                if col_name in ignore_columns:
                    continue
                if any(_has_col_differences(c) for c in comparisons):
                    diff_cols.append(col_name)
                else:
                    matched_cols.append(col_name)

            print(f"  {len(matched_cols)} match, {len(diff_cols)} diff"
                  + (f" ({', '.join(sorted(diff_cols)[:4])})" if diff_cols else ""))

            save_col_comparison(
                args.project_db, pair_name,
                columns_compared=list(result.keys()),
                matched_columns=matched_cols,
                diff_columns=diff_cols,
                comparison_details=result,
            )

            diff_map = {}
            for col_name, comparisons in result.items():
                for comp in comparisons:
                    if not _has_col_differences(comp):
                        continue
                    vlabel = comp.get('vintage_label', comp['dt'])
                    if vlabel not in diff_map:
                        diff_map[vlabel] = {}
                    col_diffs = {}
                    for stat in ('n_total', 'n_missing', 'n_unique'):
                        d = comp.get(f'{stat}_diff', 0)
                        if d != 0:
                            col_diffs[stat] = {'left': comp[f'{stat}_left'], 'right': comp[f'{stat}_right']}
                    if comp['col_type'] == 'numeric':
                        for stat in ('mean', 'std'):
                            d = comp.get(f'{stat}_diff')
                            if d is not None and abs(d) > 0.01:
                                col_diffs[stat] = {'left': comp.get(f'{stat}_left'), 'right': comp.get(f'{stat}_right')}
                    if col_diffs:
                        diff_map[vlabel][col_name] = col_diffs
            config['pairs'][pair_name]['diff_map'] = diff_map

            html_entries.append((pair_name, source_left, source_right, table_left, table_right, result, col_mappings))
        else:
            print("  No matching columns found")

    if html_path and html_entries:
        from .html_export import generate_column_stats_html, create_column_stats_table, wrap_html_document
        col_sections = []
        for pair_name, sl, sr, tl, tr, comp_result, cm in html_entries:
            meta_l = get_metadata(args.project_db, tl)
            meta_r = get_metadata(args.project_db, tr)
            pair_cfg = config['pairs'].get(pair_name, {})
            comment_map = pair_cfg.get('comment_map', {}).get('col', {})
            col_time_map = pair_cfg.get('time_map', {}).get('col', {})
            section = generate_column_stats_html(
                pair_name, sl, sr, tl, tr, comp_result, cm,
                metadata_left=meta_l, metadata_right=meta_r,
                time_map=col_time_map,
                comment_left=comment_map.get('left', ''),
                comment_right=comment_map.get('right', ''),
                left_cfg=pair_cfg.get('left'), right_cfg=pair_cfg.get('right'),
            )
            col_sections.append(section)

        vintage = getattr(args, 'vintage', 'day')
        table_html = create_column_stats_table(col_sections, vintage=vintage)
        gm = config.get('metadata', {})
        doc = wrap_html_document(
            getattr(args, 'title', None) or gm.get('col_title', 'Column Statistics Comparison'),
            [table_html],
            subtitle=getattr(args, 'subtitle', None) or gm.get('col_subtitle', ''),
        )
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(doc)
        print(f"HTML report: {html_path}")

        excel_path = getattr(args, 'excel', None) or os.path.splitext(html_path)[0] + '.xlsx'
        from .excel import build_col_comparison_workbook
        wb = build_col_comparison_workbook(
            args.project_db, args.config, args.from_date, args.to_date)
        os.makedirs(os.path.dirname(excel_path) or '.', exist_ok=True)
        wb.save(excel_path)
        print(f"Excel report: {excel_path}")

    try:
        save_unified_config(config, args.config)
        print(f"Config saved: {args.config}")
    except Exception as e:
        print(f"Warning: Could not save config: {e}")

# >>> Extract data from AWS Athena <<< #
def cmd_gen_aws(args):
    from .platforms.athena import extract_aws
    if not os.path.exists(args.config):
        print(f"Error: Config file not found: {args.config}")
        sys.exit(1)
    types = [args.type] if args.type != "both" else ["row", "col"]
    db_path = getattr(args, 'db_path', None)
    vintage = getattr(args, 'vintage', None)
    force = getattr(args, 'force', False)
    from_date = getattr(args, 'from_date', None)
    to_date = getattr(args, 'to_date', None)
    preset = getattr(args, 'preset', None)
    stats_override = _parse_stats_override(getattr(args, 'stats_override', None))
    extract_aws(args.config, args.outdir, types=types, max_workers=args.workers,
                db_path=db_path, vintage=vintage, force=force,
                from_date=from_date, to_date=to_date,
                preset=preset, stats_override=stats_override)

# >>> Extract data from Databricks SQL (Python-direct via databricks-sql) <<< #
def cmd_gen_databricks(args):
    from .platforms.databricks import extract_databricks
    if not os.path.exists(args.config):
        print(f"Error: Config file not found: {args.config}")
        sys.exit(1)
    types = [args.type] if args.type != "both" else ["row", "col"]
    db_path = getattr(args, 'db_path', None)
    vintage = getattr(args, 'vintage', None)
    force = getattr(args, 'force', False)
    from_date = getattr(args, 'from_date', None)
    to_date = getattr(args, 'to_date', None)
    preset = getattr(args, 'preset', None)
    stats_override = _parse_stats_override(getattr(args, 'stats_override', None))
    extract_databricks(args.config, args.outdir, types=types, max_workers=args.workers,
                       db_path=db_path, vintage=vintage, force=force,
                       from_date=from_date, to_date=to_date,
                       preset=preset, stats_override=stats_override)

# >>> Extract data from Oracle via oracledb (Python-direct) <<< #
def cmd_gen_oracle(args):
    from .platforms.oracle import extract_oracle
    if not os.path.exists(args.config):
        print(f"Error: Config file not found: {args.config}")
        sys.exit(1)
    types = [args.type] if args.type != "both" else ["row", "col"]
    db_path = getattr(args, 'db_path', None)
    vintage = getattr(args, 'vintage', None)
    force = getattr(args, 'force', False)
    from_date = getattr(args, 'from_date', None)
    to_date = getattr(args, 'to_date', None)
    preset = getattr(args, 'preset', None)
    stats_override = _parse_stats_override(getattr(args, 'stats_override', None))
    extract_oracle(args.config, args.outdir, types=types, max_workers=args.workers,
                   db_path=db_path, vintage=vintage, force=force,
                   from_date=from_date, to_date=to_date,
                   preset=preset, stats_override=stats_override)

# >>> Match columns between paired tables <<< #
def cmd_match_columns(args):
    _require_db(args.project_db)
    config = _load_config(args.config)

    file_maps = {}
    map_csv = getattr(args, 'map_csv', None)
    if map_csv and not os.path.exists(map_csv):
        print(f"--map-csv not found, skipping: {map_csv}")
    elif map_csv:
        import csv as csv_mod
        with open(map_csv, newline='') as f:
            for row in csv_mod.DictReader(f):
                row = {k.strip().lower(): (v or '').strip() for k, v in row.items()}
                p = row.get('pair') or row.get('pair_name')
                lcol = row.get('left') or row.get('left_column')
                rcol = row.get('right') or row.get('right_column')
                if p and lcol and rcol:
                    file_maps.setdefault(p, {})[lcol] = rcol

    matched_columns = {}
    type_violations = []
    for pair_name, pair_config in config["pairs"].items():
        left = pair_config['left']
        right = pair_config['right']
        left_table = qualified_name(left)
        right_table = qualified_name(right)
        left_src = left.get('source', 'left')
        right_src = right.get('source', 'right')

        print(f"=== Pair: {pair_name} ===")
        print(f"  Left:  {left_table} ({left_src})")
        print(f"  Right: {right_table} ({right_src})")
        print()

        left_meta = get_column_meta(args.project_db, left_table)
        right_meta = get_column_meta(args.project_db, right_table)
        if not left_meta:
            print(f"  No column metadata for: {left_table}")
            continue
        if not right_meta:
            print(f"  No column metadata for: {right_table}")
            continue

        left_cols = {m['column_name']: m['data_type'] or '' for m in left_meta}
        right_cols = {m['column_name']: m['data_type'] or '' for m in right_meta}

        replace_mode = getattr(args, 'mode', 'merge') == 'replace'
        existing_col_map = {} if replace_mode else pair_config.get('col_map', {})

        if existing_col_map:
            matched = dict(existing_col_map)
            mapped_right = set(existing_col_map.values())
            left_only = [{'name': c, 'type': t} for c, t in left_cols.items()
                         if c not in existing_col_map]
            right_only = [{'name': c, 'type': t} for c, t in right_cols.items()
                          if c not in mapped_right]
            print(f"  col_map already defined ({len(existing_col_map)}); skipping auto-match")
        else:
            result = match_columns_from_dicts(left_cols, right_cols, left_table, right_table)
            matched = dict(result.get('matched', {}))
            left_only = result.get(f'{left_table}_only', [])
            right_only = result.get(f'{right_table}_only', [])

        rule_list = (pair_config.get('col_rules') or {}).get('rules') or []
        if rule_list:
            rule_maps, _ = apply_column_rules(
                rule_list, [c['name'] for c in left_only], [c['name'] for c in right_only])
            if rule_maps:
                matched.update(rule_maps)
                rmatched_right = set(rule_maps.values())
                left_only = [c for c in left_only if c['name'] not in rule_maps]
                right_only = [c for c in right_only if c['name'] not in rmatched_right]
                print(f"Rule-matched: {len(rule_maps)} columns")

        pair_file_map = file_maps.get(pair_name, {})
        if pair_file_map:
            matched.update(pair_file_map)
            mapped_r = set(pair_file_map.values())
            left_only = [c for c in left_only if c['name'] not in pair_file_map]
            right_only = [c for c in right_only if c['name'] not in mapped_r]
            print(f"File-mapped: {len(pair_file_map)} columns (--map-csv)")

        auto_count = len(matched) - len(existing_col_map)
        print(f"Auto-matched: {auto_count} columns")
        if existing_col_map:
            print(f"Existing mappings: {len(existing_col_map)}")

        if left_only or right_only:
            print(f"  {len(left_only)} unmatched left + {len(right_only)} unmatched right "
                  f"column(s) left unmapped (map them in the web col_mapping page or via --map-csv)")

        for lcol, rcol in matched.items():
            lt = left_cols.get(lcol, '')
            rt = right_cols.get(rcol, '')
            if lt and rt and is_numeric_type(lt) != is_numeric_type(rt):
                type_violations.append((pair_name, lcol, lt, rcol, rt))

        matched_columns[pair_name] = matched

    if type_violations:
        print("\nERROR: numeric <-> categorical/date column mappings detected "
              "(stats are not comparable across type families):")
        for pn, lc, lt, rc, rt in type_violations:
            print(f"  [{pn}] {lc} ({lt}) <-> {rc} ({rt})")
        print("Fix the col_map or the column types, then re-run. No changes saved.")
        sys.exit(1)

    if matched_columns:
        for pair_name, m in matched_columns.items():
            set_pair_col_map(config, pair_name, m)
            print(f"  {pair_name}: {len(m)} column mappings saved")
        save_unified_config(config, args.config)
        print(f"Saved to: {args.config}")
        sync_config_to_db(args.project_db, config)
        print(f"Synced to database: {args.project_db}")

# >>> classify config tables by source and run each platform's extractor <<< #
def cmd_extract(args):
    from .extract import extract_all
    from .db import init_database
    if not os.path.exists(args.project_db):
        init_database(args.project_db)
        print(f"Initialized database: {args.project_db}")
    if args.type in ("col", "both"):
        _log_strict_summary(args.config)
    extract_all(
        config_path=args.config,
        outdir=getattr(args, 'outdir', None),
        csv_outdir=getattr(args, 'csv_outdir', None),
        types=[args.type] if args.type != "both" else ["row", "col"],
        max_workers=getattr(args, 'workers', None),
        db_path=args.project_db,
        vintage=getattr(args, 'vintage', None),
        force=getattr(args, 'force', False),
        from_date=getattr(args, 'from_date', None),
        to_date=getattr(args, 'to_date', None),
        preset=getattr(args, 'preset', None),
        stats_override=_parse_stats_override(getattr(args, 'stats_override', None)),
    )

# >>> sample N rows by key from both sides; dump value + backend hash16 to Excel <<< #
def cmd_sample_hash(args):
    _require_db(args.project_db)
    from .compare import sample_hash
    sample_hash(
        project_db=args.project_db,
        config_path=args.config,
        key=args.key,
        pair=getattr(args, 'pair', None),
        n=getattr(args, 'sample_n', 20),
        out=getattr(args, 'out', None),
    )

# >>> parse --keys / --keys-file into an ordered unique list of key values <<< #
def _col_sample_key_values(args):
    vals = []
    if getattr(args, 'keys', None):
        vals += args.keys.split(',')
    if getattr(args, 'keys_file', None):
        with open(args.keys_file) as f:
            vals += f.read().splitlines()
    seen, out = set(), []
    for v in vals:
        v = v.strip()
        if v and v not in seen:
            seen.add(v)
            out.append(v)
    return out

# >>> coerce a CLI key string to int/float when numeric so SQL literals stay unquoted <<< #
def _col_sample_coerce(v):
    try:
        return int(v)
    except ValueError:
        pass
    try:
        return float(v)
    except ValueError:
        return v

_COLSAMPLE_OPEN = "/*COLSAMPLE_START*/"
_COLSAMPLE_CLOSE = "/*COLSAMPLE_END*/"

# >>> strip a prior col-sample key filter from a where clause so re-runs don't stack <<< #
def _col_sample_strip(where):
    if not where or _COLSAMPLE_OPEN not in where:
        return (where or "").strip()
    pat = re.escape(_COLSAMPLE_OPEN) + r".*?" + re.escape(_COLSAMPLE_CLOSE)
    w = re.sub(r"\s*AND\s*" + pat, "", where, flags=re.S)
    w = re.sub(pat + r"\s*AND\s*", "", w, flags=re.S)
    w = re.sub(pat, "", w, flags=re.S)
    return w.strip()

# >>> case-insensitive col_map lookup; fall back to the same name on the other side <<< #
def _col_sample_map_lookup(col_map, key):
    if key in col_map:
        return col_map[key]
    kl = key.lower()
    for k, v in col_map.items():
        if k.lower() == kl:
            return v
    return key

# >>> col-sample: deterministic-sample keys, persist key IN (...) into where_map, generate col SQL <<< #
def cmd_col_sample(args):
    _require_db(args.project_db)
    import random
    import tempfile
    from .extract import extract_all
    from .db import get_pair_col_map_from_db
    from .compare import sample_keys_hashed, _key_in_clause, _dialect
    from .platforms.base import quote_ident

    config = load_unified_config(args.config)
    pairs = config.get("pairs", {})
    if not pairs:
        print("No pairs in config")
        return

    if args.pair:
        if args.pair not in pairs:
            print(f"Pair not found: {args.pair}")
            sys.exit(1)
        targets = [args.pair]
    else:
        targets = [n for n, p in pairs.items() if not p.get("skip")]

    manual = _col_sample_key_values(args)
    n = getattr(args, 'sample_n', 20)
    key_cols = [k.strip() for k in str(args.key).split(",") if k.strip()]
    sampled = []

    for pname in targets:
        pcfg = pairs[pname]
        left = pcfg.get("left", {})
        right = pcfg.get("right", {})
        col_map = pcfg.get("col_map") or get_pair_col_map_from_db(args.project_db, pname) or {}
        pcfg["col_map"] = col_map
        right_keys = [_col_sample_map_lookup(col_map, k) for k in key_cols]
        ld = _dialect(left.get("source", ""))
        rd = _dialect(right.get("source", ""))

        wm = pcfg.setdefault("where_map", {})
        base_left = _col_sample_strip(wm.get("left", ""))
        base_right = _col_sample_strip(wm.get("right", ""))

        if manual:
            if len(key_cols) != 1:
                print(f"[col-sample] {pname}: --keys/--keys-file only support a "
                      f"single key column; got composite {key_cols}. Skipping.")
                continue
            key_tuples = [(_col_sample_coerce(v),) for v in manual]
        else:
            probe = dict(left)
            probe["where"] = base_left
            key_tuples = sample_keys_hashed(probe, key_cols, n)
        if not key_tuples:
            print(f"[col-sample] {pname}: no keys found; skipping")
            continue

        left_in = _key_in_clause([quote_ident(k, ld) for k in key_cols], key_tuples)
        right_in = _key_in_clause([quote_ident(rk, rd) for rk in right_keys], key_tuples)
        wm["left"] = (f"({base_left}) AND " if base_left else "") + \
            f"{_COLSAMPLE_OPEN}{left_in}{_COLSAMPLE_CLOSE}"
        wm["right"] = (f"({base_right}) AND " if base_right else "") + \
            f"{_COLSAMPLE_OPEN}{right_in}{_COLSAMPLE_CLOSE}"
        sampled.append(pname)
        print(f"[col-sample] {pname}: {len(key_tuples)} keys on "
              f"{'+'.join(key_cols)}/{'+'.join(right_keys)} -> where_map")

    if not sampled:
        print("[col-sample] nothing sampled; no SQL written")
        return

    save_unified_config(config, args.config)
    print(f"[col-sample] persisted key filter into {args.config} (where_map, AND-merged)")

    for pname in sampled:
        pcfg = pairs[pname]
        if args.cols:
            inc = [c.strip() for c in args.cols.split(",") if c.strip()]
        elif getattr(args, 'sample_cols', None):
            dcol = (pcfg.get("left", {}).get("date_col", "") or "").lower()
            _excl = {dcol} | {k.lower() for k in key_cols}
            pool = [c for c in (pcfg.get("col_map") or {}).keys()
                    if c.lower() not in _excl]
            inc = random.sample(pool, min(args.sample_cols, len(pool))) if pool else []
            print(f"[col-sample] {pname}: random {len(inc)} of {len(pool)} value cols -> {inc}")
        else:
            inc = None
        if inc is not None:
            pcfg["col_filter"] = {"include": inc, "exclude": []}

    temp_cfg = dict(config)
    temp_cfg["pairs"] = {p: pairs[p] for p in sampled}
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
    json.dump(temp_cfg, tmp)
    tmp.close()
    try:
        extract_all(
            config_path=tmp.name,
            outdir=getattr(args, 'outdir', None),
            types=['col'],
            db_path=args.project_db,
            vintage=getattr(args, 'vintage', None),
            from_date=getattr(args, 'from_date', None),
            to_date=getattr(args, 'to_date', None),
            preset=getattr(args, 'preset', None),
            stats_override=_parse_stats_override(getattr(args, 'stats_override', None)),
            gen_only=True,
        )
    finally:
        os.unlink(tmp.name)
    print("[col-sample] generated extract_col.<platform>.sql. "
          "Execute: dtrack run-sql --type col  (or `just force=1 e2e col-run`)")

# >>> compare-csv: per-vintage-window backend pull -> local DuckDB col stats -> upsert + compare <<< #
def cmd_compare_csv(args):
    if not os.path.exists(args.project_db):
        from .db import init_database
        init_database(args.project_db)
        print(f"Initialized database: {args.project_db}")
    from .compare_csv import run_compare_csv
    raw = getattr(args, 'sample', None)
    sample = None
    if raw is not None:
        try:
            sample = float(raw) if '.' in str(raw) else int(raw)
        except ValueError:
            print(f"--sample must be an integer or a fraction; got {raw!r}")
            sys.exit(1)
    run_compare_csv(
        project_db=args.project_db,
        config_path=args.config,
        pair=getattr(args, 'pair', None),
        vintage=getattr(args, 'vintage', None),
        from_date=getattr(args, 'from_date', None),
        to_date=getattr(args, 'to_date', None),
        preset=getattr(args, 'preset', None),
        stats_override=_parse_stats_override(getattr(args, 'stats_override', None)),
        keep_csv=getattr(args, 'keep_csv', False),
        resume=getattr(args, 'resume', False),
        sample=sample,
        key=getattr(args, 'key', None),
        data_dir=getattr(args, 'data_dir', None),
        workers=getattr(args, 'workers', None),
        force=getattr(args, 'force', False),
        verbose=getattr(args, 'verbose', False),
        clean=getattr(args, 'clean', None),
        strict=getattr(args, 'strict', False),
    )

# >>> sample-key: composite-key value+hash dump (--out/default) and/or col-sample gen (--gen-col) <<< #
def cmd_sample_key(args):
    do_gen = bool(getattr(args, 'gen_col', False))
    do_dump = bool(getattr(args, 'out', None)) or not do_gen
    if do_dump:
        cmd_sample_hash(args)
    if do_gen:
        cmd_col_sample(args)

# >>> execute already-generated extract_{row,col}.<platform>.sql via each platform's runner (mirrors web run-sql) <<< #
def cmd_run_sql(args):
    from .extract import run_sql_all
    from .db import init_database
    if not os.path.exists(args.project_db):
        init_database(args.project_db)
        print(f"Initialized database: {args.project_db}")
    run_sql_all(
        config_path=args.config,
        outdir=getattr(args, 'outdir', None),
        csv_outdir=getattr(args, 'csv_outdir', None),
        types=[args.type] if args.type != "both" else ["row", "col"],
        db_path=args.project_db,
        max_workers=getattr(args, 'workers', None),
        resume=getattr(args, 'resume', False),
    )

# >>> log per-pair STRICT mode (all | off | specific left columns) to the running log <<< #
def _log_strict_summary(config_path):
    import sys
    from .config import load_unified_config
    from .logmgr import get_logger
    try:
        config = load_unified_config(config_path)
    except Exception:
        return
    from .platforms.base import strict_env_on
    env_on = strict_env_on()
    log = get_logger()
    for pname, pcfg in (config.get("pairs") or {}).items():
        if pcfg.get("skip"):
            continue
        s = pcfg.get("strict")
        if isinstance(s, (list, tuple)):
            desc = f"left cols [{', '.join(map(str, s))}]"
        elif s is True:
            desc = "ALL columns"
        elif s is False:
            desc = "off (pair)"
        else:
            desc = "ALL columns (DTRACK_STRICT)" if env_on else "off"
        msg = f"STRICT [{pname}]: {desc}"
        print(f">>> {msg}", file=sys.stderr)
        log.info(msg)

# >>> write extract_{row,col} SQL/SAS for every platform WITHOUT running (review before extract) <<< #
def cmd_generate(args):
    from .extract import extract_all
    from .db import init_database
    if not os.path.exists(args.project_db):
        init_database(args.project_db)
        print(f"Initialized database: {args.project_db}")
    if args.type in ("col", "both"):
        _log_strict_summary(args.config)
    extract_all(
        config_path=args.config,
        outdir=getattr(args, 'outdir', None),
        csv_outdir=getattr(args, 'csv_outdir', None),
        types=[args.type] if args.type != "both" else ["row", "col"],
        db_path=args.project_db,
        vintage=getattr(args, 'vintage', None),
        from_date=getattr(args, 'from_date', None),
        to_date=getattr(args, 'to_date', None),
        preset=getattr(args, 'preset', None),
        stats_override=_parse_stats_override(getattr(args, 'stats_override', None)),
        gen_only=True,
    )

# >>> Start the dtrack web UI <<< #
def cmd_serve(args):
    _require_db(args.project_db)

    config_path = args.config
    if not config_path:
        import tempfile
        import json
        temp_config = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        json.dump({"pairs": {}, "metadata": {}}, temp_config)
        temp_config.close()
        config_path = temp_config.name
        print(f"No config provided, using temporary config: {config_path}")
    elif not os.path.exists(config_path):
        print(f"Error: Config file not found: {config_path}")
        sys.exit(1)

    from .web.app import serve
    serve(db_path=args.project_db, config_path=config_path,
          port=args.port, host=args.host)

# >>> Run a SQL query against the database <<< #
def cmd_query(args):
    import sqlite3
    _require_db(args.project_db)
    sql = args.sql.strip()
    allow_write = getattr(args, 'write', False)

    if not allow_write:
        first_word = sql.split()[0].upper() if sql else ''
        if first_word not in ('SELECT', 'PRAGMA', 'EXPLAIN', 'WITH'):
            print(f"Error: Only SELECT/PRAGMA/EXPLAIN/WITH queries allowed (got '{first_word}')")
            print("Hint: Use --write flag to enable write operations")
            sys.exit(1)

    conn = sqlite3.connect(args.project_db)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        rows = cursor.fetchall()
        conn.commit()
    except sqlite3.Error as e:
        print(f"SQL error: {e}")
        conn.close()
        sys.exit(1)

    if not rows:
        if allow_write and cursor.rowcount >= 0:
            print(f"Query executed ({cursor.rowcount} row(s) affected)")
        else:
            print("(no rows)")
        conn.close()
        return

    cols = rows[0].keys()
    col_widths = {c: len(c) for c in cols}
    for row in rows:
        for c in cols:
            col_widths[c] = min(max(col_widths[c], len(str(row[c]) if row[c] is not None else '')), 40)

    print("  ".join(f"{c:<{col_widths[c]}}" for c in cols))
    print("  ".join("-" * col_widths[c] for c in cols))
    for row in rows:
        print("  ".join(f"{str(row[c]) if row[c] is not None else '':<{col_widths[c]}}" for c in cols))
    print(f"\n({len(rows)} row{'s' if len(rows) != 1 else ''})")
    conn.close()

# >>> Print where env was loaded from and which Oracle/AWS/SAS settings resolve <<< #
def cmd_doctor(args):
    from .db import MACRO2SVC, _load_macro_overrides

    env_loaded = os.environ.get('DTRACK_ENV_FILE_LOADED', '<none found>')
    print(f"env file:  {env_loaded}")
    try:
        from dotenv import find_dotenv
        shadowed = []
        for fname in ('dtrack.conf', '.env', 'dtrack.env'):
            found = find_dotenv(usecwd=True, filename=fname)
            if found and os.path.abspath(found) != os.path.abspath(env_loaded):
                shadowed.append(found)
        if shadowed:
            print(f"  NOTE: only the file above is loaded; these are IGNORED: "
                  f"{', '.join(shadowed)}")
            print("        (put HADOOP_DSN/PCDS_USR/etc. in the loaded file, "
                  "or a shell-exported var will also override it)")
    except Exception:
        pass
    print()

    print("Oracle macros (MACRO2SVC + overrides):")
    overrides = _load_macro_overrides()
    merged = {**MACRO2SVC, **overrides}
    if not merged:
        print("  (none)")
    for name, svc in sorted(merged.items()):
        origin = "override" if name in overrides else "builtin"
        print(f"  {name:8s} -> {svc}  ({origin})")
    print()

    pcds = "set" if os.environ.get('PCDS_USR') else "MISSING"
    print(f"Oracle creds: PCDS_USR={pcds}")
    for name in sorted(merged):
        pwd = "set" if os.environ.get(f'{name}_pwd') else "MISSING"
        print(f"              {name}_pwd={pwd}")
    print()

    ldap = os.environ.get('LDAP_DSN')
    print(f"LDAP_DSN: {ldap if ldap else '<unset, will connect via direct service name>'}")
    if ldap and '{service}' not in ldap:
        print("  WARNING: LDAP_DSN does not contain '{service}' placeholder")
    print()

    print("AWS / Athena env:")
    for var in ('AWS_DEFAULT_REGION', 'AWS_S3_WORK_GROUP', 'AWS_S3_STAGING_DIR',
                'AWS_TOKEN_URL', 'AWS_ARN_URL', 'AWS_USR', 'AWS_HOST'):
        val = os.environ.get(var)
        print(f"  {var:24s} = {val if val else '<unset>'}")
    aws_pwd = "set" if os.environ.get('AWS_PWD') else 'MISSING'
    print(f"  {'AWS_PWD':24s} = {aws_pwd}")
    print()

    print("Hadoop / Hive (ODBC) env:")
    hdsn = os.environ.get('HADOOP_DSN')
    print(f"  {'HADOOP_DSN':24s} = {hdsn if hdsn else '<unset, default Hive_Prod>'}")
    for var in ('KRB5CCNAME', 'KRB5_CONFIG', 'KRB5_KTNAME',
                'SSL_CERT_FILE', 'HTTPS_PROXY'):
        val = os.environ.get(var)
        print(f"  {var:24s} = {val if val else '<unset>'}")

# >>> Main CLI entry point <<< #
def main():
    parser = argparse.ArgumentParser(
        description="dtrack - Data tracking CLI for row counts and column statistics",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--env', dest='env_file', help='Path to .env file')
    parser.add_argument('--debug', choices=['ipdb', 'attach'], default=None)

    sub = parser.add_subparsers(dest='command', help='Available commands')

    p = sub.add_parser('init', help='Initialize database (and starter pairs.json)')
    p.add_argument('project_db')
    p.add_argument('--config', default=None,
                   help='Path for the starter config JSON (default: '
                        'pairs.json next to the db). Skipped if file exists.')
    p.add_argument('--force', action='store_true')
    p.add_argument('--refresh', action='store_true',
                   help='Rebuild schema; rename-create-copy preserves data in '
                        'columns that exist in both old and new schemas.')
    p.add_argument('--migrate', action='store_true',
                   help='Alias of --refresh: idempotent data-preserving '
                        'schema upgrade (adds new columns introduced by a '
                        'newer dtrack, keeps existing data).')

    p = sub.add_parser('load-row', help='Load row counts from CSV folder')
    p.add_argument('project_db')
    p.add_argument('folder', help='Folder with {qname}_row.csv files')
    p.add_argument('--config', required=True, help='Unified config JSON')
    p.add_argument('--mode', default='upsert', choices=['replace', 'append', 'upsert'])
    p.add_argument('--db', help='Database name')
    p.add_argument('--date-var', help='Date column name')

    p = sub.add_parser('load-col', help='Load column stats from CSV folder')
    p.add_argument('project_db')
    p.add_argument('folder', help='Folder with {qname}_col.csv files')
    p.add_argument('--config', required=True, help='Unified config JSON')
    p.add_argument('--mode', default='upsert', choices=['upsert', 'replace'])

    p = sub.add_parser('load-columns', help='Load column metadata')
    p.add_argument('project_db')
    p.add_argument('--config', required=True, help='Unified config JSON')
    p.add_argument('--csv', dest='csv_files', action='append', default=[],
                   help='Metadata CSV (repeatable)')
    p.add_argument('--columns-dir', dest='columns_dir',
                   help='Dump per-table {qname}_columns.csv (COLUMN_NAME,DATA_TYPE) into this dir')

    p = sub.add_parser('compare-row', help='Compare row counts')
    p.add_argument('project_db')
    p.add_argument('--config', required=True)
    p.add_argument('--from-date')
    p.add_argument('--to-date')
    p.add_argument('-y', '--yes', action='store_true', help='Skip prompts')
    p.add_argument('--html', help='Output HTML report')
    p.add_argument('--title')
    p.add_argument('--subtitle')

    p = sub.add_parser('compare-col', help='Compare column statistics')
    p.add_argument('project_db')
    p.add_argument('--config', required=True)
    p.add_argument('--from-date')
    p.add_argument('--to-date')
    p.add_argument('--no-date-filter', action='store_true')
    p.add_argument('--vintage', choices=['month', 'quarter', 'year'])
    p.add_argument('-y', '--yes', action='store_true', help='Skip prompts')
    p.add_argument('--html')
    p.add_argument('--excel', help='Excel report path (default: alongside --html as .xlsx)')
    p.add_argument('--title')
    p.add_argument('--subtitle')

    _PRESET_HELP = ("Stats preset for column extraction: quick / standard / "
                    "detailed (see constants.STATS_PRESETS)")
    _STATS_OVERRIDE_HELP = ("Per-stat on/off toggles, comma-separated. Keys: "
                           "n_total, n_missing, n_unique, min_max, length, "
                           "hash_sum, top_10. Example: "
                           "--stats-override n_unique=on,hash_sum=off")

    p = sub.add_parser('compare-csv',
                       help='Incremental col-stats compare: per vintage window, pull each '
                            'side to a tmp CSV, compute stats locally in DuckDB, upsert + compare '
                            '(bounded memory; avoids one huge backend col scan). Self-contained: '
                            'discovers count-match dates itself (reuses _row_counts if loaded); '
                            'no prior row-compare needed')
    p.add_argument('project_db')
    p.add_argument('--config', required=True)
    p.add_argument('--pair', help='Restrict to one pair (default: all non-skipped pairs)')
    p.add_argument('--vintage', default=None,
                   choices=['day', 'week', 'month', 'quarter', 'year'],
                   help='One pull window per vintage bucket. Default: the per-pair '
                        '"vintage" field if set, else day')
    p.add_argument('--from-date')
    p.add_argument('--to-date')
    p.add_argument('--preset', choices=['quick', 'standard', 'detailed'], help=_PRESET_HELP)
    p.add_argument('--stats-override', dest='stats_override', help=_STATS_OVERRIDE_HELP)
    p.add_argument('--sample',
                   help='Default sample to cut the download: integer = fixed #keys/window '
                        '(IN-list on the raw key, index-friendly), float in (0,1] = fraction '
                        'via a hash-bucket pushdown predicate (no key list; scales to billions). '
                        'A per-pair "sample" field overrides this. Requires a key (the SAME rows '
                        'land on BOTH sides via the identical hash, kept comparable)')
    p.add_argument('--key',
                   help='Default composite row key (comma-separated left columns; right resolved '
                        'via col_map) for --sample. A per-pair "key" field overrides this; key '
                        'columns MUST be in col_map (else error). Fraction sampling normalizes the '
                        'key (STRICT family: trim/lower/nfc + numeric/date canonicalization) so the '
                        'same logical key hashes to the same bucket on every backend. Fixed-N uses '
                        'the raw key by default; set per-pair "key_normalize": true to normalize it')
    p.add_argument('--data-dir', dest='data_dir',
                   help='Folder for the per-window downloaded CSVs (large files). Default: '
                        'system temp. Use this to keep big pulls off the project folder / a small disk')
    p.add_argument('--workers', type=int,
                   help='Concurrency for the LOCAL per-column stat compute within each window '
                        '(CLI > settings.max_workers > 1). Vintage windows always run one after '
                        'another (sequential); only the columns inside a window are parallel')
    p.add_argument('--keep-csv', dest='keep_csv', action='store_true',
                   help='Keep the per-window tmp CSV/config dirs (else removed after each window)')
    p.add_argument('--strict', action='store_true',
                   help='Normalize string values before length/hash/n_unique so cosmetic '
                        'differences stop flagging: case + unicode(NFC) + blank/whitespace '
                        "folding, plus trailing-zero collapse on pure decimals ('0.00'=='0', "
                        "'1.50'=='1.5'). Leading zeros are preserved ('007' stays '007', never "
                        "'7'). Same as DTRACK_STRICT=1 but scoped to this run")
    p.add_argument('--force', action='store_true',
                   help='Re-pull + recompute every window (ignore the skip-cache). Skip-cache '
                        'is the default: windows already in _col_stats are skipped, and a changed '
                        '--sample/--key/col_filter/--preset/--vintage auto-triggers a re-pull')
    p.add_argument('--resume', action='store_true',
                   help='(deprecated no-op) skip-cache is now the default; use --force to re-pull')
    p.add_argument('--verbose', action='store_true',
                   help='Print full backend SQL and per-block extract progress (default: one '
                        'concise line per step with rows + seconds; full detail always goes to run.log)')
    p.add_argument('--clean', choices=['marks', 'cache', 'all'],
                   help='Reset before running (3 tiers): marks = clear the run signature '
                        '(re-evaluate skip); cache = + drop this pair\'s _col_stats/_row_counts/'
                        'comparisons/sampled-keys + .state caches (re-pull/recompute); '
                        'all = + delete project.db and wipe --data-dir scratch')

    p = sub.add_parser('gen-aws', help='Extract data from AWS Athena')
    p.add_argument('config')
    p.add_argument('--outdir', default='./csv/')
    p.add_argument('--type', default='both', choices=['row', 'col', 'both'])
    p.add_argument('--workers', type=int)
    p.add_argument('--db', dest='db_path')
    p.add_argument('--vintage')
    p.add_argument('--force', action='store_true')
    p.add_argument('--from-date', help='Start date for incremental extraction')
    p.add_argument('--to-date', help='End date for incremental extraction')
    p.add_argument('--preset', choices=['quick', 'standard', 'detailed'],
                   help=_PRESET_HELP)
    p.add_argument('--stats-override', dest='stats_override',
                   help=_STATS_OVERRIDE_HELP)

    p = sub.add_parser('gen-databricks',
                       help='Extract data from Databricks SQL (Python-direct via databricks-sql)')
    p.add_argument('config')
    p.add_argument('--outdir', default='./csv/')
    p.add_argument('--type', default='both', choices=['row', 'col', 'both'])
    p.add_argument('--workers', type=int,
                   help='Concurrent col-extraction workers (default: DATABRICKS_MAX_WORKERS or 4)')
    p.add_argument('--db', dest='db_path')
    p.add_argument('--vintage')
    p.add_argument('--force', action='store_true')
    p.add_argument('--from-date', help='Start date for incremental extraction')
    p.add_argument('--to-date', help='End date for incremental extraction')
    p.add_argument('--preset', choices=['quick', 'standard', 'detailed'],
                   help=_PRESET_HELP)
    p.add_argument('--stats-override', dest='stats_override',
                   help=_STATS_OVERRIDE_HELP)

    p = sub.add_parser('gen-oracle', help='Extract data from Oracle (Python-direct via oracledb)')
    p.add_argument('config')
    p.add_argument('--outdir', default='./csv/')
    p.add_argument('--type', default='both', choices=['row', 'col', 'both'])
    p.add_argument('--workers', type=int,
                   help='Concurrent Oracle sessions (default: ORACLE_MAX_WORKERS or 2)')
    p.add_argument('--db', dest='db_path')
    p.add_argument('--vintage')
    p.add_argument('--force', action='store_true')
    p.add_argument('--from-date', help='Start date for incremental extraction')
    p.add_argument('--to-date', help='End date for incremental extraction')
    p.add_argument('--preset', choices=['quick', 'standard', 'detailed'],
                   help=_PRESET_HELP)
    p.add_argument('--stats-override', dest='stats_override',
                   help=_STATS_OVERRIDE_HELP)

    p = sub.add_parser('match-columns', help='Match columns between paired tables')
    p.add_argument('project_db')
    p.add_argument('--config', required=True)
    p.add_argument('--mode', default='merge', choices=['merge', 'replace'])
    p.add_argument('--map-csv', dest='map_csv',
                   help='Explicit mappings CSV (pair,left,right); overrides auto/rules')
    p.add_argument('--yes', action='store_true',
                   help='non-interactive: keep auto-matches, skip the manual mapping prompt')

    p = sub.add_parser('sample-hash',
                       help='Sample N rows by key from both sides; dump value + backend hash16 to Excel (debug hash_sum)')
    p.add_argument('project_db')
    p.add_argument('--config', required=True)
    p.add_argument('--key', required=True,
                   help='Left key column (right resolved via col_map); composite via comma')
    p.add_argument('--pair', help='Restrict to one pair (default: all non-skipped pairs)')
    p.add_argument('-n', '--sample', dest='sample_n', type=int, default=20,
                   help='Rows to sample per pair (default 20). -1 = ignore values '
                        'and report KEY combinations unmatched across sides (anti-join)')
    p.add_argument('--out', help='Output .xlsx path (default sample_hash.xlsx)')

    p = sub.add_parser('extract', help='Extract row/col CSVs for all platforms in the config')
    p.add_argument('project_db')
    p.add_argument('--config', required=True)
    p.add_argument('--outdir', help='Base output directory (csv/ derived from it)')
    p.add_argument('--csv-outdir', help='CSV output directory')
    p.add_argument('--type', default='both', choices=['row', 'col', 'both'])
    p.add_argument('--vintage')
    p.add_argument('--workers', type=int)
    p.add_argument('--force', action='store_true')
    p.add_argument('--from-date', help='Start date for incremental extraction')
    p.add_argument('--to-date', help='End date for incremental extraction')
    p.add_argument('--preset', choices=['quick', 'standard', 'detailed'],
                   help=_PRESET_HELP)
    p.add_argument('--stats-override', dest='stats_override',
                   help=_STATS_OVERRIDE_HELP)

    p = sub.add_parser('run-sql',
                       help='Run already-generated extract_{row,col}.<platform>.sql files (see `dtrack generate`)')
    p.add_argument('project_db')
    p.add_argument('--config', required=True)
    p.add_argument('--outdir', help='Base output directory (csv/ derived from it)')
    p.add_argument('--csv-outdir', help='CSV output directory')
    p.add_argument('--type', default='both', choices=['row', 'col', 'both'])
    p.add_argument('--workers', type=int)
    p.add_argument('--resume', action='store_true', help='Skip blocks whose output CSV already exists')

    p = sub.add_parser('generate',
                       help='Write extract_{row,col} SQL/SAS for all platforms WITHOUT running')
    p.add_argument('project_db')
    p.add_argument('--config', required=True)
    p.add_argument('--outdir', help='Base output directory (csv/ derived from it)')
    p.add_argument('--csv-outdir', help='CSV output directory')
    p.add_argument('--type', default='both', choices=['row', 'col', 'both'])
    p.add_argument('--vintage')
    p.add_argument('--from-date', help='Start date for incremental extraction')
    p.add_argument('--to-date', help='End date for incremental extraction')
    p.add_argument('--preset', choices=['quick', 'standard', 'detailed'],
                   help=_PRESET_HELP)
    p.add_argument('--stats-override', dest='stats_override',
                   help=_STATS_OVERRIDE_HELP)

    p = sub.add_parser('col-sample',
                       help='Sample rows by key (deterministic) + columns, write key IN (...) into where_map, '
                            'and generate col SQL (then run-sql --type col)')
    p.add_argument('project_db')
    p.add_argument('--config', required=True)
    p.add_argument('--key', required=True,
                   help='Key column on left (right resolved via col_map)')
    p.add_argument('-n', '--sample', dest='sample_n', type=int, default=20,
                   help='Number of keys to sample per pair (default 20)')
    p.add_argument('--keys', help='Explicit key values (comma) to use instead of sampling')
    p.add_argument('--keys-file', dest='keys_file',
                   help='File of key values (one per line) instead of sampling')
    p.add_argument('--pair', help='Restrict to one pair (default: all non-skipped pairs)')
    p.add_argument('--cols', help='Columns to compare (comma, left names); else all mapped')
    p.add_argument('--sample-cols', dest='sample_cols', type=int,
                   help='Randomly sample N columns from col_map')
    p.add_argument('--outdir', help='Base output directory (csv/ derived from it)')
    p.add_argument('--vintage')
    p.add_argument('--from-date')
    p.add_argument('--to-date')
    p.add_argument('--preset', choices=['quick', 'standard', 'detailed'], help=_PRESET_HELP)
    p.add_argument('--stats-override', dest='stats_override', help=_STATS_OVERRIDE_HELP)

    p = sub.add_parser('sample-key',
                       help='Composite-key sampler: value+hash16 Excel dump (default/--out), '
                            'key anti-join (-n -1), and/or where_map+col SQL gen (--gen-col)')
    p.add_argument('project_db')
    p.add_argument('--config', required=True)
    p.add_argument('--key', required=True,
                   help='Left key column(s), comma-separated for a composite key (right resolved via col_map)')
    p.add_argument('--pair', help='Restrict to one pair (default: all non-skipped pairs)')
    p.add_argument('-n', '--sample', dest='sample_n', type=int, default=20,
                   help='Keys to sample per pair (default 20). -1 = report KEY combinations '
                        'unmatched across sides (anti-join); value dump only')
    p.add_argument('--out', help='Value+hash16 Excel path (default sample_hash.xlsx). '
                                 'Runs the dump even alongside --gen-col')
    p.add_argument('--gen-col', dest='gen_col', action='store_true',
                   help='Persist the sampled key IN (...) into where_map and generate col SQL '
                        '(col-sample behavior). Combine with --out to also dump values')
    p.add_argument('--keys', help='(--gen-col) Explicit key values (comma) instead of sampling; single key only')
    p.add_argument('--keys-file', dest='keys_file',
                   help='(--gen-col) File of key values (one per line) instead of sampling; single key only')
    p.add_argument('--cols', help='(--gen-col) Columns to compare (comma, left names); else all mapped')
    p.add_argument('--sample-cols', dest='sample_cols', type=int,
                   help='(--gen-col) Randomly sample N columns from col_map')
    p.add_argument('--outdir', help='(--gen-col) Base output directory (csv/ derived from it)')
    p.add_argument('--vintage')
    p.add_argument('--from-date')
    p.add_argument('--to-date')
    p.add_argument('--preset', choices=['quick', 'standard', 'detailed'], help=_PRESET_HELP)
    p.add_argument('--stats-override', dest='stats_override', help=_STATS_OVERRIDE_HELP)

    p = sub.add_parser('serve', help='Start web UI')
    p.add_argument('project_db')
    p.add_argument('--config', default=None, help='Unified config JSON (optional)')
    p.add_argument('--port', type=int, default=8080)
    p.add_argument('--host', default='0.0.0.0')

    p = sub.add_parser('query', help='Run SQL query')
    p.add_argument('project_db')
    p.add_argument('sql')
    p.add_argument('--write', action='store_true')

    p = sub.add_parser('doctor', help='Show resolved env-file, Oracle macros, AWS env vars')

    args = parser.parse_args()

    if args.debug == 'ipdb':
        import ipdb
        sys.breakpointhook = ipdb.set_trace
        def _ipdb_excepthook(type, value, tb):
            import traceback
            traceback.print_exception(type, value, tb)
            ipdb.post_mortem(tb)
        sys.excepthook = _ipdb_excepthook
    elif args.debug == 'attach':
        import debugpy
        debugpy.listen(('0.0.0.0', 5678))
        print('Waiting for debugger attach on port 5678...')
        debugpy.wait_for_client()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    env_file = getattr(args, 'env_file', None) or os.environ.get('DTRACK_ENV_FILE')
    if not env_file:
        from dotenv import find_dotenv
        for fname in ('dtrack.conf', '.env', 'dtrack.env'):
            found = find_dotenv(usecwd=True, filename=fname)
            if found:
                env_file = found
                break
        if not env_file:
            from pathlib import Path as _P
            for path in (_P.home() / '.dtrack' / 'dtrack.conf',
                         _P('/etc/dtrack/dtrack.conf')):
                if path.exists():
                    env_file = str(path)
                    break

    if env_file:
        from dotenv import dotenv_values
        for key, val in dotenv_values(env_file).items():
            if val is not None:
                os.environ.setdefault(key, val)
        os.environ.setdefault('DTRACK_ENV_FILE_LOADED', env_file)

    commands = {
        'init': cmd_init,
        'load-row': cmd_load_row,
        'load-col': cmd_load_col,
        'load-columns': cmd_load_columns,
        'compare-row': cmd_compare_row,
        'compare-col': cmd_compare_col,
        'compare-csv': cmd_compare_csv,
        'gen-aws': cmd_gen_aws,
        'gen-databricks': cmd_gen_databricks,
        'gen-oracle': cmd_gen_oracle,
        'match-columns': cmd_match_columns,
        'extract': cmd_extract,
        'generate': cmd_generate,
        'run-sql': cmd_run_sql,
        'sample-hash': cmd_sample_hash,
        'col-sample': cmd_col_sample,
        'sample-key': cmd_sample_key,
        'serve': cmd_serve,
        'query': cmd_query,
        'doctor': cmd_doctor,
    }

    handler = commands.get(args.command)
    if handler:
        from .logmgr import get_logger, banner
        import shlex
        log = get_logger()
        banner(f"dtrack {args.command}  |  {shlex.join(sys.argv[1:])}")
        log.info("[cli] %s %s", args.command,
                 {k: v for k, v in vars(args).items()
                  if v not in (None, False) and k not in ('command', 'debug')})
        try:
            handler(args)
            log.info("[cli] %s done", args.command)
        except Exception as e:
            log.exception("[cli] %s failed: %s", args.command, e)
            if args.debug:
                raise
            print(f"Error: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()
            sys.exit(1)
    else:
        print(f"Unknown command: {args.command}")
        sys.exit(1)

if __name__ == '__main__':
    main()
