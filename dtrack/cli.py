#!/usr/bin/env python3
"""Command-line interface for dtrack (config-driven only)."""

import argparse
import json
import os
import re
import sys

from .db import (
    init_database,
    list_tables,
    get_row_counts,
    get_col_stats,
    get_metadata,
    list_table_pairs,
    get_table_pair,
    register_table_pair,
    insert_column_meta,
    get_column_meta,
    save_row_comparison,
    get_row_comparison,
    patch_metadata,
    save_col_comparison,
    sync_config_to_db,
    generic_upsert,
    generic_update,
    generic_delete,
    parse_where_clause,
)
from .loader import load_row_counts, load_column_data, load_precomputed_col_stats
from .compare import (
    compare_row_counts,
    compare_column_stats,
    get_column_mapping,
    parse_col_map_string,
    match_columns_from_dicts,
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
    add_ignored_columns,
    get_ignored_rows,
    get_ignored_columns,
    get_col_type_overrides,
)
from .platforms.base import qualified_name, is_sas_table
from .interact import prompt_skip_pair, prompt_ignore_items, prompt_mapping, confirm, save_and_pause


# ============================================================================
# Helper: load config + tables
# ============================================================================

def _load_config(config_path):
    """Load and return (config, config_path)."""
    if not os.path.exists(config_path):
        print(f"Error: Config file not found: {config_path}")
        sys.exit(1)
    return load_unified_config(config_path)


def _require_db(db_path):
    """Exit if db doesn't exist."""
    if not os.path.exists(db_path):
        print(f"Error: Database not found: {db_path}")
        print(f"Run: dtrack init {db_path}")
        sys.exit(1)


# ============================================================================
# Commands
# ============================================================================

def cmd_init(args):
    """Initialize a new dtrack database."""
    db_path = args.project_db
    if args.refresh:
        if not os.path.exists(db_path):
            print(f"Error: Database not found: {db_path}")
            sys.exit(1)
        from .db import refresh_database
        actions = refresh_database(db_path)
        print(f"Refreshed database: {db_path}")
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


def cmd_load_row(args):
    """Load row count data from CSV."""
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
            where_clause=tbl.get('where', ''),
        )
        rows = get_row_counts(args.project_db, qname)
        total = sum(count for _, count in rows)
        print(f"  Loaded {len(rows)} date buckets, total: {total:,}")


def cmd_load_col(args):
    """Load column statistics from CSV."""
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


def cmd_load_columns(args):
    """Load column metadata from CSV or live discovery."""
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
                    # Strip whitespace from keys to handle \r\n line endings
                    row = {k.strip(): v for k, v in row.items()}
                    src = row.get('source') or row.get('SOURCE', '')
                    tbl = row.get('table') or row.get('TABLE', '')
                    col = row.get('column_name') or row.get('COLUMN_NAME', '')
                    dt = row.get('data_type') or row.get('DATA_TYPE', '')
                    if tbl and col:
                        key = (src.lower(), tbl.lower())
                        csv_lookup.setdefault(key, {})[col] = dt

    for tbl in tables:
        source = tbl.get('source', '')
        qname = qualified_name(tbl)
        raw_table = tbl['table']
        name = tbl.get('name', '')

        csv_key = (source.lower(), raw_table.lower())
        csv_key_name = (source.lower(), name.lower())
        matched_key = csv_key if csv_key in csv_lookup else (csv_key_name if csv_key_name in csv_lookup else None)

        if matched_key:
            columns = csv_lookup[matched_key]
            count = insert_column_meta(args.project_db, qname, columns, source=source)
            print(f"{qname}: loaded {count} columns from CSV")
            continue

        conn_macro = tbl.get('conn_macro', '')
        print(f"{qname}: discovering columns from {source} ({conn_macro}: {raw_table})")
        _load_columns_entry(args.project_db, qname, raw_table, source, conn_macro)


def _load_columns_entry(project_db, store_name, raw_table, source, conn_macro):
    """Discover and load columns for one table entry."""
    import csv as csv_mod

    if conn_macro and source.lower() == 'aws':
        mock_dir = os.environ.get('DTRACK_MOCK') or os.environ.get('DTRACK_ATHENA_MOCK')
        if mock_dir:
            mock_csv = os.path.join(mock_dir, f"{store_name}_columns.csv")
            if not os.path.exists(mock_csv):
                print(f"WARNING: [mock] File not found: {mock_csv}, skipping {store_name}")
                return
            columns = {}
            with open(mock_csv, 'r', newline='') as f:
                reader = csv_mod.DictReader(f)
                for row in reader:
                    # Strip whitespace from keys to handle \r\n line endings
                    row = {k.strip(): v for k, v in row.items()}
                    name = row.get('column_name') or row.get('COLUMN_NAME', '')
                    dtype = row.get('data_type') or row.get('DATA_TYPE', '')
                    if name:
                        columns[name] = dtype
            print(f"[mock] Loaded {len(columns)} columns from {mock_csv}")
        else:
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
    elif conn_macro:
        from .db import oracle_connect, discover_columns
        print(f"Connecting to Oracle via '{conn_macro}'...")
        conn = oracle_connect(conn_macro)
        print(f"Discovering columns for: {raw_table}")
        columns = discover_columns(conn, raw_table)
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


def _print_row_comparison(db_path, table_left, table_right, source_left, source_right,
                          from_date=None, to_date=None, pair_name=None,
                          date_col_left=None, date_col_right=None):
    """Run and print row count comparison for a single pair."""
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
    dr_left = summary['date_range_left']
    dr_right = summary['date_range_right']

    print(f"\n{source_left}: {dr_left[0]} to {dr_left[1]} | {summary['count_left']} dates | total: {summary['total_left']:,}")
    print(f"{source_right}: {dr_right[0]} to {dr_right[1]} | {summary['count_right']} dates | total: {summary['total_right']:,}")

    # Overlap
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
        print(f"\nMismatched: (none)")

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


def _build_where_from_dates(table_cfg, matching_dates, excluded_dates, db_path=None):
    """Build WHERE clause from matching and excluded dates."""
    date_col = table_cfg.get('date_col', 'dt')
    is_sas = is_sas_table(table_cfg)

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
        if is_sas:
            col_expr = f"datepart({date_col})"
            def fmt_date(d):
                from datetime import datetime as _dt
                if re.match(r'^\d{6}$', str(d)):
                    return f"'{d}'"
                dt_obj = _dt.strptime(str(d), "%Y-%m-%d")
                return f"'{dt_obj.strftime('%d%b%Y').upper()}'d"
        elif source == 'aws':
            col_expr = f"CAST({date_col} AS DATE)"
            def fmt_date(d):
                return f"'{d}'" if re.match(r'^\d{6}$', str(d)) else f"DATE '{d}'"
        else:
            col_expr = f"TRUNC({date_col})"
            def fmt_date(d):
                return f"'{d}'" if re.match(r'^\d{6}$', str(d)) else f"DATE '{d}'"
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
                return f"DATE '{original}'"
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


def cmd_compare_row(args):
    """Compare row counts between paired tables."""
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

        # HITL: skip/ignore prompt
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

    # Save where_map to config
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
            "Add time_map values (check ./sas/_timing.csv and ./csv/_timing.csv)",
            "Add comment_map notes (shown as tooltips in HTML)",
        ])
    else:
        save_unified_config(config, args.config)

    sync_config_to_db(args.project_db, config)
    print(f"Synced to database: {args.project_db}")

    # HTML report
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


def cmd_compare_col(args):
    """Compare column statistics between paired tables."""
    _require_db(args.project_db)
    config = _load_config(args.config)
    html_path = getattr(args, 'html', None)
    yes = getattr(args, 'yes', False)

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

            # Build diff_map
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

    # HTML
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

    try:
        save_unified_config(config, args.config)
        print(f"Config saved: {args.config}")
    except Exception as e:
        print(f"Warning: Could not save config: {e}")


def cmd_gen_sas(args):
    """Generate SAS extraction files from config."""
    from .platforms.oracle import gen_sas
    if not os.path.exists(args.config):
        print(f"Error: Config file not found: {args.config}")
        sys.exit(1)
    types = [args.type] if args.type != "both" else ["row", "col"]
    db_path = getattr(args, 'db_path', None)
    vintage = getattr(args, 'vintage', None)
    from_date = getattr(args, 'from_date', None)
    to_date = getattr(args, 'to_date', None)
    gen_sas(args.config, args.outdir, types=types, db_path=db_path, vintage=vintage,
            from_date=from_date, to_date=to_date)


def cmd_gen_aws(args):
    """Extract data from AWS Athena."""
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
    extract_aws(args.config, args.outdir, types=types, max_workers=args.workers,
                db_path=db_path, vintage=vintage, force=force,
                from_date=from_date, to_date=to_date)


def cmd_match_columns(args):
    """Match columns between paired tables."""
    _require_db(args.project_db)
    config = _load_config(args.config)

    matched_columns = {}
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

        result = match_columns_from_dicts(left_cols, right_cols, left_table, right_table)
        matched = {**result.get('matched', {}), **existing_col_map}

        left_only = result.get(f'{left_table}_only', [])
        right_only = result.get(f'{right_table}_only', [])

        # Filter already-mapped
        existing_left = set(existing_col_map.keys())
        existing_right = set(existing_col_map.values())
        left_only = [c for c in left_only if c['name'] not in existing_left]
        right_only = [c for c in right_only if c['name'] not in existing_right]

        auto_count = len(matched) - len(existing_col_map)
        print(f"Auto-matched: {auto_count} columns")
        if existing_col_map:
            print(f"Existing mappings: {len(existing_col_map)}")

        if left_only or right_only:
            left_items = [(c['name'], c['type']) for c in left_only]
            right_items = [(c['name'], c['type']) for c in right_only]
            matched = prompt_mapping(left_items, right_items, existing_map=matched)

        matched_columns[pair_name] = matched

    if matched_columns:
        for pair_name, m in matched_columns.items():
            set_pair_col_map(config, pair_name, m)
            print(f"  {pair_name}: {len(m)} column mappings saved")
        save_unified_config(config, args.config)
        print(f"Saved to: {args.config}")
        sync_config_to_db(args.project_db, config)
        print(f"Synced to database: {args.project_db}")


def cmd_list(args):
    """List all tables in the database."""
    _require_db(args.project_db)
    tables = list_tables(args.project_db)
    if not tables:
        print("No tables found")
        return
    print(f"\n{'Table':<30} {'Type':<8} {'Source':<10} {'Vintage':<8}")
    print("-" * 60)
    for t in tables:
        print(f"{t.get('table_name',''):<30} {t.get('data_type',''):<8} {t.get('source',''):<10} {t.get('vintage',''):<8}")
    print(f"\nTotal: {len(tables)} tables")


def cmd_list_pairs(args):
    """List all registered table pairs."""
    _require_db(args.project_db)
    pairs = list_table_pairs(args.project_db)
    if not pairs:
        print("No table pairs found")
        return
    print(f"\n{'Pair Name':<25} {'Left Table':<25} {'Right Table':<25} {'Mappings':<10}")
    print("-" * 90)
    for p in pairs:
        m = p["col_mappings"]
        map_str = f"{len(m)} cols" if m else "(none)"
        print(f"{p['pair_name']:<25} {p['table_left']:<25} {p['table_right']:<25} {map_str:<10}")
    print(f"\nTotal: {len(pairs)} pair(s)")


def cmd_run(args):
    """Run the full pipeline: init → extract → load → compare → HTML."""
    from .pipeline import run_pipeline
    run_pipeline(
        project_db=args.project_db,
        config_path=args.config,
        outdir=getattr(args, 'outdir', None),
        sas_outdir=getattr(args, 'sas_outdir', None),
        csv_outdir=getattr(args, 'csv_outdir', None),
        types=[args.type] if args.type != "both" else ["row", "col"],
        vintage=getattr(args, 'vintage', None),
        yes=getattr(args, 'yes', False),
        html_row=getattr(args, 'html_row', None),
        html_col=getattr(args, 'html_col', None),
        title=getattr(args, 'title', None),
        subtitle=getattr(args, 'subtitle', None),
        workers=getattr(args, 'workers', None),
        force=getattr(args, 'force', False),
        skip_extract=getattr(args, 'skip_extract', False),
        skip_load=getattr(args, 'skip_load', False),
        skip_compare=getattr(args, 'skip_compare', False),
        from_date=getattr(args, 'from_date', None),
        to_date=getattr(args, 'to_date', None),
    )


def cmd_serve(args):
    """Start the dtrack web UI."""
    _require_db(args.project_db)

    # Create a temporary config if none provided
    config_path = args.config
    if not config_path:
        import tempfile
        import json
        # Create a temporary config with empty pairs
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


def cmd_query(args):
    """Run a SQL query against the database."""
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


# ============================================================================
# Main
# ============================================================================

def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="dtrack - Data tracking CLI for row counts and column statistics",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--env', dest='env_file', help='Path to .env file')
    parser.add_argument('--debug', choices=['ipdb', 'attach'], default=None)

    sub = parser.add_subparsers(dest='command', help='Available commands')

    # init
    p = sub.add_parser('init', help='Initialize database')
    p.add_argument('project_db')
    p.add_argument('--force', action='store_true')
    p.add_argument('--refresh', action='store_true')

    # load-row
    p = sub.add_parser('load-row', help='Load row counts from CSV folder')
    p.add_argument('project_db')
    p.add_argument('folder', help='Folder with {qname}_row.csv files')
    p.add_argument('--config', required=True, help='Unified config JSON')
    p.add_argument('--mode', default='upsert', choices=['replace', 'append', 'upsert'])
    p.add_argument('--db', help='Database name')
    p.add_argument('--date-var', help='Date column name')

    # load-col
    p = sub.add_parser('load-col', help='Load column stats from CSV folder')
    p.add_argument('project_db')
    p.add_argument('folder', help='Folder with {qname}_col.csv files')
    p.add_argument('--config', required=True, help='Unified config JSON')
    p.add_argument('--mode', default='upsert', choices=['upsert', 'replace'])

    # load-columns
    p = sub.add_parser('load-columns', help='Load column metadata')
    p.add_argument('project_db')
    p.add_argument('--config', required=True, help='Unified config JSON')
    p.add_argument('--csv', dest='csv_files', action='append', default=[],
                   help='Metadata CSV (repeatable)')

    # compare-row
    p = sub.add_parser('compare-row', help='Compare row counts')
    p.add_argument('project_db')
    p.add_argument('--config', required=True)
    p.add_argument('--from-date')
    p.add_argument('--to-date')
    p.add_argument('-y', '--yes', action='store_true', help='Skip prompts')
    p.add_argument('--html', help='Output HTML report')
    p.add_argument('--title')
    p.add_argument('--subtitle')

    # compare-col
    p = sub.add_parser('compare-col', help='Compare column statistics')
    p.add_argument('project_db')
    p.add_argument('--config', required=True)
    p.add_argument('--from-date')
    p.add_argument('--to-date')
    p.add_argument('--no-date-filter', action='store_true')
    p.add_argument('--vintage', choices=['month', 'quarter', 'year'])
    p.add_argument('-y', '--yes', action='store_true', help='Skip prompts')
    p.add_argument('--html')
    p.add_argument('--title')
    p.add_argument('--subtitle')

    # gen-sas
    p = sub.add_parser('gen-sas', help='Generate SAS extraction files')
    p.add_argument('config')
    p.add_argument('--outdir', default='./sas/')
    p.add_argument('--type', default='both', choices=['row', 'col', 'both'])
    p.add_argument('--db', dest='db_path')
    p.add_argument('--vintage')
    p.add_argument('--from-date', help='Start date for incremental extraction')
    p.add_argument('--to-date', help='End date for incremental extraction')

    # gen-aws
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

    # match-columns
    p = sub.add_parser('match-columns', help='Match columns between paired tables')
    p.add_argument('project_db')
    p.add_argument('--config', required=True)
    p.add_argument('--mode', default='merge', choices=['merge', 'replace'])

    # list
    p = sub.add_parser('list', help='List all tables')
    p.add_argument('project_db')

    # list-pairs
    p = sub.add_parser('list-pairs', help='List table pairs')
    p.add_argument('project_db')

    # run (full pipeline)
    p = sub.add_parser('run', help='Run full pipeline (init → extract → load → compare → HTML)')
    p.add_argument('project_db')
    p.add_argument('--config', required=True)
    p.add_argument('--outdir', help='Base output directory')
    p.add_argument('--sas-outdir', help='SAS output directory')
    p.add_argument('--csv-outdir', help='CSV output directory')
    p.add_argument('--type', default='both', choices=['row', 'col', 'both'])
    p.add_argument('--vintage')
    p.add_argument('-y', '--yes', action='store_true', help='Skip prompts')
    p.add_argument('--html-row', help='Row comparison HTML path')
    p.add_argument('--html-col', help='Column comparison HTML path')
    p.add_argument('--title')
    p.add_argument('--subtitle')
    p.add_argument('--workers', type=int)
    p.add_argument('--force', action='store_true')
    p.add_argument('--skip-extract', action='store_true')
    p.add_argument('--skip-load', action='store_true')
    p.add_argument('--skip-compare', action='store_true')
    p.add_argument('--from-date', help='Start date for incremental extraction')
    p.add_argument('--to-date', help='End date for incremental extraction')

    # serve
    p = sub.add_parser('serve', help='Start web UI')
    p.add_argument('project_db')
    p.add_argument('--config', default=None, help='Unified config JSON (optional)')
    p.add_argument('--port', type=int, default=8080)
    p.add_argument('--host', default='0.0.0.0')

    # query
    p = sub.add_parser('query', help='Run SQL query')
    p.add_argument('project_db')
    p.add_argument('sql')
    p.add_argument('--write', action='store_true')

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

    # Load env
    env_file = getattr(args, 'env_file', None)
    if env_file:
        from dotenv import dotenv_values
        env_dir = os.path.dirname(os.path.abspath(env_file))
        for key, val in dotenv_values(env_file).items():
            os.environ[key] = val
    else:
        from dotenv import load_dotenv, find_dotenv
        found = find_dotenv(usecwd=True, filename='dtrack.conf')
        env_dir = os.path.dirname(found) if found else os.getcwd()
        load_dotenv(found)

    for mock_var in ('DTRACK_MOCK', 'DTRACK_ORACLE_MOCK', 'DTRACK_ATHENA_MOCK'):
        mock_dir = os.environ.get(mock_var)
        if mock_dir and not os.path.isabs(mock_dir):
            os.environ[mock_var] = os.path.join(env_dir, mock_dir)

    commands = {
        'init': cmd_init,
        'load-row': cmd_load_row,
        'load-col': cmd_load_col,
        'load-columns': cmd_load_columns,
        'compare-row': cmd_compare_row,
        'compare-col': cmd_compare_col,
        'gen-sas': cmd_gen_sas,
        'gen-aws': cmd_gen_aws,
        'match-columns': cmd_match_columns,
        'run': cmd_run,
        'list': cmd_list,
        'list-pairs': cmd_list_pairs,
        'serve': cmd_serve,
        'query': cmd_query,
    }

    handler = commands.get(args.command)
    if handler:
        if args.debug:
            handler(args)
        else:
            try:
                handler(args)
            except Exception as e:
                print(f"Error: {e}", file=sys.stderr)
                import traceback
                traceback.print_exc()
                sys.exit(1)
    else:
        print(f"Unknown command: {args.command}")
        sys.exit(1)


if __name__ == '__main__':
    main()
