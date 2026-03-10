#!/usr/bin/env python3
"""Command-line interface for dtrack"""

import argparse
import re
import sys
import os
from pathlib import Path

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
    save_sampled_dates,
    get_sampled_dates,
    generic_upsert,
    generic_update,
    generic_delete,
    parse_where_clause,
)
from .loader import load_row_counts, load_column_data, load_precomputed_col_stats
from .load_map import load_map
from .compare import (
    compare_row_counts,
    compare_column_stats,
    get_column_mapping,
    parse_col_map_string,
)
from .ppt import PPTBuilder, parse_markdown_to_ppt


def cmd_init(args):
    """Initialize a new dtrack database"""
    db_path = args.project_db
    if os.path.exists(db_path) and not args.force:
        print(f"Error: Database already exists: {db_path}")
        print("Use --force to overwrite")
        sys.exit(1)

    init_database(db_path)
    print(f"Initialized database: {db_path}")


def cmd_load_row(args):
    """Load row count data from CSV"""
    if not os.path.exists(args.project_db):
        print(f"Error: Database not found: {args.project_db}")
        print(f"Run: dtrack init {args.project_db}")
        sys.exit(1)

    config_path = getattr(args, 'config', None)
    if config_path:
        import json
        if not os.path.exists(config_path):
            print(f"Error: Config file not found: {config_path}")
            sys.exit(1)
        if not os.path.isdir(args.file_or_folder):
            print(f"Error: {args.file_or_folder} must be a folder when using --config")
            sys.exit(1)
        with open(config_path, 'r') as f:
            config = json.load(f)

        # Detect config format and extract tables
        if "pairs" in config and isinstance(config["pairs"], dict):
            from .config import get_all_tables_from_unified
            tables = get_all_tables_from_unified(config)
        else:
            tables = config.get('tables', [])

        from .extract import _qualified_name
        for tbl in tables:
            qname = _qualified_name(tbl)
            source = tbl.get('source', '')
            csv_path = os.path.join(args.file_or_folder, f"{qname}_row.csv")
            if not os.path.exists(csv_path):
                print(f"WARNING: {csv_path} not found, skipping {qname}")
                continue

            print(f"\n--- {qname} ---")
            # date_col from config is the source DB column name (e.g. RPT_DT),
            # not necessarily the CSV header. Pass it for metadata only.
            config_date_col = tbl.get('date_col')
            # Use vintage from config, fall back to command line arg
            table_vintage = tbl.get('vintage', args.vintage)
            load_row_counts(
                db_path=args.project_db,
                file_or_folder=csv_path,
                table_name=qname,
                mode=args.mode,
                vintage=table_vintage,
                source=source,
                db_name=args.db,
                source_table=tbl.get('table', ''),
                date_col=args.date_var,
                date_var_override=config_date_col,
                where_clause=tbl.get('where', ''),
            )
            rows = get_row_counts(args.project_db, qname)
            total = sum(count for _, count in rows)
            print(f"  Loaded {len(rows)} date buckets, total: {total:,}")
        return

    # Determine table name
    if args.table_name:
        table_name = args.table_name
    else:
        # Use filename without extension
        if os.path.isfile(args.file_or_folder):
            table_name = Path(args.file_or_folder).stem
        else:
            print("Error: --table is required when loading from a folder")
            sys.exit(1)

    print(f"Loading row counts into table: {table_name}")
    print(f"  Source: {args.file_or_folder}")
    print(f"  Mode: {args.mode}")
    print(f"  Vintage: {args.vintage}")

    load_row_counts(
        db_path=args.project_db,
        file_or_folder=args.file_or_folder,
        table_name=table_name,
        mode=args.mode,
        vintage=args.vintage,
        source=args.source,
        db_name=args.db,
        source_table=args.source_table,
        date_col=args.date_var,
    )

    # Show summary
    rows = get_row_counts(args.project_db, table_name)
    print(f"✓ Loaded {len(rows)} date buckets")
    total = sum(count for _, count in rows)
    print(f"  Total row count: {total:,}")


def cmd_list(args):
    """List all tables in the database"""
    if not os.path.exists(args.project_db):
        print(f"Error: Database not found: {args.project_db}")
        sys.exit(1)

    tables = list_tables(args.project_db)

    if not tables:
        print("No tables found")
        return

    print(f"\nTables in {args.project_db}:")
    print("-" * 100)
    print(f"{'Table':<30} {'Type':<8} {'Source':<10} {'DB':<15} {'Date Var':<12} {'Vintage':<8}")
    print("-" * 100)

    for table in tables:
        name = table.get('table_name', '') or ''
        data_type = table.get('data_type', '') or ''
        source = table.get('source', '') or ''
        db = table.get('db', '') or ''
        date_var = table.get('date_var', '') or ''
        vintage = table.get('vintage', '') or ''

        print(f"{name:<30} {data_type:<8} {source:<10} {db:<15} {date_var:<12} {vintage:<8}")

    print(f"\nTotal: {len(tables)} tables")


def cmd_show(args):
    """Show row count data from a table"""
    if not os.path.exists(args.project_db):
        print(f"Error: Database not found: {args.project_db}")
        sys.exit(1)

    rows = get_row_counts(args.project_db, args.table, limit=args.limit)

    if not rows:
        print(f"No data found in table: {args.table}")
        return

    # Show metadata
    metadata = get_metadata(args.project_db, args.table)
    if metadata:
        print(f"\nTable: {args.table}")
        print(f"Source: {metadata.get('source', 'N/A')}")
        print(f"Database: {metadata.get('db', 'N/A')}")
        print(f"Vintage: {metadata.get('vintage', 'N/A')}")
        print()

    # Show data
    print(f"{'Date':<12} {'Row Count':>12}")
    print("-" * 26)
    for dt, count in rows:
        print(f"{dt:<12} {count:>12,}")

    print("-" * 26)
    total = sum(count for _, count in rows)
    print(f"{'Total:':<12} {total:>12,}")
    print(f"\nShowing {len(rows)} rows")


def cmd_load_map(args):
    """Load table pairs from JSON configuration"""
    if not os.path.exists(args.project_db):
        print(f"Error: Database not found: {args.project_db}")
        print(f"Run: dtrack init {args.project_db}")
        sys.exit(1)

    if not os.path.exists(args.config_file):
        print(f"Error: Config file not found: {args.config_file}")
        sys.exit(1)

    print(f"Loading table pairs from: {args.config_file}")
    print(f"Database: {args.project_db}")
    print(f"Data type: {args.type}")
    print()

    load_map(
        db_path=args.project_db,
        config_path=args.config_file,
        data_type=args.type,
    )


def cmd_list_pairs(args):
    """List all registered table pairs"""
    if not os.path.exists(args.project_db):
        print(f"Error: Database not found: {args.project_db}")
        sys.exit(1)

    pairs = list_table_pairs(args.project_db)

    if not pairs:
        print("No table pairs found")
        return

    print(f"\nRegistered table pairs in {args.project_db}:")
    print("-" * 100)
    print(f"{'Pair Name':<25} {'Left Table':<25} {'Right Table':<25} {'Mappings':<20}")
    print("-" * 100)

    for pair in pairs:
        name = pair["pair_name"]
        left = pair["table_left"]
        right = pair["table_right"]
        mappings = pair["col_mappings"]

        # Show count of mappings
        if mappings:
            map_str = f"{len(mappings)} columns"
        else:
            map_str = "(none)"

        print(f"{name:<25} {left:<25} {right:<25} {map_str:<20}")

    print(f"\nTotal: {len(pairs)} pair(s)")

    # Show details if requested
    if args.verbose:
        print("\nColumn Mappings:")
        print("-" * 100)
        for pair in pairs:
            if pair["col_mappings"]:
                print(f"\n{pair['pair_name']}:")
                for left_col, right_col in pair["col_mappings"].items():
                    print(f"  {left_col} → {right_col}")


def _print_row_comparison(db_path, table_left, table_right, source_left, source_right, from_date=None, to_date=None, pair_name=None):
    """Run and print row count comparison for a single pair."""
    source_left = source_left or "left"
    source_right = source_right or "right"

    print(f"\nComparing row counts: {table_left} vs {table_right}")
    print("=" * 70)

    result = compare_row_counts(
        db_path, table_left, table_right,
        from_date=from_date, to_date=to_date,
    )

    summary = result["summary"]
    dr_left = summary['date_range_left']
    dr_right = summary['date_range_right']

    print(f"\n{source_left}: {dr_left[0]} to {dr_left[1]} | {summary['count_left']} dates | total: {summary['total_left']:,}")
    print(f"{source_right}: {dr_right[0]} to {dr_right[1]} | {summary['count_right']} dates | total: {summary['total_right']:,}")

    # Overlap range
    has_overlap = dr_left[0] and dr_right[0]
    overlap_start = overlap_end = None
    if has_overlap:
        overlap_start = max(dr_left[0], dr_right[0])
        overlap_end = min(dr_left[1], dr_right[1])
        if overlap_start <= overlap_end:
            print(f"\nOverlap range: {overlap_start} to {overlap_end}")
        else:
            print(f"\nOverlap range: (no overlap)")
            has_overlap = False

    # Split only_left/only_right into outside-overlap and misload (inside overlap)
    def _in_overlap(dt):
        return has_overlap and overlap_start <= dt <= overlap_end

    misload_left = [(dt, c) for dt, c in result['only_left'] if _in_overlap(dt)]
    misload_right = [(dt, c) for dt, c in result['only_right'] if _in_overlap(dt)]
    outside_left = [(dt, c) for dt, c in result['only_left'] if not _in_overlap(dt)]
    outside_right = [(dt, c) for dt, c in result['only_right'] if not _in_overlap(dt)]

    # Mismatched (within overlap)
    if result["mismatched"]:
        print(f"\nMismatched ({len(result['mismatched'])} dates):")
        for dt, count_left, count_right in result["mismatched"]:
            diff = count_right - count_left
            print(f"  {dt}: {source_left}={count_left:,}, {source_right}={count_right:,}, diff={diff:+,}")
    else:
        print(f"\nMismatched: (none)")

    # Misload (one-sided dates within overlap range)
    if misload_left or misload_right:
        print(f"\nMisload ({len(misload_left) + len(misload_right)} dates):")
        for dt, count in misload_left:
            print(f"  {dt}: {source_left}={count:,}, {source_right}=0, diff={count:+,}")
        for dt, count in misload_right:
            print(f"  {dt}: {source_left}=0, {source_right}={count:,}, diff={-count:+,}")

    # Only in left (outside overlap)
    if outside_left:
        print(f"\nOnly in {source_left} ({len(outside_left)} dates):")
        for dt, count in outside_left[:5]:
            print(f"  {dt}: {count:,}")
        if len(outside_left) > 5:
            print(f"  ... ({len(outside_left) - 5} more)")
    else:
        print(f"\nOnly in {source_left}: (none)")

    # Only in right (outside overlap)
    if outside_right:
        print(f"\nOnly in {source_right} ({len(outside_right)} dates):")
        for dt, count in outside_right[:5]:
            print(f"  {dt}: {count:,}")
        if len(outside_right) > 5:
            print(f"  ... ({len(outside_right) - 5} more)")
    else:
        print(f"\nOnly in {source_right}: (none)")

    # Matching
    print(f"\nMatching ({len(result['matching'])} dates):")
    if result["matching"]:
        for dt, count in result["matching"][:3]:
            print(f"  {dt}: {count:,}")
        if len(result["matching"]) > 3:
            print(f"  ... ({len(result['matching']) - 3} more)")
    else:
        print("  (none)")

    # Save results to _row_comparison if pair_name provided
    if pair_name:
        matching_dates = [dt for dt, _ in result['matching']]
        excluded_dates = [dt for dt, _, _ in result['mismatched']]
        excluded_dates += [dt for dt, _ in misload_left]
        excluded_dates += [dt for dt, _ in misload_right]
        save_row_comparison(
            db_path, pair_name,
            overlap_start, overlap_end,
            matching_dates, excluded_dates,
        )
        print(f"  Saved comparison results for pair '{pair_name}'")

    print()

    return result


def _build_where_from_dates(table_cfg, matching_dates, excluded_dates, db_path=None):
    """Build WHERE clause from matching and excluded dates."""
    date_col = table_cfg.get('date_col', 'dt')

    # Look up date column data type from _column_meta
    date_dtype = ''
    if db_path:
        from .db import get_column_meta
        from .extract import _qualified_name
        qn = _qualified_name(table_cfg)
        col_meta = get_column_meta(db_path, qn)
        for cm in col_meta:
            if cm['column_name'].upper() == date_col.upper():
                date_dtype = (cm.get('data_type') or '').upper()
                break

    def fmt_date(d):
        is_yyyymm = bool(re.match(r'^\d{6}$', str(d)))
        if is_yyyymm:
            if date_dtype.startswith(('NUMBER', 'INTEGER', 'INT', 'BIGINT')):
                return str(d)
            else:
                return f"'{d}'"
        else:
            if date_dtype.startswith(('VARCHAR', 'CHAR', 'STRING', 'TEXT')):
                return f"'{d}'"
            elif date_dtype.startswith(('DATE', 'TIMESTAMP')):
                return f"DATE '{d}'"
            else:
                return f"'{d}'"

    # Build WHERE clause
    parts = []

    # Add original WHERE from config
    orig_where = table_cfg.get('where', '').strip()
    if orig_where:
        parts.append(f"({orig_where})")

    # Add date range if we have matching dates
    if matching_dates:
        min_date = min(matching_dates)
        max_date = max(matching_dates)
        parts.append(f"{date_col} >= {fmt_date(min_date)}")
        parts.append(f"{date_col} <= {fmt_date(max_date)}")

        # Add NOT IN for excluded dates
        if excluded_dates:
            excluded_list = ", ".join(fmt_date(d) for d in excluded_dates)
            parts.append(f"{date_col} NOT IN ({excluded_list})")

    return " AND ".join(parts) if parts else ""


def cmd_compare_row(args):
    """Compare row counts between two tables"""
    if not os.path.exists(args.project_db):
        print(f"Error: Database not found: {args.project_db}")
        sys.exit(1)

    config_path = getattr(args, 'config', None)
    yes = getattr(args, 'yes', False)
    html_path = getattr(args, 'html', None)

    # Collect (pair_name, source_left, source_right, table_left, table_right, result) for HTML
    html_entries = []

    if config_path:
        import json
        if not os.path.exists(config_path):
            print(f"Error: Config file not found: {config_path}")
            sys.exit(1)
        with open(config_path, 'r') as f:
            config = json.load(f)

        # Detect config format
        is_unified = "pairs" in config and isinstance(config["pairs"], dict)

        if is_unified:
            # Unified format
            from .config import set_pair_where_map
            from .extract import _qualified_name

            pairs_to_process = []
            for pair_name, pair_config in config["pairs"].items():
                left = pair_config["left"]
                right = pair_config["right"]
                table_left = _qualified_name(left)
                table_right = _qualified_name(right)
                source_left = left.get("source", "left")
                source_right = right.get("source", "right")
                col_map = pair_config.get("col_map", {})
                pairs_to_process.append((pair_name, table_left, table_right, source_left, source_right, col_map))

            # When using --config, compare all pairs automatically (no per-pair prompts)
            # Use -y flag only for backward compatibility
            for pair_name, table_left, table_right, source_left, source_right, col_map in pairs_to_process:

                # Auto-register pair
                register_table_pair(
                    args.project_db, pair_name,
                    table_left, table_right,
                    source_left=source_left,
                    source_right=source_right,
                    col_mappings=col_map if col_map else None,
                )

                result = _print_row_comparison(
                    args.project_db, table_left, table_right,
                    source_left, source_right,
                    from_date=args.from_date, to_date=args.to_date,
                    pair_name=pair_name,
                )

                html_entries.append((pair_name, source_left, source_right, table_left, table_right, result, {}))

        else:
            # Old format (list of pairs) - compare all automatically when using --config
            for pair_cfg in config.get('pairs', []):
                pair_name = pair_cfg['name']
                tables = pair_cfg.get('tables', {})
                sources = list(tables.keys())
                if len(sources) < 2:
                    print(f"Warning: Pair '{pair_name}' needs 2 tables, skipping")
                    continue
                source_left, source_right = sources[0], sources[1]
                table_left = tables[source_left]['table_name']
                table_right = tables[source_right]['table_name']

                # Auto-register pair
                col_map = pair_cfg.get('col_map', {})
                register_table_pair(
                    args.project_db, pair_name,
                    table_left, table_right,
                    source_left=source_left,
                    source_right=source_right,
                    col_mappings=col_map if col_map else None,
                )

                result = _print_row_comparison(
                    args.project_db, table_left, table_right,
                    source_left, source_right,
                    from_date=args.from_date, to_date=args.to_date,
                    pair_name=pair_name,
                )

                # Auto-generate where_map from comparison results (old format - SQL style)
                where_map = {}
                comp = get_row_comparison(args.project_db, pair_name)
                if comp and comp.get('overlap_start') and comp.get('overlap_end'):
                    excluded = comp.get('excluded_dates', [])
                    for src, tbl in [(source_left, table_left), (source_right, table_right)]:
                        meta = get_metadata(args.project_db, tbl)
                        date_var = (meta.get('date_var') or '') if meta else ''
                        source_type = (meta.get('source') or '').lower() if meta else ''
                        if not date_var:
                            continue

                        # Get date column data type from _column_meta
                        col_meta = get_column_meta(args.project_db, tbl)
                        date_dtype = ''
                        for cm in col_meta:
                            if cm['column_name'].lower() == date_var.lower():
                                date_dtype = (cm.get('data_type') or '').upper()
                                break

                        # Format date literals based on source/type
                        def _fmt_date(d, _src=source_type, _dt=date_dtype):
                            import re
                            is_yyyymm = bool(re.match(r'^\d{6}$', str(d)))

                            if is_yyyymm:
                                if _dt.startswith(('NUMBER', 'INTEGER', 'INT', 'BIGINT')):
                                    return str(d)
                                else:
                                    return f"'{d}'"
                            else:
                                if _dt.startswith(('VARCHAR', 'CHAR', 'STRING', 'TEXT')):
                                    return f"'{d}'"
                                elif _dt.startswith(('DATE', 'TIMESTAMP')):
                                    return f"DATE '{d}'"
                                else:
                                    return f"'{d}'"

                        parts = []
                        # Prepend original WHERE from extract config (stored in _metadata)
                        original_where = (meta.get('where_clause') or '').strip()
                        if original_where:
                            parts.append(original_where)
                        parts.append(f"{date_var} >= {_fmt_date(comp['overlap_start'])}")
                        parts.append(f"{date_var} <= {_fmt_date(comp['overlap_end'])}")
                        if excluded:
                            excluded_list = ", ".join(_fmt_date(d) for d in excluded)
                            parts.append(f"{date_var} NOT IN ({excluded_list})")
                        where_map[src] = ' AND '.join(parts)

                    pair_cfg['where_map'] = where_map

                html_entries.append((pair_name, source_left, source_right, table_left, table_right, result, where_map))

        # Automatically save to config when using --config
        if config_path and is_unified:
            print("\n💾 Saving to config file...")

            # Update where_map for unified format
            if is_unified:
                from .config import set_pair_where_map
                from datetime import datetime
                for pair_name, source_left, source_right, table_left, table_right, result, where_map in html_entries:
                    comp = get_row_comparison(args.project_db, pair_name)
                    if comp:
                        matching = comp.get('matching_dates', [])
                        excluded = comp.get('excluded_dates', [])

                        # Generate WHERE statements from dates
                        left_cfg = config['pairs'][pair_name]['left']
                        right_cfg = config['pairs'][pair_name]['right']
                        where_left = _build_where_from_dates(left_cfg, matching, excluded, db_path=args.project_db)
                        where_right = _build_where_from_dates(right_cfg, matching, excluded, db_path=args.project_db)

                        set_pair_where_map(config, pair_name, where_left, where_right)

                        # Save WHERE clauses to database immediately
                        from .db import generic_update
                        generic_update(
                            args.project_db,
                            "_row_comparison",
                            {"pair_name": pair_name},
                            {"where_left": where_left, "where_right": where_right}
                        )

                        # Add time_map placeholder to metadata
                        if 'metadata' not in config['pairs'][pair_name]:
                            config['pairs'][pair_name]['metadata'] = {}

                        config['pairs'][pair_name]['metadata']['last_comparison'] = datetime.now().isoformat()

                        # Initialize time_map with placeholders for left and right sources
                        if 'time_map' not in config['pairs'][pair_name]['metadata']:
                            config['pairs'][pair_name]['metadata']['time_map'] = {
                                source_left: "—",
                                source_right: "—"
                            }

                        print(f"  ✓ {pair_name}: {len(matching)} matching dates")

            # Write updated config back
            from .config import save_unified_config
            try:
                if is_unified:
                    save_unified_config(config, config_path)
                else:
                    with open(config_path, 'w') as f:
                        json.dump(config, f, indent=2)
                print(f"\n✓ Saved to: {config_path}")

                # Pause for manual review and editing
                print("\n" + "="*80)
                print("📝 Review and edit the config file now")
                print("="*80)
                print(f"  File: {config_path}")
                print()
                print("  You can now:")
                print("  - Add time_map values (check ./sas/_timing.csv and ./csv/_timing.csv)")
                print("    Format: \"time_map\": {\"pcds\": \"5.2s\", \"aws\": \"3.1s\"}")
                print("  - Review where_map")
                print("  - Add notes to metadata")
                print()
                input("Press Enter when done editing...")

                # Reload config file with user's edits
                print("\n🔄 Reloading config from", config_path)
                with open(config_path, 'r') as f:
                    config = json.load(f)

                # Sync to database (where_map WHERE statements are for display only)
                # The actual date arrays are already in the database from compare-row
                print("\n💾 Syncing to database...")
                from .db import sync_config_to_db
                sync_config_to_db(args.project_db, config)
                print(f"✓ Synced to database: {args.project_db}")

                # Show what was synced
                if is_unified:
                    for pair_name in config.get("pairs", {}):
                        time_map = config["pairs"][pair_name].get("metadata", {}).get("time_map", {})
                        if time_map:
                            times = [f"{k}={v}" for k, v in time_map.items() if v and v != "—"]
                            if times:
                                print(f"  - {pair_name}: time_map = {{{', '.join(times)}}}")

                print(f"\n📄 Your edits are saved in: {config_path}")
                print("   (File was NOT overwritten after sync)")

            except Exception as e:
                print(f"Warning: Could not save config: {e}")

    elif args.pair:
        pair = get_table_pair(args.project_db, args.pair)
        if not pair:
            print(f"Error: Pair '{args.pair}' not found")
            sys.exit(1)
        result = _print_row_comparison(
            args.project_db, pair["table_left"], pair["table_right"],
            pair.get("source_left", "left"), pair.get("source_right", "right"),
            from_date=args.from_date, to_date=args.to_date,
            pair_name=args.pair,
        )
        html_entries.append((args.pair, pair.get("source_left", "left"), pair.get("source_right", "right"),
                            pair["table_left"], pair["table_right"], result, {}))
    elif args.table_left and args.table_right:
        result = _print_row_comparison(
            args.project_db, args.table_left, args.table_right,
            "left", "right",
            from_date=args.from_date, to_date=args.to_date,
        )
        html_entries.append(("adhoc", "left", "right", args.table_left, args.table_right, result, {}))
    else:
        # Compare all pairs
        pairs = list_table_pairs(args.project_db)
        if not pairs:
            print("No table pairs registered. Use 'dtrack load-map' or 'dtrack compare-row --config' to register pairs.")
            sys.exit(1)
        for pair in pairs:
            result = _print_row_comparison(
                args.project_db, pair["table_left"], pair["table_right"],
                pair.get("source_left", "left"), pair.get("source_right", "right"),
                from_date=args.from_date, to_date=args.to_date,
                pair_name=pair["pair_name"],
            )
            html_entries.append((pair["pair_name"], pair.get("source_left", "left"), pair.get("source_right", "right"),
                                pair["table_left"], pair["table_right"], result, {}))

    # Generate HTML report if requested
    if html_path and html_entries:
        from .html_export import generate_row_count_html, create_row_count_table, wrap_html_document

        row_sections = []
        for pair_name, src_l, src_r, tbl_l, tbl_r, comp_result, where_map in html_entries:
            meta_l = get_metadata(args.project_db, tbl_l)
            meta_r = get_metadata(args.project_db, tbl_r)

            # Read time_map from database or config
            time_map = {}
            row_comp = get_row_comparison(args.project_db, pair_name)
            if row_comp and row_comp.get('time_map'):
                # Database stores time_map as JSON
                import json
                if isinstance(row_comp['time_map'], str):
                    time_map = json.loads(row_comp['time_map'])
                else:
                    time_map = row_comp['time_map']
            elif config_path and is_unified and pair_name in config.get('pairs', {}):
                time_map = config['pairs'][pair_name].get('metadata', {}).get('time_map', {})

            section = generate_row_count_html(
                pair_name, src_l, src_r, tbl_l, tbl_r,
                comp_result, metadata_left=meta_l, metadata_right=meta_r,
                where_map=where_map,
                time_map=time_map,
            )
            row_sections.append(section)

        table_html = create_row_count_table(row_sections)

        # Read title/subtitle from config metadata if available
        if config_path and is_unified:
            global_meta = config.get('metadata', {})
            default_title = global_meta.get('title') or "Row Count Comparison"
            default_subtitle = global_meta.get('subtitle') or "updates every Thursday"
        else:
            default_title = "Row Count Comparison"
            default_subtitle = "updates every Thursday"

        doc = wrap_html_document(
            getattr(args, 'title', None) or default_title,
            [table_html],
            subtitle=getattr(args, 'subtitle', None) or default_subtitle,
        )

        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(doc)
        print(f"HTML report: {html_path}")


def cmd_compare_col(args):
    """Compare column statistics between two tables"""
    if not os.path.exists(args.project_db):
        print(f"Error: Database not found: {args.project_db}")
        sys.exit(1)

    config_path = getattr(args, 'config', None)
    html_path = getattr(args, 'html', None)

    # Batch mode: compare all pairs from config
    if config_path:
        import json
        if not os.path.exists(config_path):
            print(f"Error: Config file not found: {config_path}")
            sys.exit(1)

        with open(config_path, 'r') as f:
            config = json.load(f)

        # Detect config format
        is_unified = "pairs" in config and isinstance(config["pairs"], dict)

        if not is_unified:
            print("Error: compare-col --config requires unified config format")
            sys.exit(1)

        from .config import get_all_tables_from_unified
        from .extract import _qualified_name

        # Collect results for HTML generation
        html_entries = []

        for pair_name, pair_config in config["pairs"].items():
            left = pair_config["left"]
            right = pair_config["right"]
            table_left = _qualified_name(left)
            table_right = _qualified_name(right)
            source_left = left.get("source", "left")
            source_right = right.get("source", "right")
            col_mappings = pair_config.get("col_map", {})

            # Auto-register pair
            register_table_pair(
                args.project_db, pair_name,
                table_left, table_right,
                source_left=source_left,
                source_right=source_right,
                col_mappings=col_mappings if col_mappings else None,
            )

            print(f"\n{'='*70}")
            print(f"Comparing column stats: {pair_name}")
            print(f"  {source_left}: {table_left}")
            print(f"  {source_right}: {table_right}")
            print(f"{'='*70}")

            # Get matched dates from row comparison for filtering
            matched_dates = None
            if not args.no_date_filter:
                row_result = compare_row_counts(
                    args.project_db, table_left, table_right,
                    from_date=args.from_date, to_date=args.to_date,
                )
                matched_dates = {dt for dt, _ in row_result['matching']}
                if matched_dates:
                    print(f"Filtering to {len(matched_dates)} matching dates from row comparison")

            # Compare column statistics
            result = compare_column_stats(
                args.project_db, table_left, table_right,
                columns=None,
                col_mappings=col_mappings,
                from_date=args.from_date, to_date=args.to_date,
                matched_dates=matched_dates,
            )

            if result:
                _print_col_comparison(result, source_left, source_right)

                # Classify columns as matched or differing
                from .compare import _has_col_differences
                matched_cols = []
                diff_cols = []

                for col_name, comparisons in result.items():
                    if any(_has_col_differences(comp) for comp in comparisons):
                        diff_cols.append(col_name)
                    else:
                        matched_cols.append(col_name)

                # Save comparison results to database
                from .db import save_col_comparison
                save_col_comparison(
                    args.project_db, pair_name,
                    columns_compared=list(result.keys()),
                    matched_columns=matched_cols,
                    diff_columns=diff_cols,
                    comparison_details=result,
                )

                html_entries.append((pair_name, source_left, source_right, table_left, table_right, result, col_mappings))
            else:
                print("  No matching columns found")

            print()

        # Generate HTML report if requested
        if html_path and html_entries:
            from .html_export import generate_column_stats_html, create_column_stats_table, wrap_html_document

            col_sections = []
            for pair_name, src_l, src_r, tbl_l, tbl_r, comp_result, col_map in html_entries:
                meta_l = get_metadata(args.project_db, tbl_l)
                meta_r = get_metadata(args.project_db, tbl_r)
                section = generate_column_stats_html(
                    pair_name, src_l, src_r, tbl_l, tbl_r,
                    comp_result, col_map,
                    metadata_left=meta_l,
                    metadata_right=meta_r,
                )
                col_sections.append(section)

            vintage = getattr(args, 'vintage', 'day')
            table_html = create_column_stats_table(col_sections, vintage=vintage)

            # Read title/subtitle from config metadata
            global_meta = config.get('metadata', {})
            default_title = global_meta.get('col_title') or "Column Statistics Comparison"
            default_subtitle = global_meta.get('col_subtitle') or "updates first Thursday of every month"

            doc = wrap_html_document(
                getattr(args, 'title', None) or default_title,
                [table_html],
                subtitle=getattr(args, 'subtitle', None) or default_subtitle,
            )

            with open(html_path, 'w') as f:
                f.write(doc)
            print(f"HTML report: {html_path}")

        return

    # Single pair or ad-hoc mode
    # Determine table names and column mapping
    if args.pair:
        pair = get_table_pair(args.project_db, args.pair)
        if not pair:
            print(f"Error: Pair '{args.pair}' not found")
            sys.exit(1)
        table_left = pair["table_left"]
        table_right = pair["table_right"]
        source_left = pair.get("source_left", "left")
        source_right = pair.get("source_right", "right")
        col_mappings = pair.get("col_mappings", {})
    else:
        if not args.table_left or not args.table_right:
            print("Error: Either --pair or both table names must be specified")
            sys.exit(1)
        table_left = args.table_left
        table_right = args.table_right
        source_left = "left"
        source_right = "right"
        col_mappings = {}

    # Override with --col-map if provided
    if args.col_map:
        col_mappings = parse_col_map_string(args.col_map)

    # Parse columns if provided
    columns = None
    if args.columns:
        columns = [c.strip() for c in args.columns.split(',')]

    print(f"\nComparing column stats: {table_left} vs {table_right}")
    if args.from_date or args.to_date:
        date_range = f"{args.from_date or 'beginning'} to {args.to_date or 'end'}"
        print(f"Date range: {date_range}")
    if col_mappings:
        print(f"Column mapping: {len(col_mappings)} columns mapped")

    # Auto-filter to matched dates when --pair is used (unless --no-date-filter)
    matched_dates = None
    if args.pair and not args.no_date_filter:
        row_result = compare_row_counts(
            args.project_db, table_left, table_right,
            from_date=args.from_date, to_date=args.to_date,
        )
        all_common = len(row_result['matching']) + len(row_result['mismatched'])
        matched_dates = {dt for dt, _ in row_result['matching']}
        excluded = len(row_result['mismatched']) + len(row_result['only_left']) + len(row_result['only_right'])
        total = all_common + len(row_result['only_left']) + len(row_result['only_right'])
        print(f"Filtering to {len(matched_dates)}/{total} dates (excluding {excluded} mismatched)")

    print("=" * 70)

    # Vintage windowing
    vintage = getattr(args, 'vintage', None)
    if vintage:
        from .date_utils import bucket_date
        dates_to_use = matched_dates if matched_dates is not None else None

        # If no matched_dates computed yet, get all common dates
        if dates_to_use is None:
            from .db import get_col_stats as _gcs
            stats_l = _gcs(args.project_db, table_left, from_date=args.from_date, to_date=args.to_date)
            stats_r = _gcs(args.project_db, table_right, from_date=args.from_date, to_date=args.to_date)
            dates_to_use = {s['dt'] for s in stats_l} & {s['dt'] for s in stats_r}

        # Group into vintage buckets
        buckets = {}
        for dt in sorted(dates_to_use):
            bucket = bucket_date(dt, vintage)
            buckets.setdefault(bucket, set()).add(dt)

        for bucket_key in sorted(buckets):
            bucket_dates = buckets[bucket_key]
            print(f"\n=== {bucket_key} ({len(bucket_dates)} matched dates) ===")

            result = compare_column_stats(
                args.project_db, table_left, table_right,
                columns=columns, col_mappings=col_mappings,
                from_date=args.from_date, to_date=args.to_date,
                matched_dates=bucket_dates,
            )

            if not result:
                print("  No matching columns found")
                continue

            _print_col_comparison(result, source_left, source_right)

        print()
        return

    # Non-vintage comparison
    result = compare_column_stats(
        args.project_db, table_left, table_right,
        columns=columns, col_mappings=col_mappings,
        from_date=args.from_date, to_date=args.to_date,
        matched_dates=matched_dates,
    )

    if not result:
        print("\nNo matching columns found for comparison")
        return

    _print_col_comparison(result, source_left, source_right)
    print()


def _print_col_comparison(result, source_left, source_right):
    """Print column comparison results."""
    for col_name, comparisons in result.items():
        if not comparisons:
            continue

        first = comparisons[0]
        col_type = first["col_type"]
        left_col = first["left_col"]
        right_col = first["right_col"]

        print(f"\nColumn: {left_col} ({col_type})")
        if left_col != right_col:
            print(f"  {source_left}: {left_col} → {source_right}: {right_col}")
        print("-" * 70)

        if col_type == "numeric":
            # Determine which columns have any differences
            has_n_total_diff = any(comp.get('n_total_diff', 0) != 0 for comp in comparisons)
            has_n_miss_diff = any(comp.get('n_missing_diff', 0) != 0 for comp in comparisons)
            has_mean_diff = any(comp.get('mean_diff') is not None and abs(comp.get('mean_diff', 0)) > 0.01 for comp in comparisons)
            has_std_diff = any(comp.get('std_diff') is not None and abs(comp.get('std_diff', 0)) > 0.01 for comp in comparisons)

            # Build header with only differing columns
            headers = ["Date"]
            if has_n_total_diff:
                headers.append("n_total")
            if has_n_miss_diff:
                headers.append("n_miss")
            if has_mean_diff:
                headers.append("mean")
            if has_std_diff:
                headers.append("std")

            if len(headers) == 1:
                print("  ✓ All statistics match")
                continue

            # Print header
            header_line = f"{'Date':<12} " + " ".join(f"{h:<20}" for h in headers[1:])
            print(header_line)
            print("-" * 70)

            for comp in comparisons[:5]:
                row_parts = [f"{comp['dt']:<12}"]

                if has_n_total_diff:
                    row_parts.append(f"{comp['n_total_left']:,} / {comp['n_total_right']:,} ({comp['n_total_diff']:+,})")
                if has_n_miss_diff:
                    row_parts.append(f"{comp['n_missing_left']:,} / {comp['n_missing_right']:,} ({comp['n_missing_diff']:+,})")
                if has_mean_diff:
                    if comp['mean_left'] is not None and comp['mean_right'] is not None:
                        row_parts.append(f"{comp['mean_left']:.1f} / {comp['mean_right']:.1f} ({comp['mean_diff']:+.1f})")
                    else:
                        row_parts.append("N/A")
                if has_std_diff:
                    if comp['std_left'] is not None and comp['std_right'] is not None:
                        row_parts.append(f"{comp['std_left']:.1f} / {comp['std_right']:.1f} ({comp['std_diff']:+.1f})")
                    else:
                        row_parts.append("N/A")

                print(" ".join(f"{part:<20}" if i > 0 else part for i, part in enumerate(row_parts)))

            if len(comparisons) > 5:
                print(f"  ... ({len(comparisons) - 5} more dates)")
        else:
            # Determine which columns have any differences
            has_n_total_diff = any(comp.get('n_total_diff', 0) != 0 for comp in comparisons)
            has_n_miss_diff = any(comp.get('n_missing_diff', 0) != 0 for comp in comparisons)
            has_n_uniq_diff = any(comp.get('n_unique_diff', 0) != 0 for comp in comparisons)

            # Build header with only differing columns
            headers = ["Date"]
            if has_n_total_diff:
                headers.append("n_total")
            if has_n_miss_diff:
                headers.append("n_miss")
            if has_n_uniq_diff:
                headers.append("n_unique")

            if len(headers) == 1:
                print("  ✓ All statistics match")
                continue

            # Print header
            header_line = f"{'Date':<12} " + " ".join(f"{h:<20}" for h in headers[1:])
            print(header_line)
            print("-" * 70)

            for comp in comparisons[:5]:
                row_parts = [f"{comp['dt']:<12}"]

                if has_n_total_diff:
                    row_parts.append(f"{comp['n_total_left']:,} / {comp['n_total_right']:,} ({comp['n_total_diff']:+,})")
                if has_n_miss_diff:
                    row_parts.append(f"{comp['n_missing_left']:,} / {comp['n_missing_right']:,} ({comp['n_missing_diff']:+,})")
                if has_n_uniq_diff:
                    row_parts.append(f"{comp['n_unique_left']:,} / {comp['n_unique_right']:,} ({comp['n_unique_diff']:+,})")

                print(" ".join(f"{part:<20}" if i > 0 else part for i, part in enumerate(row_parts)))

            if len(comparisons) > 5:
                print(f"  ... ({len(comparisons) - 5} more dates)")

            # Show top_10 differences if they exist
            has_top10_diff = any(comp.get('top_10_left') != comp.get('top_10_right') for comp in comparisons)
            if has_top10_diff:
                print()
                print("  Top 10 value differences detected:")
                import json
                for comp in comparisons[:3]:  # Show first 3 dates with top_10
                    top10_left = comp.get('top_10_left')
                    top10_right = comp.get('top_10_right')
                    if top10_left != top10_right and top10_left and top10_right:
                        try:
                            left_vals = json.loads(top10_left) if isinstance(top10_left, str) else top10_left
                            right_vals = json.loads(top10_right) if isinstance(top10_right, str) else top10_right
                            print(f"    {comp['dt']}: {len(left_vals)} vs {len(right_vals)} unique values (see HTML for details)")
                        except:
                            pass


def cmd_show_stats(args):
    """Show column statistics"""
    if not os.path.exists(args.project_db):
        print(f"Error: Database not found: {args.project_db}")
        sys.exit(1)

    stats = get_col_stats(
        args.project_db,
        args.table,
        column_name=args.column,
        from_date=args.from_date,
        to_date=args.to_date,
        limit=args.limit,
    )

    if not stats:
        print(f"No statistics found for table: {args.table}")
        return

    # Show metadata
    metadata = get_metadata(args.project_db, args.table)
    if metadata:
        print(f"\nTable: {args.table}")
        print(f"Source: {metadata.get('source', 'N/A')}")
        print(f"Database: {metadata.get('db', 'N/A')}")
        print()

    # Group by column
    from itertools import groupby

    stats.sort(key=lambda x: (x['column_name'], x['dt']))

    for column_name, group in groupby(stats, key=lambda x: x['column_name']):
        group_list = list(group)
        col_type = group_list[0]['col_type']

        print(f"\nColumn: {column_name} ({col_type})")
        print("-" * 80)

        if col_type == 'numeric':
            print(f"{'Date':<12} {'n_total':>10} {'n_missing':>10} {'n_unique':>10} {'mean':>12} {'std':>12}")
            print("-" * 80)
            for s in group_list:
                mean_str = f"{s['mean']:.2f}" if s['mean'] is not None else "N/A"
                std_str = f"{s['std']:.2f}" if s['std'] is not None else "N/A"
                print(f"{s['dt']:<12} {s['n_total']:>10} {s['n_missing']:>10} {s['n_unique']:>10} {mean_str:>12} {std_str:>12}")
        else:
            print(f"{'Date':<12} {'n_total':>10} {'n_missing':>10} {'n_unique':>10} {'min':<15} {'max':<15}")
            print("-" * 80)
            for s in group_list:
                min_val = s['min_val'] if s['min_val'] else "N/A"
                max_val = s['max_val'] if s['max_val'] else "N/A"
                print(f"{s['dt']:<12} {s['n_total']:>10} {s['n_missing']:>10} {s['n_unique']:>10} {min_val:<15} {max_val:<15}")


def cmd_gen_sas(args):
    """Generate SAS extraction files from config"""
    from .extract import gen_sas

    if not os.path.exists(args.config):
        print(f"Error: Config file not found: {args.config}")
        sys.exit(1)

    env_path = getattr(args, 'env', None)
    if env_path and not os.path.exists(env_path):
        print(f"Error: .env file not found: {env_path}")
        sys.exit(1)

    types = [args.type] if args.type != "both" else ["row", "col"]

    print(f"Generating SAS files from: {args.config}")
    print(f"Output directory: {args.outdir}")
    print(f"Types: {', '.join(types)}")
    print()

    db_path = getattr(args, 'db_path', None)
    vintage = getattr(args, 'vintage', 'day')
    gen_sas(args.config, args.outdir, types=types, env_path=env_path, db_path=db_path, vintage=vintage)


def cmd_gen_aws(args):
    """Generate/extract data from AWS Athena"""
    if not os.path.exists(args.config):
        print(f"Error: Config file not found: {args.config}")
        sys.exit(1)

    db_path = getattr(args, 'db_path', None)

    from .extract import extract_aws
    types = [args.type] if args.type != "both" else ["row", "col"]
    print(f"Extracting from AWS Athena using: {args.config}")
    print(f"Output directory: {args.outdir}")
    print(f"Types: {', '.join(types)}")
    print(f"Workers: {args.workers}")
    print()
    vintage = getattr(args, 'vintage', 'day')
    extract_aws(args.config, args.outdir, types=types, max_workers=args.workers, db_path=db_path, vintage=vintage)


def cmd_load_col_stats(args):
    """Load pre-computed column statistics from CSV or folder"""
    if not os.path.exists(args.project_db):
        print(f"Error: Database not found: {args.project_db}")
        print(f"Run: dtrack init {args.project_db}")
        sys.exit(1)

    # Check if config is provided for batch processing
    config_path = getattr(args, 'config', None)
    if config_path:
        import json
        if not os.path.exists(config_path):
            print(f"Error: Config file not found: {config_path}")
            sys.exit(1)

        folder_path = getattr(args, 'csv_file', None) or getattr(args, 'file_or_folder', None)
        if not folder_path or not os.path.isdir(folder_path):
            print(f"Error: When using --config, provide a folder path (not a single file)")
            sys.exit(1)

        with open(config_path, 'r') as f:
            config = json.load(f)

        # Check if it's unified format or old format
        if "pairs" in config and isinstance(config["pairs"], dict):
            # Unified format - extract all tables
            from .config import get_all_tables_from_unified
            tables = get_all_tables_from_unified(config)
        else:
            # Old format
            tables = config.get('tables', [])

        from .extract import _qualified_name
        for tbl in tables:
            qname = _qualified_name(tbl)
            csv_path = os.path.join(folder_path, f"{qname}_col.csv")
            if not os.path.exists(csv_path):
                print(f"WARNING: {csv_path} not found, skipping {qname}")
                continue

            print(f"\n--- {qname} ---")
            # Use vintage from config, fall back to 'day'
            table_vintage = tbl.get('vintage', 'day')
            count = load_precomputed_col_stats(
                db_path=args.project_db,
                csv_path=csv_path,
                table_name=qname,
                mode=args.mode,
                source=tbl.get('source'),
                db_name=tbl.get('database') or tbl.get('conn_macro'),
                vintage=table_vintage,
            )
            print(f"  ✓ Loaded {count} stat rows")
        return

    # Single file mode
    csv_file = getattr(args, 'csv_file', None) or getattr(args, 'file_or_folder', None)
    if not csv_file:
        print("Error: CSV file or folder required")
        sys.exit(1)

    if not os.path.exists(csv_file):
        print(f"Error: CSV file not found: {csv_file}")
        sys.exit(1)

    table = getattr(args, 'table', None)
    if not table:
        print("Error: --table is required when loading a single file")
        sys.exit(1)

    print(f"Loading pre-computed column statistics")
    print(f"  Source: {csv_file}")
    print(f"  Table: {table}")
    print(f"  Mode: {args.mode}")

    count = load_precomputed_col_stats(
        db_path=args.project_db,
        csv_path=csv_file,
        table_name=table,
        mode=args.mode,
    )
    print(f"✓ Loaded {count} stat rows")


def cmd_match_columns(args):
    """Match columns between tables - single pair or all pairs from config"""
    import json
    from .extract import match_columns_from_dicts

    if not os.path.exists(args.project_db):
        print(f"Error: Database not found: {args.project_db}")
        sys.exit(1)

    # Mode 1: All pairs from config
    if args.config:
        if not os.path.exists(args.config):
            print(f"Error: Config file not found: {args.config}")
            sys.exit(1)

        with open(args.config, 'r') as f:
            config = json.load(f)

        # Detect config format
        is_unified = "pairs" in config and isinstance(config["pairs"], dict)

        if not config.get('pairs'):
            print("No pairs found in config")
            sys.exit(1)

        pairs_list = list(config['pairs'].items()) if is_unified else [(p.get('name'), p) for p in config['pairs']]
        print(f"Matching columns for {len(pairs_list)} pair(s)...")
        print()

        matched_columns = {}  # Store matched columns for each pair

        for pair_name, pair_config in pairs_list:
            if is_unified:
                # Unified format
                from .extract import _qualified_name
                left = pair_config['left']
                right = pair_config['right']
                left_table = _qualified_name(left)
                right_table = _qualified_name(right)
                left_src = left.get('source', 'left')
                right_src = right.get('source', 'right')
            else:
                # Old format
                pair = pair_config
                pair_name = pair.get('name', '?')
                tables = pair.get('tables', {})

                # Get table names from all sources in the pair
                table_names = []
                for src, tbl_info in tables.items():
                    table_name = tbl_info.get('table_name')
                    if table_name:
                        table_names.append((src, table_name))

                if len(table_names) < 2:
                    print(f"⚠️  Pair '{pair_name}': Need at least 2 tables, skipping")
                    print()
                    continue

                # Match first two tables (typically left vs right)
                left_src, left_table = table_names[0]
                right_src, right_table = table_names[1]

            print(f"=== Pair: {pair_name} ===")
            print(f"  Left:  {left_table} ({left_src})")
            print(f"  Right: {right_table} ({right_src})")
            print()

            left_meta = get_column_meta(args.project_db, left_table)
            right_meta = get_column_meta(args.project_db, right_table)

            if not left_meta:
                print(f"⚠️  No column metadata found for: {left_table}")
                print()
                continue
            if not right_meta:
                print(f"⚠️  No column metadata found for: {right_table}")
                print()
                continue

            left_cols = {m['column_name']: m['data_type'] or '' for m in left_meta}
            right_cols = {m['column_name']: m['data_type'] or '' for m in right_meta}

            # Get existing col_map to exclude already-mapped columns
            replace_mode = getattr(args, 'mode', 'merge') == 'replace'
            existing_col_map = {} if replace_mode else (pair_config.get('col_map', {}) if is_unified else {})

            result = match_columns_from_dicts(left_cols, right_cols, left_table, right_table, outfile=None)

            # Filter out columns that are already in existing col_map
            left_only = result.get(f'{left_table}_only', [])
            right_only = result.get(f'{right_table}_only', [])
            matched = result.get('matched', {})

            # Merge auto-matched with existing col_map
            all_matched = {**matched, **existing_col_map}

            # Remove columns from unmapped lists if they're in existing col_map
            existing_left_cols = set(existing_col_map.keys())
            existing_right_cols = set(existing_col_map.values())
            left_only = [col for col in left_only if col['name'] not in existing_left_cols]
            right_only = [col for col in right_only if col['name'] not in existing_right_cols]

            # Use all_matched for final save
            matched = all_matched

            print()
            print("=" * 100)
            print(f"📝 Column Matching - {pair_name}")
            print("=" * 100)
            print(f"✓ Auto-matched: {len(matched)} columns (case-insensitive)")
            print()

            if left_only or right_only:
                # Build sorted lists for alignment (unmapped only)
                left_unmapped = [(col['name'], col['type']) for col in left_only]
                left_unmapped.sort(key=lambda x: x[0].lower())

                right_unmapped = [(col['name'], col['type']) for col in right_only]
                right_unmapped.sort(key=lambda x: x[0].lower())

                # Build display output
                output_lines = []
                left_width = 40
                right_width = 40

                # Header
                header = f"{'LEFT (' + left_src + ')':<{left_width}}  {'RIGHT (' + right_src + ')':<{right_width}}  STATUS"
                separator = "─" * 100
                output_lines.append(header)
                output_lines.append(separator)

                # Merge and align both lists
                i, j = 0, 0
                while i < len(left_unmapped) or j < len(right_unmapped):
                    left_name, left_type = left_unmapped[i] if i < len(left_unmapped) else ("", "")
                    right_name, right_type = right_unmapped[j] if j < len(right_unmapped) else ("", "")

                    # Determine alignment
                    if not left_name:  # Right only remaining
                        left_display = ""
                        right_display = f"{right_name} ({right_type})"
                        status = "✓ NEW"
                        j += 1
                    elif not right_name:  # Left only remaining
                        left_display = f"{left_name} ({left_type})"
                        right_display = ""
                        status = "✗ DROP"
                        i += 1
                    else:
                        # Compare alphabetically (case-insensitive)
                        cmp = (left_name.lower() > right_name.lower()) - (left_name.lower() < right_name.lower())

                        if cmp < 0:  # Left comes first alphabetically
                            left_display = f"{left_name} ({left_type})"
                            right_display = ""
                            status = "✗ DROP"
                            i += 1
                        else:  # Right comes first alphabetically
                            left_display = ""
                            right_display = f"{right_name} ({right_type})"
                            status = "✓ NEW"
                            j += 1

                    line = f"{left_display:<{left_width}}  {right_display:<{right_width}}  {status}"
                    output_lines.append(line)

                # Display to console
                for line in output_lines:
                    print(line)

                print()
                print(f"📊 Unmapped: {len(left_only)} left-only (✗ DROP), {len(right_only)} right-only (✓ NEW)")
                print()

                # Ask to save
                safe_name = re.sub(r'[^\w\-]', '_', pair_name)
                save_filename = f"{safe_name}_unmapped.txt"
                response = input(f"💾 Save unmapped columns to {save_filename}? (y/n): ").strip().lower()
                if response == 'y':
                    with open(save_filename, 'w') as f:
                        f.write('\n'.join(output_lines))
                    print(f"✅ Saved to {save_filename}")
                print()

            else:
                print("✅ All columns matched!")
                print()

            # Store matched columns for later save
            matched_columns[pair_name] = matched
            print()

        # Ask user before saving col_map to config
        print()
        print("=" * 100)
        print(f"✅ Column matching complete for all {len(pairs_list)} pairs")
        print("=" * 100)

        if matched_columns:
            total_mapped = sum(len(cols) for cols in matched_columns.values())
            print(f"\n💾 Found {total_mapped} auto-matched columns across {len(matched_columns)} pair(s)")

            # Reload config in case user edited during unmapped prompts
            print(f"\n🔄 Reloading config from {args.config}")
            with open(args.config, 'r') as f:
                config = json.load(f)

            # Merge auto-matched columns with existing col_map (preserve user edits)
            for pair_name, matched in matched_columns.items():
                if is_unified:
                    # Only update if col_map is empty or doesn't exist
                    existing_map = config['pairs'][pair_name].get('col_map', {})
                    if not existing_map:
                        config['pairs'][pair_name]['col_map'] = matched
                        print(f"  ✓ {pair_name}: {len(matched)} column mappings (auto-matched)")
                    else:
                        print(f"  ℹ️  {pair_name}: {len(existing_map)} column mappings (keeping existing)")
                else:
                    for pair in config['pairs']:
                        if pair.get('name') == pair_name:
                            existing_map = pair.get('col_map', {})
                            if not existing_map:
                                pair['col_map'] = matched
                            break

            # Write config file
            from .config import save_unified_config
            try:
                if is_unified:
                    save_unified_config(config, args.config)
                else:
                    with open(args.config, 'w') as f:
                        json.dump(config, f, indent=2)
                print(f"\n✓ Saved to: {args.config}")

                # Pause for manual review and editing
                print("\n" + "="*80)
                print("📝 Review and edit the config file now")
                print("="*80)
                print(f"  File: {args.config}")
                print()
                print("  You can now:")
                print("  - Review auto-matched column mappings")
                print("  - Add or remove column mappings manually")
                print("  - Adjust mapping for renamed columns")
                print()
                resp = input("Press Enter when done editing (or 's' to skip reload): ").strip().lower()

                # Reload config file with user's edits
                print(f"\n🔄 Reloading config from {args.config}")
                with open(args.config, 'r') as f:
                    reloaded_config = json.load(f)

                # Verify reload worked
                for pair_name in matched_columns.keys():
                    old_count = len(matched_columns[pair_name])
                    new_count = len(reloaded_config.get('pairs', {}).get(pair_name, {}).get('col_map', {}))
                    if new_count != old_count:
                        print(f"   📝 {pair_name}: {old_count} → {new_count} (edited)")
                    else:
                        print(f"   ✓ {pair_name}: {new_count} (unchanged)")

                config = reloaded_config

                # Sync to database
                print("\n💾 Syncing to database...")
                from .db import sync_config_to_db
                sync_config_to_db(args.project_db, config)
                print(f"✓ Synced to database: {args.project_db}")

                # Show what was synced
                if is_unified:
                    for pair_name in config.get("pairs", {}):
                        col_map = config["pairs"][pair_name].get("col_map", {})
                        if col_map:
                            print(f"  - {pair_name}: {len(col_map)} column mappings")

                print(f"\n📄 Your edits are saved in: {args.config}")
                print("   (File was NOT overwritten after sync)")

            except Exception as e:
                print(f"⚠️  Could not save config: {e}")

        print()
        if left_only or right_only:
            print("💡 Next steps:")
            print(f"   1. Review unmapped columns in the saved .txt files")
            print(f"   2. If needed, manually add renamed column mappings to col_map in {args.config}")
            print(f"   3. Example: \"AMT\": \"amount\" maps left AMT column to right amount column")
        print("=" * 100)

    # Mode 2: Single pair from --left and --right
    elif args.left and args.right:
        left_meta = get_column_meta(args.project_db, args.left)
        right_meta = get_column_meta(args.project_db, args.right)

        if not left_meta:
            print(f"Error: No column metadata found for table: {args.left}")
            sys.exit(1)
        if not right_meta:
            print(f"Error: No column metadata found for table: {args.right}")
            sys.exit(1)

        left_cols = {m['column_name']: m['data_type'] or '' for m in left_meta}
        right_cols = {m['column_name']: m['data_type'] or '' for m in right_meta}

        match_columns_from_dicts(left_cols, right_cols, args.left, args.right, outfile=None)

    else:
        print("Error: Must provide either --config or both --left and --right")
        sys.exit(1)


def _load_columns_entry(project_db, store_name, raw_table, source, conn_macro):
    """Discover and load columns for one table entry.

    Args:
        project_db: Path to database file
        store_name: Name to store under in _column_meta (qualified name)
        raw_table: Actual table name for discovery queries / mock paths
        source: Source identifier (pcds, oracle, aws)
        conn_macro: Oracle macro name or Athena database name
    """
    import csv as csv_mod

    if conn_macro and source.lower() == 'aws':
        mock_dir = os.environ.get('DTRACK_ATHENA_MOCK')
        if mock_dir:
            mock_csv = os.path.join(mock_dir, conn_macro, raw_table, 'columns.csv')
            if not os.path.exists(mock_csv):
                print(f"WARNING: [mock] File not found: {mock_csv}, skipping {store_name}")
                return
            columns = {}
            with open(mock_csv, 'r', newline='') as f:
                reader = csv_mod.DictReader(f)
                for row in reader:
                    name = row.get('column_name') or row.get('COLUMN_NAME', '')
                    dtype = row.get('data_type') or row.get('DATA_TYPE', '')
                    if name:
                        columns[name] = dtype
            print(f"[mock] Loaded {len(columns)} columns from {mock_csv}")
        else:
            from .extract import _discover_columns_athena, athena_connect, aws_creds_renew
            print(f"Connecting to Athena ({conn_macro})...")
            aws_creds_renew()
            conn = athena_connect(data_base=conn_macro)
            cursor = conn.cursor()
            print(f"Discovering columns for: {conn_macro}.{raw_table}")
            columns = _discover_columns_athena(cursor, conn_macro, raw_table)
            cursor.close()
            conn.close()

        if not columns:
            print(f"WARNING: No columns found for {conn_macro}.{raw_table}, skipping {store_name}")
            return

        if not mock_dir:
            print(f"Discovered {len(columns)} columns")

    elif conn_macro:
        from .db import oracle_connect, discover_columns
        print(f"Connecting to Oracle via '{conn_macro}'...")
        conn = oracle_connect(conn_macro)
        print(f"Discovering columns for: {raw_table}")
        columns = discover_columns(conn, raw_table)
        if conn is not None:
            conn.close()

        if not columns:
            print(f"WARNING: No columns found for {raw_table}, skipping {store_name}")
            return

        print(f"Discovered {len(columns)} columns")

    else:
        print(f"WARNING: No conn_macro for {store_name}, skipping")
        return

    count = insert_column_meta(project_db, store_name, columns, source=source)
    print(f"Loaded {count} columns into _column_meta for table: {store_name}")


def cmd_load_columns(args):
    """Load column metadata from CSV, Oracle, or Athena into _column_meta table"""
    import csv

    if not os.path.exists(args.project_db):
        print(f"Error: Database not found: {args.project_db}")
        sys.exit(1)

    config_path = getattr(args, 'config', None)
    if config_path:
        import json
        if not os.path.exists(config_path):
            print(f"Error: Config file not found: {config_path}")
            sys.exit(1)
        with open(config_path, 'r') as f:
            config = json.load(f)

        # Detect config format and extract tables
        if "pairs" in config and isinstance(config["pairs"], dict):
            from .config import get_all_tables_from_unified
            tables = get_all_tables_from_unified(config)
        else:
            tables = config.get('tables', [])

        from .extract import _qualified_name
        for tbl in tables:
            source = tbl.get('source', '')
            qname = _qualified_name(tbl)
            raw_table = tbl['table']
            # For AWS, conn_macro = database name; for Oracle, conn_macro from config
            if source.lower() == 'aws':
                conn_macro = tbl.get('database', '')
            else:
                conn_macro = tbl.get('conn_macro', '')

            print(f"\n--- {qname} ({source}: {raw_table}) ---")
            _load_columns_entry(args.project_db, qname, raw_table, source, conn_macro)
        return

    conn_macro = getattr(args, 'conn_macro', None)
    csv_file = getattr(args, 'csv_file', None)
    source = getattr(args, 'source', None) or ''

    if not args.table:
        print("Error: --table is required unless --config is provided")
        sys.exit(1)

    if csv_file:
        # CSV path
        if not os.path.exists(csv_file):
            print(f"Error: CSV file not found: {csv_file}")
            sys.exit(1)

        columns = {}
        with open(csv_file, 'r', newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                name = row.get('COLUMN_NAME') or row.get('column_name', '')
                dtype = row.get('DATA_TYPE') or row.get('data_type', '')
                if name:
                    columns[name] = dtype

        if not columns:
            print("No columns found in CSV")
            sys.exit(1)

    elif conn_macro and source.lower() == 'aws':
        # Athena discovery (conn_macro = database name)
        mock_dir = os.environ.get('DTRACK_ATHENA_MOCK')
        if mock_dir:
            # Mock mode: read from CSV
            mock_csv = os.path.join(mock_dir, conn_macro, args.table, 'columns.csv')
            if not os.path.exists(mock_csv):
                print(f"[mock] File not found: {mock_csv}")
                sys.exit(1)
            columns = {}
            with open(mock_csv, 'r', newline='') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    name = row.get('column_name') or row.get('COLUMN_NAME', '')
                    dtype = row.get('data_type') or row.get('DATA_TYPE', '')
                    if name:
                        columns[name] = dtype
            print(f"[mock] Loaded {len(columns)} columns from {mock_csv}")
        else:
            from .extract import _discover_columns_athena, athena_connect, aws_creds_renew
            print(f"Connecting to Athena ({conn_macro})...")
            aws_creds_renew()
            conn = athena_connect(data_base=conn_macro)
            cursor = conn.cursor()
            print(f"Discovering columns for: {conn_macro}.{args.table}")
            columns = _discover_columns_athena(cursor, conn_macro, args.table)
            cursor.close()
            conn.close()

        if not columns:
            print(f"No columns found for table: {conn_macro}.{args.table}")
            sys.exit(1)

        if not mock_dir:
            print(f"Discovered {len(columns)} columns")

    elif conn_macro:
        # Live Oracle discovery
        from .db import oracle_connect, discover_columns
        print(f"Connecting to Oracle via '{conn_macro}'...")
        conn = oracle_connect(conn_macro)
        print(f"Discovering columns for: {args.table}")
        columns = discover_columns(conn, args.table)
        if conn is not None:
            conn.close()

        if not columns:
            print(f"No columns found for table: {args.table}")
            sys.exit(1)

        print(f"Discovered {len(columns)} columns")

    else:
        print("Error: Provide a csv_file or --conn-macro (with --source pcds|aws)")
        sys.exit(1)

    count = insert_column_meta(args.project_db, args.table, columns, source=source)
    print(f"Loaded {count} columns into _column_meta for table: {args.table}")


def cmd_ppt_create(args):
    """Create PowerPoint presentation from markdown"""
    markdown_path = args.markdown_file
    output_path = args.output
    template_path = args.template
    config_path = args.config

    if not os.path.exists(markdown_path):
        print(f"Error: Markdown file not found: {markdown_path}")
        sys.exit(1)

    if template_path and not os.path.exists(template_path):
        print(f"Error: Template file not found: {template_path}")
        sys.exit(1)

    if config_path and not os.path.exists(config_path):
        print(f"Error: Config file not found: {config_path}")
        sys.exit(1)

    try:
        result = parse_markdown_to_ppt(
            markdown_path, output_path, template_path, config_path,
        )
        print(f"Presentation created: {result}")
    except Exception as e:
        print(f"Error creating presentation: {e}")
        sys.exit(1)


def cmd_ppt_template(args):
    """Create a PowerPoint template file"""
    from .ppt import PPTBuilder

    output_path = args.output
    builder = PPTBuilder()

    # Add sample slides to demonstrate layouts
    builder.add_title_slide("Template Title", "Subtitle", "Date")
    builder.add_section_slide("Section Title")
    slide = builder.add_content_slide("Content Slide")
    builder.add_bullets(slide, ["Bullet 1", "Bullet 2", "Bullet 3"])

    builder.save(output_path)
    print(f"✅ Template created: {output_path}")


def cmd_query(args):
    """Run a SQL query against the database"""
    import sqlite3

    if not os.path.exists(args.project_db):
        print(f"Error: Database not found: {args.project_db}")
        sys.exit(1)

    sql = args.sql.strip()
    allow_write = getattr(args, 'write', False)

    # Safety: only allow read-only statements unless --write is specified
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
        conn.commit()  # Commit changes for write operations
    except sqlite3.Error as e:
        print(f"SQL error: {e}")
        conn.close()
        sys.exit(1)

    if not rows:
        # For write operations, show affected row count
        if allow_write and cursor.rowcount >= 0:
            print(f"Query executed successfully ({cursor.rowcount} row(s) affected)")
        else:
            print("(no rows)")
        conn.close()
        return

    cols = rows[0].keys()

    # Compute column widths
    col_widths = {}
    for c in cols:
        col_widths[c] = len(c)
        for row in rows:
            col_widths[c] = min(max(col_widths[c], len(str(row[c] or ''))), 40)

    # Print
    header = "  ".join(f"{c:<{col_widths[c]}}" for c in cols)
    sep = "  ".join("─" * col_widths[c] for c in cols)
    print(header)
    print(sep)
    for row in rows:
        line = "  ".join(f"{str(row[c] or ''):<{col_widths[c]}}" for c in cols)
        print(line)

    print(f"\n({len(rows)} row{'s' if len(rows) != 1 else ''})")
    conn.close()


def _parse_assignments(pairs):
    """Parse key=value pairs into a dict."""
    data = {}
    for assignment in pairs:
        if '=' not in assignment:
            print(f"Error: Invalid assignment (expected key=value): {assignment}")
            sys.exit(1)
        key, value = assignment.split('=', 1)
        data[key] = value
    return data


def _preview_matching_rows(db_path, table, where, limit=10):
    """Query and display rows matching WHERE conditions. Returns row count."""
    import sqlite3

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    where_sql, where_params = parse_where_clause(where)
    cursor.execute(
        f"SELECT * FROM {table} WHERE {where_sql}",
        where_params,
    )
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        print(f"  (no matching rows)")
        return 0

    # Get column names from first row
    cols = rows[0].keys()

    # Print header + rows (truncate values for readability)
    col_widths = {c: max(len(c), 6) for c in cols}
    for row in rows[:limit]:
        for c in cols:
            val_len = len(str(row[c] or ''))
            col_widths[c] = min(max(col_widths[c], val_len), 30)

    header = "  ".join(f"{c:<{col_widths[c]}}" for c in cols)
    print(f"  {header}")
    print(f"  {'─' * len(header)}")
    for row in rows[:limit]:
        line = "  ".join(f"{str(row[c] or ''):<{col_widths[c]}}" for c in cols)
        print(f"  {line}")
    if len(rows) > limit:
        print(f"  ... and {len(rows) - limit} more row(s)")

    return len(rows)


def _format_where(where):
    """Format WHERE dict as human-readable string, expanding operators."""
    parts = []
    for col, val in where.items():
        if val.startswith('!~='):
            parts.append(f"{col} NOT LIKE '{val[3:]}'")
        elif val.startswith('~='):
            parts.append(f"{col} LIKE '{val[2:]}'")
        elif val.startswith('!='):
            parts.append(f"{col} != '{val[2:]}'")
        elif ',' in val and not val.startswith("'"):
            items = [v.strip() for v in val.split(',')]
            parts.append(f"{col} IN ({', '.join(repr(v) for v in items)})")
        elif '%' in val:
            parts.append(f"{col} LIKE '{val}'")
        else:
            parts.append(f"{col} = '{val}'")
    return " AND ".join(parts)


def _confirm(prompt="Proceed? [y/N] "):
    """Ask user for confirmation. Returns True if confirmed."""
    resp = input(prompt).strip().lower()
    return resp in ('y', 'yes')


def cmd_upsert(args):
    """Insert, update, or delete records in any table"""
    if not os.path.exists(args.project_db):
        print(f"Error: Database not found: {args.project_db}")
        sys.exit(1)

    identifiers = _parse_assignments(args.assignments)
    if not identifiers:
        print("Error: No key=value pairs provided")
        sys.exit(1)

    updates = _parse_assignments(args.update) if args.update else None
    skip_confirm = args.yes

    # Mode: --delete
    if args.delete:
        where_display = _format_where(identifiers)

        if args.dry_run:
            where_sql, where_params = parse_where_clause(identifiers)
            print(f"[dry-run] DELETE FROM {args.table} WHERE {where_sql}")
            print(f"[dry-run] params: {where_params}")
            return

        print(f"WHERE: {where_display}")
        print(f"\nRows to DELETE from {args.table}:")
        count = _preview_matching_rows(args.project_db, args.table, identifiers)
        if count == 0:
            print("Nothing to delete.")
            return
        if not skip_confirm and not _confirm(f"Delete {count} row(s)? [y/N] "):
            print("Aborted.")
            return

        count = generic_delete(args.project_db, args.table, identifiers)
        print(f"Deleted {count} row(s) from {args.table}")

    # Mode: --update (explicit WHERE + SET)
    elif updates:
        where_display = _format_where(identifiers)

        if args.dry_run:
            set_clause = ", ".join(f"{k}=?" for k in updates)
            where_sql, where_params = parse_where_clause(identifiers)
            print(f"[dry-run] UPDATE {args.table} SET {set_clause} WHERE {where_sql}")
            print(f"[dry-run] params: {list(updates.values()) + where_params}")
            return

        print(f"WHERE: {where_display}")
        print(f"\nRows to UPDATE in {args.table}:")
        count = _preview_matching_rows(args.project_db, args.table, identifiers)
        if count == 0:
            print("No matching rows to update.")
            return
        print(f"\nWill set: {', '.join(f'{k}={v}' for k, v in updates.items())}")
        if not skip_confirm and not _confirm(f"Update {count} row(s)? [y/N] "):
            print("Aborted.")
            return

        count = generic_update(args.project_db, args.table, identifiers, updates)
        print(f"Updated {count} row(s) in {args.table}")

    # Mode: upsert (smart insert-or-update by PK)
    else:
        if args.dry_run:
            from .db import _get_table_schema
            import sqlite3
            conn = sqlite3.connect(args.project_db)
            cursor = conn.cursor()
            all_cols, pk_cols = _get_table_schema(cursor, args.table)
            non_pk = [k for k in identifiers if k not in pk_cols]
            if pk_cols:
                where = " AND ".join(f"{c}=?" for c in pk_cols)
                print(f"[dry-run] PK: {pk_cols}")
                if non_pk:
                    print(f"[dry-run] If exists → UPDATE {args.table} SET {', '.join(f'{k}=?' for k in non_pk)} WHERE {where}")
                cols = ", ".join(identifiers.keys())
                print(f"[dry-run] If new    → INSERT INTO {args.table} ({cols}) VALUES ({', '.join('?' for _ in identifiers)})")
            else:
                cols = ", ".join(identifiers.keys())
                print(f"[dry-run] INSERT INTO {args.table} ({cols}) VALUES ({', '.join('?' for _ in identifiers)})")
            print(f"[dry-run] params: {list(identifiers.values())}")
            conn.close()
            return

        # Check if record exists by PK to show preview
        from .db import _get_table_schema
        import sqlite3
        conn = sqlite3.connect(args.project_db)
        cursor = conn.cursor()
        _, pk_cols = _get_table_schema(cursor, args.table)
        conn.close()

        if pk_cols:
            pk_where = {k: identifiers[k] for k in pk_cols if k in identifiers}
            if pk_where:
                print(f"Existing row in {args.table}:")
                existing = _preview_matching_rows(args.project_db, args.table, pk_where)
                if existing:
                    non_pk = {k: v for k, v in identifiers.items() if k not in pk_cols}
                    if non_pk:
                        print(f"\nWill set: {', '.join(f'{k}={v}' for k, v in non_pk.items())}")
                    else:
                        print("\nRow already exists, nothing to update.")
                        return
                    if not skip_confirm and not _confirm(f"Update this row? [y/N] "):
                        print("Aborted.")
                        return
                else:
                    print(f"\nWill insert: {', '.join(f'{k}={v}' for k, v in identifiers.items())}")
                    if not skip_confirm and not _confirm(f"Insert new row? [y/N] "):
                        print("Aborted.")
                        return

        count = generic_upsert(args.project_db, args.table, identifiers)
        print(f"Upserted {count} row(s) in {args.table}")


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description="dtrack - Data tracking CLI for row counts and column statistics",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--env', dest='env_file', help='Path to .env file for environment variables')
    parser.add_argument('--debug', choices=['ipdb', 'debugpy'], default=None,
                        help='Enable debugging: ipdb for interactive breakpoints, debugpy for VS Code attach on port 5678')

    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # init command
    parser_init = subparsers.add_parser('init', help='Initialize a new database')
    parser_init.add_argument('project_db', help='Path to database file')
    parser_init.add_argument('--force', action='store_true', help='Overwrite existing database')

    # load-row command
    parser_load_row = subparsers.add_parser(
        'load-row',
        help='Load row count data',
        description='Load row count data from CSV files. Expected format: date_value,row_count\n'
                    'File naming: {source}_{table_name}_row.csv (e.g., pcds_cust_daily_row.csv, aws_txn_monthly_row.csv)',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser_load_row.add_argument('project_db', help='Path to database file')
    parser_load_row.add_argument('file_or_folder', help='CSV file or folder with {source}_{table_name}_row.csv files')
    parser_load_row.add_argument('--table', '--table-name', dest='table_name', help='Table name (defaults to filename)')
    parser_load_row.add_argument('--mode', default='upsert', choices=['replace', 'append', 'upsert'],
                                  help='Load mode (default: upsert)')
    parser_load_row.add_argument('--vintage', default='day', choices=['day', 'week', 'month', 'quarter', 'year'],
                                  help='Time granularity (default: day)')
    parser_load_row.add_argument('--source', help='Data source (e.g., aws, pcds, oracle)')
    parser_load_row.add_argument('--db', help='Database or service name')
    parser_load_row.add_argument('--source-table', help='Original table name')
    parser_load_row.add_argument('--date-var', help='Date column name (auto-detected if not provided)')
    parser_load_row.add_argument('--config', help='Extraction config JSON; loads {source}_{table_name}_row.csv from folder for each table')

    # load-map command
    parser_load_map = subparsers.add_parser('load-map', help='Load table pairs from JSON config')
    parser_load_map.add_argument('project_db', help='Path to database file')
    parser_load_map.add_argument('config_file', help='JSON configuration file')
    parser_load_map.add_argument('--type', default='row', choices=['row', 'col'],
                                  help='Data type to load: row (counts) or col (statistics) (default: row)')

    # list command
    parser_list = subparsers.add_parser('list', help='List all tables')
    parser_list.add_argument('project_db', help='Path to database file')

    # list-pairs command
    parser_list_pairs = subparsers.add_parser('list-pairs', help='List all registered table pairs')
    parser_list_pairs.add_argument('project_db', help='Path to database file')
    parser_list_pairs.add_argument('-v', '--verbose', action='store_true', help='Show column mappings')

    # show command
    parser_show = subparsers.add_parser('show', help='Show row count data')
    parser_show.add_argument('project_db', help='Path to database file')
    parser_show.add_argument('table', help='Table name')
    parser_show.add_argument('--limit', type=int, help='Limit number of rows')

    # show-stats command
    parser_show_stats = subparsers.add_parser('show-stats', help='Show column statistics')
    parser_show_stats.add_argument('project_db', help='Path to database file')
    parser_show_stats.add_argument('table', help='Table name')
    parser_show_stats.add_argument('--column', help='Filter by column name')
    parser_show_stats.add_argument('--from-date', help='Start date filter (YYYY-MM-DD)')
    parser_show_stats.add_argument('--to-date', help='End date filter (YYYY-MM-DD)')
    parser_show_stats.add_argument('--limit', type=int, help='Limit number of rows')

    # compare-row command
    parser_compare_row = subparsers.add_parser('compare-row', help='Compare row counts between two tables')
    parser_compare_row.add_argument('project_db', help='Path to database file')
    parser_compare_row.add_argument('--pair', help='Pair name (from load-map)')
    parser_compare_row.add_argument('--table-left', dest='table_left', help='Left table name')
    parser_compare_row.add_argument('--table-right', dest='table_right', help='Right table name')
    parser_compare_row.add_argument('--from-date', help='Start date filter (YYYY-MM-DD)')
    parser_compare_row.add_argument('--to-date', help='End date filter (YYYY-MM-DD)')
    parser_compare_row.add_argument('--config', help='Pairs config JSON (auto-registers pairs and compares)')
    parser_compare_row.add_argument('-y', '--yes', action='store_true',
                                     help='Compare all pairs without prompting (for each pair comparison)')
    parser_compare_row.add_argument('--html', help='Output HTML report file')
    parser_compare_row.add_argument('--title', help='HTML report title (default: "Row Count Comparison")')
    parser_compare_row.add_argument('--subtitle', help='HTML report subtitle (default: "updates every Thursday")')

    # compare-col command
    parser_compare_col = subparsers.add_parser('compare-col', help='Compare column statistics between two tables')
    parser_compare_col.add_argument('project_db', help='Path to database file')
    parser_compare_col.add_argument('--pair', help='Pair name (from load-map)')
    parser_compare_col.add_argument('--table-left', dest='table_left', help='Left table name')
    parser_compare_col.add_argument('--table-right', dest='table_right', help='Right table name')
    parser_compare_col.add_argument('--columns', help='Comma-separated list of columns to compare')
    parser_compare_col.add_argument('--col-map', help='Column mapping: "left1=right1,left2=right2"')
    parser_compare_col.add_argument('--from-date', help='Start date filter (YYYY-MM-DD)')
    parser_compare_col.add_argument('--to-date', help='End date filter (YYYY-MM-DD)')
    parser_compare_col.add_argument('--config', help='Pairs config JSON (auto-registers pairs and compares)')
    parser_compare_col.add_argument('--no-date-filter', action='store_true',
                                     help='Include all dates (skip auto-filtering to matched row counts)')
    parser_compare_col.add_argument('--vintage', choices=['month', 'quarter', 'year'],
                                     help='Group comparison by vintage window')
    parser_compare_col.add_argument('--html', help='Output HTML report file')
    parser_compare_col.add_argument('--title', help='HTML report title (default: "Column Statistics Comparison")')
    parser_compare_col.add_argument('--subtitle', help='HTML report subtitle (default: "updates first Thursday of every month")')

    # gen-sas command
    parser_gen_sas = subparsers.add_parser('gen-sas', help='Generate SAS extraction files from config')
    parser_gen_sas.add_argument('config', help='Extraction config JSON file')
    parser_gen_sas.add_argument('--outdir', default='./sas/', help='Output directory (default: ./sas/)')
    parser_gen_sas.add_argument('--type', default='both', choices=['row', 'col', 'both'],
                                 help='Type to generate (default: both)')
    parser_gen_sas.add_argument('--env', help='Path to .env file with credentials (pcds_usr, pcds_pw, email_to, lib_path)')
    parser_gen_sas.add_argument('--db', dest='db_path', help='Database path to read column metadata from _column_meta')
    parser_gen_sas.add_argument('--vintage', default='day', choices=['day', 'week', 'month', 'quarter', 'year', 'sample'],
                                 help='Date bucketing granularity via Oracle TRUNC (default: day). "sample" picks N random matching dates.')

    # gen-aws command
    parser_gen_aws = subparsers.add_parser('gen-aws', help='Extract data from AWS Athena')
    parser_gen_aws.add_argument('config', help='Extraction config JSON file')
    parser_gen_aws.add_argument('--outdir', default='./csv/', help='Output directory (default: ./csv/)')
    parser_gen_aws.add_argument('--type', default='both', choices=['row', 'col', 'both'],
                                 help='Type to extract (default: both)')
    parser_gen_aws.add_argument('--workers', type=int, default=4, help='Max parallel workers (default: 4)')
    parser_gen_aws.add_argument('--db', dest='db_path',
                                 help='Database path to read column metadata from _column_meta')
    parser_gen_aws.add_argument('--vintage', default='day', choices=['day', 'week', 'month', 'quarter', 'year', 'sample'],
                                 help='Date bucketing granularity via Athena date_trunc (default: day). "sample" picks N random matching dates.')

    # load-col command
    parser_load_col = subparsers.add_parser('load-col', help='Load column statistics from CSV')
    parser_load_col.add_argument('project_db', help='Path to database file')
    parser_load_col.add_argument('file_or_folder', help='CSV file or folder with *_col.csv files')
    parser_load_col.add_argument('--table', help='Table name (required for single file mode)')
    parser_load_col.add_argument('--config', help='Config JSON file (enables batch mode from folder)')
    parser_load_col.add_argument('--mode', default='upsert', choices=['upsert', 'replace'],
                                        help='Load mode (default: upsert)')
    parser_load_col.set_defaults(func=cmd_load_col_stats)

    # load-columns command
    parser_load_cols = subparsers.add_parser('load-columns', help='Load column metadata from CSV, Oracle, or Athena into DB')
    parser_load_cols.add_argument('project_db', help='Path to database file')
    parser_load_cols.add_argument('csv_file', nargs='?', default=None, help='CSV file (optional if --conn-macro or --source aws used)')
    parser_load_cols.add_argument('--table', help='Table name to associate columns with (required unless --config)')
    parser_load_cols.add_argument('--source', help='Source identifier (e.g., pcds, aws)')
    parser_load_cols.add_argument('--conn-macro', help='Connection identifier: Oracle macro (pcds, pb23) or Athena database name')
    parser_load_cols.add_argument('--config', help='Extraction config JSON; discovers columns for each table entry')

    # match-columns command
    parser_match_cols = subparsers.add_parser(
        'match-columns',
        help='Match columns between paired tables',
        description='Match columns between tables. Two modes:\n'
                    '  1. Single pair: --left and --right (display only)\n'
                    '  2. All pairs: --config (updates col_map in config file)\n\n'
                    'Shows: auto-matched (case-insensitive), source-only, and prompts for manual mappings.',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser_match_cols.add_argument('project_db', help='Path to project database')
    parser_match_cols.add_argument('--config', help='Pairs config JSON; match all pairs and update col_map in place')
    parser_match_cols.add_argument('--left', help='Left table name (from _column_meta source_table field)')
    parser_match_cols.add_argument('--right', help='Right table name (from _column_meta source_table field)')
    parser_match_cols.add_argument('--mode', default='merge', choices=['merge', 'replace'],
                                  help='merge: keep existing col_map, add new matches; replace: discard existing col_map (default: merge)')

    # query command
    parser_query = subparsers.add_parser(
        'query',
        help='Run a SQL query against the database',
        description='Run a SQL query and display results as a table.\n\n'
                    'By default, only SELECT, PRAGMA, EXPLAIN, and WITH statements are allowed (read-only).\n'
                    'Use --write to enable write operations (INSERT, UPDATE, DELETE, DROP, etc.).\n\n'
                    'Tables:\n'
                    '  _metadata       Table metadata (source, db, vintage, etc.)\n'
                    '  _row_counts     Row counts by source_table + dt\n'
                    '  _col_stats      Column statistics by source_table + column_name + dt\n'
                    '  _column_meta    Column metadata (name, data_type, source)\n'
                    '  _table_pairs    Paired tables with column mappings\n'
                    '  _row_comparison Row comparison results\n'
                    '  _sample_date    Sampled dates for vintage=sample\n\n'
                    'Examples (read-only):\n'
                    '  dtrack query p.db "SELECT * FROM _metadata"\n'
                    '  dtrack query p.db "SELECT * FROM _row_counts WHERE source_table LIKE \'%%cust%%\'"\n'
                    '  dtrack query p.db "PRAGMA table_info(_metadata)"\n'
                    '  dtrack query p.db "SELECT name FROM sqlite_master WHERE type=\'table\'"\n\n'
                    'Examples (with --write):\n'
                    '  dtrack query p.db "DROP TABLE _sample_date" --write\n'
                    '  dtrack query p.db "DELETE FROM _metadata WHERE vintage=\'test\'" --write\n'
                    '  dtrack query p.db "UPDATE _metadata SET vintage=\'week\' WHERE table_name=\'foo\'" --write',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser_query.add_argument('project_db', help='Path to database file')
    parser_query.add_argument('sql', help='SQL query to execute')
    parser_query.add_argument('--write', action='store_true', help='Allow write operations (INSERT, UPDATE, DELETE, DROP, etc.)')

    # upsert command
    parser_upsert = subparsers.add_parser(
        'upsert',
        help='Insert, update, or delete records in any table',
        description='Insert, update, or delete records in any SQLite table.\n\n'
                    'Modes:\n'
                    '  upsert:  dtrack upsert DB TABLE col=val ...           (insert or update by PK)\n'
                    '  update:  dtrack upsert DB TABLE col=val --update k=v  (WHERE + SET)\n'
                    '  delete:  dtrack upsert DB TABLE col=val --delete      (WHERE conditions)\n\n'
                    'Value operators (for --update/--delete WHERE conditions):\n'
                    '  col=val            exact match\n'
                    '  col=!=val          not equal\n'
                    '  col=~=pattern      LIKE (use %% for wildcard)\n'
                    '  col=!~=pattern     NOT LIKE\n'
                    '  col=val1,val2      IN (comma-separated)\n'
                    '  col=%%pattern%%    LIKE (auto-detected from %% in value)\n\n'
                    'Examples:\n'
                    '  dtrack upsert p.db _metadata table_name=foo source=oracle\n'
                    '  dtrack upsert p.db _metadata table_name=%%_daily --update vintage=week\n'
                    '  dtrack upsert p.db _row_counts --delete source_table=~=pcds_%%\n'
                    '  dtrack upsert p.db _row_counts --delete source_table=tbl1,tbl2',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser_upsert.add_argument('project_db', help='Path to database file')
    parser_upsert.add_argument('table', help='Table name (e.g., _metadata, _row_counts)')
    parser_upsert.add_argument('assignments', nargs='+', help='Column=value pairs (identifiers for --update/--delete, or full row for upsert)')
    parser_upsert.add_argument('--update', nargs='+', metavar='COL=VAL', help='Columns to set (pairs before --update become WHERE conditions)')
    parser_upsert.add_argument('--delete', action='store_true', help='Delete matching rows (pairs become WHERE conditions)')
    parser_upsert.add_argument('--dry-run', action='store_true', help='Show SQL without executing')
    parser_upsert.add_argument('-y', '--yes', action='store_true', help='Skip confirmation prompt')

    # ppt-create command
    parser_ppt_create = subparsers.add_parser('ppt-create', help='Create PowerPoint from markdown')
    parser_ppt_create.add_argument('markdown_file', help='Markdown file to convert')
    parser_ppt_create.add_argument('-o', '--output', required=True, help='Output PowerPoint file')
    parser_ppt_create.add_argument('-t', '--template', help='Template PowerPoint file (optional)')
    parser_ppt_create.add_argument('-c', '--config', help='Layout config JSON file (optional)')

    # ppt-template command
    parser_ppt_template = subparsers.add_parser('ppt-template', help='Create PowerPoint template')
    parser_ppt_template.add_argument('-o', '--output', required=True, help='Output template file')

    args = parser.parse_args()

    # Set up debugger if requested
    if args.debug == 'ipdb':
        import ipdb
        sys.breakpointhook = ipdb.set_trace
        def _ipdb_excepthook(type, value, tb):
            import traceback
            traceback.print_exception(type, value, tb)
            ipdb.post_mortem(tb)
        sys.excepthook = _ipdb_excepthook
    elif args.debug == 'debugpy':
        import debugpy
        debugpy.listen(('0.0.0.0', 5678))
        print('Waiting for debugger attach on port 5678...')
        debugpy.wait_for_client()
        print('Debugger attached.')

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Load config file into os.environ
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

    # Resolve mock paths relative to config file location
    for mock_var in ('DTRACK_ORACLE_MOCK', 'DTRACK_ATHENA_MOCK'):
        mock_dir = os.environ.get(mock_var)
        if mock_dir and not os.path.isabs(mock_dir):
            os.environ[mock_var] = os.path.join(env_dir, mock_dir)

    # Dispatch to command handler
    commands = {
        'init': cmd_init,
        'load-row': cmd_load_row,
        'load-col': cmd_load_col_stats,
        'load-map': cmd_load_map,
        'list': cmd_list,
        'list-pairs': cmd_list_pairs,
        'show': cmd_show,
        'show-stats': cmd_show_stats,
        'compare-row': cmd_compare_row,
        'compare-col': cmd_compare_col,
        'gen-sas': cmd_gen_sas,
        'gen-aws': cmd_gen_aws,
        'load-columns': cmd_load_columns,
        'match-columns': cmd_match_columns,
        'query': cmd_query,
        'upsert': cmd_upsert,
        'ppt-create': cmd_ppt_create,
        'ppt-template': cmd_ppt_template,
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
