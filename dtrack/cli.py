#!/usr/bin/env python3
"""Command-line interface for dtrack"""

import argparse
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
)
from .loader import load_row_counts, load_column_data
from .load_map import load_map
from .compare import (
    compare_row_counts,
    compare_column_stats,
    get_column_mapping,
    parse_col_map_string,
)


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

    # Determine table name
    if args.table_name:
        table_name = args.table_name
    else:
        # Use filename without extension
        if os.path.isfile(args.file_or_folder):
            table_name = Path(args.file_or_folder).stem
        else:
            print("Error: --table-name is required when loading from a folder")
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


def cmd_load_col(args):
    """Load column statistics from CSV"""
    if not os.path.exists(args.project_db):
        print(f"Error: Database not found: {args.project_db}")
        print(f"Run: dtrack init {args.project_db}")
        sys.exit(1)

    if not args.date_col:
        print("Error: --date-col is required")
        sys.exit(1)

    # Determine source table name
    if args.source_table:
        source_table = args.source_table
    else:
        source_table = Path(args.file_path).stem

    # Parse columns if provided
    columns = None
    if args.columns:
        columns = [c.strip() for c in args.columns.split(',')]

    print(f"Loading column statistics for: {source_table}")
    print(f"  Source: {args.file_path}")
    print(f"  Date column: {args.date_col}")
    print(f"  Mode: {args.mode}")
    print(f"  Vintage: {args.vintage}")
    if columns:
        print(f"  Columns: {', '.join(columns)}")

    load_column_data(
        db_path=args.project_db,
        file_path=args.file_path,
        source_table=source_table,
        date_col=args.date_col,
        columns=columns,
        mode=args.mode,
        vintage=args.vintage,
        from_date=args.from_date,
        to_date=args.to_date,
        source=args.source,
        db_name=args.db,
    )

    # Show summary
    stats = get_col_stats(args.project_db, source_table)
    if stats:
        dates = set(s['dt'] for s in stats)
        cols = set(s['column_name'] for s in stats)
        print(f"✓ Loaded statistics for {len(cols)} columns across {len(dates)} dates")


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


def cmd_compare_row(args):
    """Compare row counts between two tables"""
    if not os.path.exists(args.project_db):
        print(f"Error: Database not found: {args.project_db}")
        sys.exit(1)

    # Determine table names
    if args.pair:
        pair = get_table_pair(args.project_db, args.pair)
        if not pair:
            print(f"Error: Pair '{args.pair}' not found")
            sys.exit(1)
        table_left = pair["table_left"]
        table_right = pair["table_right"]
        source_left = pair.get("source_left", "left")
        source_right = pair.get("source_right", "right")
    else:
        if not args.table_left or not args.table_right:
            print("Error: Either --pair or both table names must be specified")
            sys.exit(1)
        table_left = args.table_left
        table_right = args.table_right
        source_left = "left"
        source_right = "right"

    print(f"\nComparing row counts: {table_left} vs {table_right}")
    print("=" * 70)

    # Perform comparison
    result = compare_row_counts(
        args.project_db,
        table_left,
        table_right,
        from_date=args.from_date,
        to_date=args.to_date,
    )

    summary = result["summary"]

    # Print summary
    print(f"\n{source_left}: {summary['date_range_left'][0]} to {summary['date_range_left'][1]} | {summary['count_left']} dates | total: {summary['total_left']:,}")
    print(f"{source_right}: {summary['date_range_right'][0]} to {summary['date_range_right'][1]} | {summary['count_right']} dates | total: {summary['total_right']:,}")

    # Only in left
    print(f"\nOnly in {source_left} ({len(result['only_left'])} dates):")
    if result["only_left"]:
        for dt, count in result["only_left"][:5]:
            print(f"  {dt}: {count:,}")
        if len(result["only_left"]) > 5:
            print(f"  ... ({len(result['only_left']) - 5} more)")
    else:
        print("  (none)")

    # Only in right
    print(f"\nOnly in {source_right} ({len(result['only_right'])} dates):")
    if result["only_right"]:
        for dt, count in result["only_right"][:5]:
            print(f"  {dt}: {count:,}")
        if len(result["only_right"]) > 5:
            print(f"  ... ({len(result['only_right']) - 5} more)")
    else:
        print("  (none)")

    # Matching
    print(f"\nMatching ({len(result['matching'])} dates):")
    if result["matching"]:
        print(f"  (showing first 3)")
        for dt, count in result["matching"][:3]:
            print(f"  {dt}: {count:,}")
        if len(result["matching"]) > 3:
            print(f"  ... ({len(result['matching']) - 3} more)")
    else:
        print("  (none)")

    # Mismatched
    print(f"\nMismatched ({len(result['mismatched'])} dates):")
    if result["mismatched"]:
        for dt, count_left, count_right in result["mismatched"]:
            diff = count_right - count_left
            print(f"  {dt}: {source_left}={count_left:,}, {source_right}={count_right:,}, diff={diff:+,}")
    else:
        print("  (none)")

    # Summary
    print(f"\nSummary: {len(result['only_left'])} only-{source_left}, {len(result['only_right'])} only-{source_right}, {len(result['matching'])} match, {len(result['mismatched'])} mismatch")
    print()


def cmd_compare_col(args):
    """Compare column statistics between two tables"""
    if not os.path.exists(args.project_db):
        print(f"Error: Database not found: {args.project_db}")
        sys.exit(1)

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
    print("=" * 70)

    # Perform comparison
    result = compare_column_stats(
        args.project_db,
        table_left,
        table_right,
        columns=columns,
        col_mappings=col_mappings,
        from_date=args.from_date,
        to_date=args.to_date,
    )

    if not result:
        print("\nNo matching columns found for comparison")
        return

    # Display results for each column
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
            # Show numeric comparison
            print(f"{'Date':<12} {'n_total':<20} {'n_miss':<20} {'mean':<20} {'std':<20}")
            print("-" * 70)
            for comp in comparisons[:5]:  # Show first 5 dates
                n_total_str = f"{comp['n_total_left']:,} / {comp['n_total_right']:,} ({comp['n_total_diff']:+,})"
                n_miss_str = f"{comp['n_missing_left']:,} / {comp['n_missing_right']:,} ({comp['n_missing_diff']:+,})"
                if comp['mean_left'] is not None and comp['mean_right'] is not None:
                    mean_str = f"{comp['mean_left']:.1f} / {comp['mean_right']:.1f} ({comp['mean_diff']:+.1f})"
                else:
                    mean_str = "N/A"
                if comp['std_left'] is not None and comp['std_right'] is not None:
                    std_str = f"{comp['std_left']:.1f} / {comp['std_right']:.1f} ({comp['std_diff']:+.1f})"
                else:
                    std_str = "N/A"

                print(f"{comp['dt']:<12} {n_total_str:<20} {n_miss_str:<20} {mean_str:<20} {std_str:<20}")

            if len(comparisons) > 5:
                print(f"  ... ({len(comparisons) - 5} more dates)")

        else:
            # Show categorical comparison
            print(f"{'Date':<12} {'n_total':<20} {'n_miss':<20} {'n_unique':<20}")
            print("-" * 70)
            for comp in comparisons[:5]:
                n_total_str = f"{comp['n_total_left']:,} / {comp['n_total_right']:,} ({comp['n_total_diff']:+,})"
                n_miss_str = f"{comp['n_missing_left']:,} / {comp['n_missing_right']:,} ({comp['n_missing_diff']:+,})"
                n_uniq_str = f"{comp['n_unique_left']:,} / {comp['n_unique_right']:,} ({comp['n_unique_diff']:+,})"

                print(f"{comp['dt']:<12} {n_total_str:<20} {n_miss_str:<20} {n_uniq_str:<20}")

            if len(comparisons) > 5:
                print(f"  ... ({len(comparisons) - 5} more dates)")

    print()


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


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description="dtrack - Data tracking CLI for row counts and column statistics",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # init command
    parser_init = subparsers.add_parser('init', help='Initialize a new database')
    parser_init.add_argument('project_db', help='Path to database file')
    parser_init.add_argument('--force', action='store_true', help='Overwrite existing database')

    # load-row command
    parser_load_row = subparsers.add_parser('load-row', help='Load row count data')
    parser_load_row.add_argument('project_db', help='Path to database file')
    parser_load_row.add_argument('file_or_folder', help='CSV file or folder with CSV files')
    parser_load_row.add_argument('--table-name', help='Table name (defaults to filename)')
    parser_load_row.add_argument('--mode', default='upsert', choices=['replace', 'append', 'upsert'],
                                  help='Load mode (default: upsert)')
    parser_load_row.add_argument('--vintage', default='day', choices=['day', 'week', 'month', 'quarter', 'year'],
                                  help='Time granularity (default: day)')
    parser_load_row.add_argument('--source', help='Data source (e.g., aws, pcds, oracle)')
    parser_load_row.add_argument('--db', help='Database or service name')
    parser_load_row.add_argument('--source-table', help='Original table name')
    parser_load_row.add_argument('--date-var', help='Date column name (auto-detected if not provided)')

    # load-col command
    parser_load_col = subparsers.add_parser('load-col', help='Load column statistics')
    parser_load_col.add_argument('project_db', help='Path to database file')
    parser_load_col.add_argument('file_path', help='CSV file with data')
    parser_load_col.add_argument('--date-col', required=True, help='Name of the date column')
    parser_load_col.add_argument('--columns', help='Comma-separated list of columns to analyze')
    parser_load_col.add_argument('--mode', default='upsert', choices=['replace', 'upsert'],
                                  help='Load mode (default: upsert)')
    parser_load_col.add_argument('--vintage', default='day', choices=['day', 'week', 'month', 'quarter', 'year'],
                                  help='Time granularity (default: day)')
    parser_load_col.add_argument('--from-date', help='Start date filter (YYYY-MM-DD)')
    parser_load_col.add_argument('--to-date', help='End date filter (YYYY-MM-DD)')
    parser_load_col.add_argument('--source', help='Data source (e.g., aws, pcds, oracle)')
    parser_load_col.add_argument('--db', help='Database or service name')
    parser_load_col.add_argument('--source-table', help='Source table name (defaults to filename)')

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

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Dispatch to command handler
    commands = {
        'init': cmd_init,
        'load-row': cmd_load_row,
        'load-col': cmd_load_col,
        'load-map': cmd_load_map,
        'list': cmd_list,
        'list-pairs': cmd_list_pairs,
        'show': cmd_show,
        'show-stats': cmd_show_stats,
        'compare-row': cmd_compare_row,
        'compare-col': cmd_compare_col,
    }

    handler = commands.get(args.command)
    if handler:
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
