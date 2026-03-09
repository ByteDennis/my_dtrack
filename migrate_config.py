#!/usr/bin/env python3
"""
Migration script to convert old two-file config format to unified format.

Usage:
    python migrate_config.py extract_config.json pairs_config.json -o unified_config.json
"""

import argparse
import json
import sys
from pathlib import Path


def migrate(extract_config_path, pairs_config_path, output_path):
    """Convert old format to unified format."""

    # Import config functions
    sys.path.insert(0, str(Path(__file__).parent))
    from dtrack.config import convert_old_to_unified, save_unified_config

    print(f"Reading old configs...")
    print(f"  Extract config: {extract_config_path}")
    print(f"  Pairs config: {pairs_config_path}")

    # Convert
    unified = convert_old_to_unified(
        extract_config_path=extract_config_path,
        pairs_config_path=pairs_config_path
    )

    # Save
    save_unified_config(unified, output_path)

    print(f"\n✓ Created unified config: {output_path}")
    print(f"  Pairs: {len(unified['pairs'])}")

    # Show summary
    print("\nPairs summary:")
    for pair_name, pair_config in unified['pairs'].items():
        left = pair_config['left']
        right = pair_config['right']
        col_map_count = len(pair_config.get('col_map', {}))
        print(f"  {pair_name}:")
        print(f"    Left:  {left.get('source')}/{left.get('name')}")
        print(f"    Right: {right.get('source')}/{right.get('name')}")
        print(f"    Mapped columns: {col_map_count}")


def main():
    parser = argparse.ArgumentParser(
        description='Migrate old dtrack config format to unified format'
    )
    parser.add_argument('extract_config', help='Path to extract_config.json')
    parser.add_argument('pairs_config', help='Path to pairs_config.json')
    parser.add_argument('-o', '--output', required=True, help='Output path for unified config')

    args = parser.parse_args()

    # Validate inputs
    if not Path(args.extract_config).exists():
        print(f"Error: Extract config not found: {args.extract_config}")
        sys.exit(1)

    if not Path(args.pairs_config).exists():
        print(f"Error: Pairs config not found: {args.pairs_config}")
        sys.exit(1)

    if Path(args.output).exists():
        resp = input(f"Output file {args.output} exists. Overwrite? (y/N) ")
        if resp.lower() != 'y':
            print("Aborted")
            sys.exit(0)

    migrate(args.extract_config, args.pairs_config, args.output)


if __name__ == '__main__':
    main()
