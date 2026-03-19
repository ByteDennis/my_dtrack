"""Pipeline orchestration for dtrack: init → extract → load → compare → HTML."""

import os
import sys


def run_pipeline(
    project_db,
    config_path,
    outdir=None,
    sas_outdir=None,
    csv_outdir=None,
    types=None,
    vintage=None,
    yes=False,
    html_row=None,
    html_col=None,
    title=None,
    subtitle=None,
    workers=None,
    force=False,
    skip_extract=False,
    skip_load=False,
    skip_compare=False,
):
    """Run the full dtrack pipeline.

    Steps:
        1. Init database (if needed)
        2. Generate SAS / extract Athena
        3. Load row counts and column stats
        4. Compare rows and columns
        5. Generate HTML reports
    """
    from .config import load_unified_config, get_all_tables_from_unified
    from .db import init_database
    from .platforms.base import qualified_name

    if types is None:
        types = ["row", "col"]

    if outdir is None:
        outdir = os.getcwd()
    if sas_outdir is None:
        sas_outdir = os.path.join(outdir, "sas")
    if csv_outdir is None:
        csv_outdir = os.path.join(outdir, "csv")

    output_dir = os.path.join(outdir, "output")
    if html_row is None:
        html_row = os.path.join(output_dir, "compare_row.html")
    if html_col is None:
        html_col = os.path.join(output_dir, "compare_col.html")

    # -- Step 1: Init --
    if not os.path.exists(project_db):
        init_database(project_db)
        print(f"Initialized database: {project_db}")
    else:
        print(f"Using existing database: {project_db}")

    config = load_unified_config(config_path)
    tables = get_all_tables_from_unified(config)

    # Classify tables by platform
    sas_tables = []
    aws_tables = []
    for tbl in tables:
        source = (tbl.get("source") or "").lower()
        processed = tbl.get("processed") or ""
        if processed.startswith("$"):
            sas_tables.append(tbl)
        elif source == "aws":
            aws_tables.append(tbl)
        else:
            sas_tables.append(tbl)

    # -- Step 2: Extract --
    if not skip_extract:
        if sas_tables:
            print(f"\n--- Generating SAS extraction ({len(sas_tables)} tables) ---")
            from .platforms.oracle import gen_sas
            gen_sas(config_path, sas_outdir, types=types, db_path=project_db, vintage=vintage)

        if aws_tables:
            print(f"\n--- Extracting from Athena ({len(aws_tables)} tables) ---")
            from .platforms.athena import extract_aws
            extract_aws(config_path, csv_outdir, types=types, max_workers=workers,
                        db_path=project_db, vintage=vintage, force=force)
    else:
        print("Skipping extraction (--skip-extract)")

    # -- Step 3: Load --
    if not skip_load:
        _load_data(project_db, config, tables, csv_outdir, sas_outdir, types)
    else:
        print("Skipping load (--skip-load)")

    # -- Step 4: Compare --
    if not skip_compare:
        _run_comparisons(project_db, config, config_path, types, yes,
                         html_row, html_col, title, subtitle, vintage)
    else:
        print("Skipping comparison (--skip-compare)")

    print("\nPipeline complete.")


def _load_data(project_db, config, tables, csv_outdir, sas_outdir, types):
    """Load row counts and column stats from CSV folders."""
    from .loader import load_row_counts, load_precomputed_col_stats
    from .db import get_row_counts, get_metadata
    from .platforms.base import qualified_name

    for tbl in tables:
        qname = qualified_name(tbl)
        source = (tbl.get("source") or "").lower()
        folder = csv_outdir if source == "aws" else sas_outdir

        if "row" in types:
            csv_path = os.path.join(folder, f"{qname}_row.csv")
            if os.path.exists(csv_path):
                load_row_counts(
                    db_path=project_db,
                    file_or_folder=csv_path,
                    table_name=qname,
                    mode="upsert",
                    source=tbl.get("source"),
                    date_col=tbl.get("date_col"),
                    where_clause=tbl.get("where", ""),
                )
                rows = get_row_counts(project_db, qname)
                print(f"  {qname}: {len(rows)} date buckets loaded")

        if "col" in types:
            csv_path = os.path.join(folder, f"{qname}_col.csv")
            if os.path.exists(csv_path):
                table_vintage = tbl.get("vintage")
                if not table_vintage:
                    meta = get_metadata(project_db, qname)
                    table_vintage = (meta.get("vintage") or "day") if meta else "day"
                count = load_precomputed_col_stats(
                    db_path=project_db,
                    csv_path=csv_path,
                    table_name=qname,
                    mode="upsert",
                    source=tbl.get("source"),
                    vintage=table_vintage,
                )
                print(f"  {qname}: {count} col stat rows loaded")


def _run_comparisons(project_db, config, config_path, types, yes,
                     html_row, html_col, title, subtitle, vintage):
    """Run row and column comparisons, generate HTML."""
    from argparse import Namespace

    if "row" in types:
        print("\n--- Comparing row counts ---")
        from .cli import cmd_compare_row
        args = Namespace(
            project_db=project_db,
            config=config_path,
            from_date=None,
            to_date=None,
            yes=yes,
            html=html_row,
            title=title,
            subtitle=subtitle,
        )
        cmd_compare_row(args)

    if "col" in types:
        print("\n--- Comparing column stats ---")
        from .cli import cmd_compare_col
        args = Namespace(
            project_db=project_db,
            config=config_path,
            from_date=None,
            to_date=None,
            no_date_filter=False,
            vintage=vintage,
            yes=yes,
            html=html_col,
            title=title,
            subtitle=subtitle,
        )
        cmd_compare_col(args)
