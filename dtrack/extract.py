"""Extraction dispatch: classify config tables by source, run each platform's extractor."""

import os

from .config import load_unified_config, get_all_tables_from_unified


# >>> group config tables by the platform that extracts them <<< #
def classify_tables_by_platform(tables):
    groups = {"sas": [], "aws": [], "oracle": [], "databricks": [], "hadoop": []}
    for tbl in tables:
        source = (tbl.get("source") or "").lower()
        processed = tbl.get("processed") or ""
        if processed.startswith("$"):
            groups["sas"].append(tbl)
        elif source in ("aws", "oracle", "databricks", "hadoop"):
            groups[source].append(tbl)
        else:
            groups["sas"].append(tbl)
    return groups


# >>> classify config tables and run every platform extractor that has tables <<< #
def extract_all(config_path, outdir=None, sas_outdir=None, csv_outdir=None,
                types=None, max_workers=None, db_path=None, vintage=None,
                force=False, from_date=None, to_date=None,
                preset=None, stats_override=None):
    if types is None:
        types = ["row", "col"]
    if outdir is None:
        outdir = os.getcwd()
    if sas_outdir is None:
        sas_outdir = os.path.join(outdir, "sas")
    if csv_outdir is None:
        csv_outdir = os.path.join(outdir, "csv")

    config = load_unified_config(config_path)
    groups = classify_tables_by_platform(get_all_tables_from_unified(config))

    if groups["sas"]:
        print(f"\n--- Generating SAS extraction ({len(groups['sas'])} tables) ---")
        from .platforms.hadoop import gen_sas
        gen_sas(config_path, sas_outdir, types=types, db_path=db_path, vintage=vintage,
                from_date=from_date, to_date=to_date,
                preset=preset, stats_override=stats_override)

    if groups["oracle"]:
        print(f"\n--- Extracting from Oracle ({len(groups['oracle'])} tables) ---")
        from .platforms.oracle import extract_oracle
        extract_oracle(config_path, csv_outdir, types=types, max_workers=max_workers,
                       db_path=db_path, vintage=vintage, force=force,
                       from_date=from_date, to_date=to_date,
                       preset=preset, stats_override=stats_override)

    if groups["aws"]:
        print(f"\n--- Extracting from Athena ({len(groups['aws'])} tables) ---")
        from .platforms.athena import extract_aws
        extract_aws(config_path, csv_outdir, types=types, max_workers=max_workers,
                    db_path=db_path, vintage=vintage, force=force,
                    from_date=from_date, to_date=to_date,
                    preset=preset, stats_override=stats_override)

    if groups["hadoop"]:
        print(f"\n--- Extracting from Hadoop ({len(groups['hadoop'])} tables) ---")
        from .platforms.hadoop import extract_hadoop
        extract_hadoop(config_path, csv_outdir, types=types, max_workers=max_workers,
                       db_path=db_path, vintage=vintage, force=force,
                       from_date=from_date, to_date=to_date,
                       preset=preset, stats_override=stats_override)

    if groups["databricks"]:
        print(f"\n--- Extracting from Databricks ({len(groups['databricks'])} tables) ---")
        from .platforms.databricks import extract_databricks
        extract_databricks(config_path, csv_outdir, types=types, max_workers=max_workers,
                           db_path=db_path, vintage=vintage, force=force,
                           from_date=from_date, to_date=to_date,
                           preset=preset, stats_override=stats_override)
