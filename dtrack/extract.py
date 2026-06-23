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


# >>> platform string -> extractor callable (lazy import); None if unknown <<< #
def get_platform_extractor(platform):
    platform = (platform or "").lower()
    if platform == "aws":
        from .platforms.athena import extract_aws
        return extract_aws
    if platform == "oracle":
        from .platforms.oracle import extract_oracle
        return extract_oracle
    if platform == "databricks":
        from .platforms.databricks import extract_databricks
        return extract_databricks
    if platform == "hadoop":
        from .platforms.hadoop import extract_hadoop
        return extract_hadoop
    if platform in ("sas", "hadoop_sas"):
        from .platforms.hadoop import gen_sas
        return gen_sas
    return None


# >>> run a single platform's extractor (web /api/extract path); False if unknown <<< #
def extract_one(platform, config_path, outdir, types=None, db_path=None,
                from_date=None, to_date=None, max_workers=None):
    fn = get_platform_extractor(platform)
    if fn is None:
        return False
    kwargs = dict(types=types, db_path=db_path, from_date=from_date, to_date=to_date)
    if (platform or "").lower() not in ("sas", "hadoop_sas"):
        kwargs["max_workers"] = max_workers
    fn(config_path, outdir, **kwargs)
    return True


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

    from .logmgr import get_logger
    get_logger().info("[extract] types=%s groups=%s", types,
                      {k: len(v) for k, v in groups.items() if v})

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
