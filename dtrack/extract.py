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
                preset=None, stats_override=None, gen_only=False):
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

    verb = "Generating" if gen_only else "Extracting"

    if groups["sas"]:
        print(f"\n--- Generating SAS extraction ({len(groups['sas'])} tables) ---")
        from .platforms.hadoop import gen_sas
        gen_sas(config_path, sas_outdir, types=types, db_path=db_path, vintage=vintage,
                from_date=from_date, to_date=to_date,
                preset=preset, stats_override=stats_override, gen_only=gen_only)

    if groups["oracle"]:
        print(f"\n--- {verb} from Oracle ({len(groups['oracle'])} tables) ---")
        from .platforms.oracle import extract_oracle
        extract_oracle(config_path, csv_outdir, types=types, max_workers=max_workers,
                       db_path=db_path, vintage=vintage, force=force,
                       from_date=from_date, to_date=to_date,
                       preset=preset, stats_override=stats_override, gen_only=gen_only)

    if groups["aws"]:
        print(f"\n--- {verb} from Athena ({len(groups['aws'])} tables) ---")
        from .platforms.athena import extract_aws
        extract_aws(config_path, csv_outdir, types=types, max_workers=max_workers,
                    db_path=db_path, vintage=vintage, force=force,
                    from_date=from_date, to_date=to_date,
                    preset=preset, stats_override=stats_override, gen_only=gen_only)

    if groups["hadoop"]:
        print(f"\n--- {verb} from Hadoop ({len(groups['hadoop'])} tables) ---")
        from .platforms.hadoop import extract_hadoop
        extract_hadoop(config_path, csv_outdir, types=types, max_workers=max_workers,
                       db_path=db_path, vintage=vintage, force=force,
                       from_date=from_date, to_date=to_date,
                       preset=preset, stats_override=stats_override, gen_only=gen_only)

    if groups["databricks"]:
        print(f"\n--- {verb} from Databricks ({len(groups['databricks'])} tables) ---")
        from .platforms.databricks import extract_databricks
        extract_databricks(config_path, csv_outdir, types=types, max_workers=max_workers,
                           db_path=db_path, vintage=vintage, force=force,
                           from_date=from_date, to_date=to_date,
                           preset=preset, stats_override=stats_override, gen_only=gen_only)


# >>> platform string -> (runner, mock-env-var) for run_sql_all <<< #
_RUNNERS = {
    "aws": ("dtrack.platforms.athena", "run_sql_file", "DTRACK_ATHENA_MOCK"),
    "oracle": ("dtrack.platforms.oracle", "run_oracle_sql_file", "DTRACK_ORACLE_MOCK"),
    "hadoop": ("dtrack.platforms.hadoop", "run_hadoop_sql_file", "DTRACK_HADOOP_MOCK"),
    "databricks": ("dtrack.platforms.databricks", "run_databricks_sql_file", "DTRACK_DATABRICKS_MOCK"),
}


# >>> execute already-generated extract_{type}.<platform>.sql via each platform's runner (mirrors web /api/extract/run-sql) <<< #
def run_sql_all(config_path, outdir=None, sas_outdir=None, csv_outdir=None,
                types=None, max_workers=None, db_path=None, resume=False):
    import importlib
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
    get_logger().info("[run-sql] types=%s groups=%s", types,
                      {k: len(v) for k, v in groups.items() if v})

    for platform in ("oracle", "aws", "hadoop", "databricks"):
        tbls = groups.get(platform) or []
        if not tbls:
            continue
        mock_dir = os.environ.get("DTRACK_MOCK") or os.environ.get(_RUNNERS[platform][2])
        if mock_dir:
            from .platforms.base import _extract_mock_tables
            print(f"\n--- [run-sql] {platform}: mock copy ({len(tbls)} tables) ---")
            _extract_mock_tables(tbls, csv_outdir, types, mock_dir)
            continue
        mod_name, fn_name, _ = _RUNNERS[platform]
        runner = getattr(importlib.import_module(mod_name), fn_name)
        for t in types:
            sql_path = os.path.join(csv_outdir, f"extract_{t}.{platform}.sql")
            if not os.path.exists(sql_path):
                print(f"[run-sql] {platform} {t}: missing {sql_path} "
                      f"(run `dtrack generate --type {t}` first)")
                continue
            print(f"\n--- [run-sql] {platform} {t}: {sql_path} ---")
            runner(sql_path, csv_outdir, max_workers=max_workers,
                   db_path=db_path, resume=resume)

    if groups.get("sas"):
        print(f"\n--- [run-sql] sas ({len(groups['sas'])} tables): run extract_{{type}}.sas "
              f"on the SAS server (not executed here) ---")
