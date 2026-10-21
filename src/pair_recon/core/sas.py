from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import yaml

from pair_recon.app.config import load_environment, local_output_root
from pair_recon.core.debug import run_debugger
from pair_recon_left.backends.sources import source_from_config
from pair_recon_left.common.model.manifest import key_family_specs, sample_plan
from pair_recon_left.common.sql.dates import bounds_where, quote_ident
from pair_recon_left.common.sql.sampling import sample_predicate_sql
from pair_recon_right.engine.backends.targets import SparkTarget


def _keys(pair):
    return [value.strip() for value in str(pair.get("key") or "").split(",") if value.strip()]


def _families(pair, keys):
    return key_family_specs(pair, keys)


def _relation(pair, side, config):
    if side == "right":
        return SparkTarget(None, config).relation(), "spark", "odbc"
    source = source_from_config(config)
    if source.dialect not in {"oracle", "hadoop"}:
        raise ValueError("SAS Left export supports Oracle or Hadoop; CSV is already local")
    return (
        source.relation(from_date=pair.get("fromDate"), to_date=pair.get("toDate")),
        source.dialect,
        "oracle" if source.dialect == "oracle" else "odbc",
    )


def _sql(pair, side, rate):
    config = pair[side]
    relation, dialect, adapter = _relation(pair, side, config)
    mapping = pair["col_map"]
    keys = _keys(pair)
    if not keys:
        raise ValueError("SAS sampling requires a pair key")
    side_keys = keys if side == "left" else [mapping.get(key, key) for key in keys]
    values = list(mapping if side == "left" else mapping.values())
    columns = list(dict.fromkeys([*side_keys, *values]))
    if pair.get("partition_mode", "date") == "date" and config.get("date_col"):
        columns = list(dict.fromkeys([config["date_col"], *columns]))
    plan = sample_plan(float(rate))
    sample = sample_predicate_sql(
        plan,
        [quote_ident(key, dialect) for key in side_keys],
        _families(pair, keys),
        dialect,
        side,
    )
    clauses = [config.get("where"), sample]
    if pair.get("partition_mode", "date") == "date" and config.get("date_col"):
        clauses.append(
            bounds_where(
                config["date_col"],
                dialect=dialect,
                date_type=config.get("date_type"),
                from_date=pair.get("fromDate"),
                to_date=pair.get("toDate"),
            )
        )
    where = " AND ".join(f"({value})" for value in clauses if value) or "1=1"
    selected = ", ".join(quote_ident(column, dialect) for column in columns)
    return f"SELECT {selected} FROM {relation} WHERE {where}", adapter


def _script(sql, adapter, output):
    path = str(output.resolve()).replace("'", "''")
    return "\n".join(
        (
            "options encoding='utf-8';",
            "%let pair_recon_connection=%sysget(PAIR_RECON_CONNECTION);",
            "proc sql;",
            f"connect to {adapter} (&pair_recon_connection);",
            "create table work.pair_recon_sample as",
            f"select * from connection to {adapter} (",
            sql,
            ");",
            f"disconnect from {adapter};",
            "quit;",
            "proc export data=work.pair_recon_sample",
            f"outfile='{path}'",
            "dbms=csv replace;",
            "run;",
            "",
        )
    )


def export_bundle(pair_name, pair, bundle, rate=0.01, sides=("left",)):
    target = Path(bundle)
    target.mkdir(parents=True, exist_ok=True)
    selected = tuple(dict.fromkeys(sides))
    if not selected or set(selected) - {"left", "right"}:
        raise ValueError("sides must contain left, right, or both")
    scripts = {}
    for side in selected:
        sql, adapter = _sql(pair, side, rate)
        script = target / f"{side}.sas"
        script.write_text(_script(sql, adapter, target / f"{side}_sample.csv"), encoding="utf-8")
        scripts[side] = str(script)
    request_path = target / "request.yaml"
    previous = (
        yaml.safe_load(request_path.read_text(encoding="utf-8")) if request_path.is_file() else {}
    ) or {}
    generated = previous.get("generated") or {} if previous.get("pair") == pair_name else {}
    for side in selected:
        generated[side] = {"script": f"{side}.sas", "rate": float(rate)}
    request = {
        "format": "pair-recon-sas-v1",
        "pair": pair_name,
        "rate": float(rate),
        "partition_mode": pair.get("partition_mode", "date"),
        "from_date": pair.get("fromDate"),
        "to_date": pair.get("toDate"),
        "vintage": pair.get("vintage", "day"),
        "col_map": pair["col_map"],
        "strict": bool(pair.get("strict")),
        "col_type_overrides": pair.get("col_type_overrides") or {},
        "column_types": pair.get("column_types") or {},
        "generated": generated,
        "outputs": {side: f"{side}_sample.csv" for side in ("left", "right")},
    }
    request_path.write_text(
        yaml.safe_dump(request, allow_unicode=True, sort_keys=False), encoding="utf-8"
    )
    return {"bundle": str(target), "scripts": scripts}


def _catalog_pair(defaults_path, pair_name=None):
    from pair_recon.app.catalog import Catalog
    from pair_recon.app.config import load_defaults
    from pair_recon.app.runner import StatementSql, pair_config, workspace

    defaults = load_defaults(defaults_path)
    required = ("profile", "warehouse_id", "pair_table", "status_table", "error_table")
    missing = [name for name in required if not defaults.get(name)]
    if missing:
        raise ValueError(f"defaults are missing: {', '.join(missing)}")
    progress = defaults.get("progress_table") or (
        defaults["status_table"].rsplit(".", 1)[0] + ".pair_recon_progress_snapshot"
    )
    client = workspace(defaults["profile"])
    catalog = Catalog(
        StatementSql(client, defaults["warehouse_id"]),
        defaults["pair_table"],
        defaults["status_table"],
        defaults["error_table"],
        progress,
    )
    name = pair_name or defaults.get("pair")
    rows = {row["pair_name"]: row for row in catalog.pairs()}
    if name not in rows:
        raise ValueError(f"enabled pair not found: {name}")
    return name, pair_config(rows[name])


def execute(args):
    load_environment()
    name, pair = _catalog_pair(args.defaults, args.pair)
    sides = ("left", "right") if args.side == "both" else (args.side,)
    bundle = args.bundle or local_output_root() / "sas"
    result = export_bundle(name, pair, bundle, args.rate, sides)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


def main(argv=None):
    values = list(sys.argv[1:] if argv is None else argv)
    normalized = []
    for value in values:
        if any(value.startswith(f"{name}=") for name in ("side", "bundle", "rate", "pair")):
            name, item = value.split("=", 1)
            normalized.extend((f"--{name}", item))
        else:
            normalized.append(value)
    parser = argparse.ArgumentParser(prog="pair_recon sas")
    commands = parser.add_subparsers(dest="command", required=True)
    export = commands.add_parser("export")
    export.add_argument("--defaults", type=Path, default=Path(".pair_recon.yaml"))
    export.add_argument("--pair")
    export.add_argument("--bundle", type=Path)
    export.add_argument("--rate", type=float, default=0.01)
    export.add_argument("--side", choices=("left", "right", "both"), default="left")
    export.add_argument("--debug", nargs="?", const="console", choices=("console", "attach"))
    args = parser.parse_args(normalized)
    return run_debugger(args.debug, lambda: execute(args))


if __name__ == "__main__":
    exit_code = main()
    if exit_code:
        raise SystemExit(exit_code)
