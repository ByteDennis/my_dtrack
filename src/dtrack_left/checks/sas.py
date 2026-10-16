from __future__ import annotations

import csv
import hashlib
import json
import re
from pathlib import Path
from typing import Any

from ..backends.sources import OracleSource
from ..common.dates import bounds_where, canonical_date, quote_ident, resolve_date_type
from ..common.duck import use_temp
from ..common.hashing import key_family
from ..common.manifest import build_manifest, window_id
from ..common.protocol import fixed_keys_path, write_run_status
from ..common.query_windows import QUERY_WINDOW_MARKER
from ..common.sampling import cast_text_sql, hash32_sql, hash_hex_sql, normalize_sql
from .col_check import ColCheckPipeline, LeftRuntime
from .row_check import active_pairs, public_pair_config, publish_row_check_results

SAS_BUNDLE_FORMAT = "dtrack-sas-v1"


def _digest(value: Any) -> str:
    body = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(body).hexdigest()


def _pair_directory(name: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9_-]+", "-", name).strip("-")[:40] or "pair"
    return f"{slug}-{_digest(name)[:8]}"


def _request(config: dict[str, Any], run_id: str, pair_names: list[str] | None) -> dict[str, Any]:
    pairs = active_pairs(config, pair_names)
    if pair_names and set(pair_names) != set(pairs):
        missing = sorted(set(pair_names) - set(pairs))
        raise ValueError(f"selected pair(s) are missing or skipped: {', '.join(missing)}")
    public = {}
    for name, pair in pairs.items():
        if pair.get("partition_mode") == "all":
            raise ValueError(f"{name}: SAS export requires partition_mode date")
        if str(pair["left"].get("source") or "").lower() != "oracle":
            raise ValueError(f"{name}: SAS export supports only a Left Oracle source")
        date_type = resolve_date_type(pair["left"].get("date_type"))
        if date_type.requires_probe:
            raise ValueError(
                f"{name}: SAS export needs a concrete Left date_type; "
                "set string format dash|compact or sas format date9|datetime|days"
            )
        pair_config = public_pair_config(pair)
        manifest = build_manifest(
            run_id,
            name,
            pair_config,
            {"gate_decision": {"passed": True}, "partitions": {}},
        )
        if manifest["sample"]["mode"] == "fixed" and manifest["sample"]["count"] > 1000:
            raise ValueError(f"{name}: first SAS bundle version supports fixed sample <= 1000 keys")
        public[name] = {"directory": _pair_directory(name), "config": pair_config}
    config_view = {
        "version": 2,
        "timezone": config.get("timezone"),
        "pairs": {name: row["config"] for name, row in public.items()},
    }
    return {
        "format": SAS_BUNDLE_FORMAT,
        "run_id": run_id,
        "config_hash": _digest(config_view),
        "timezone": config.get("timezone"),
        "pairs": public,
    }


def _partition_relation(left: dict[str, Any], partitions_macro: str) -> str:
    source = OracleSource(left)
    if not left.get("query"):
        return source.relation()

    def predicate(match: re.Match) -> str:
        spec = left["query_windows"][match.group(1)]
        expression = resolve_date_type(spec["date_type"]).normalize_sql(
            spec["expression"], "oracle"
        )
        return f"({expression} IN ({partitions_macro}))"

    query = QUERY_WINDOW_MARKER.sub(predicate, left["query"])
    return f"({query}) dtrack_source"


def _projection(columns: list[str]) -> str:
    return ", ".join(
        f'{quote_ident(column, "oracle")} AS "{column.replace(chr(34), chr(34) * 2)}"'
        for column in columns
    )


def _render_pair(name: str, row: dict[str, Any], run_id: str) -> str:
    pair = row["config"]
    left = pair["left"]
    token = "p" + _digest(name)[:10]
    directory = row["directory"]
    source = OracleSource(left)
    date_type = resolve_date_type(left.get("date_type"))
    date_column = quote_ident(left["date_col"], "oracle")
    canonical_day = date_type.normalize_sql(date_column, "oracle")
    bounds = bounds_where(
        left["date_col"],
        dialect="oracle",
        date_type=left.get("date_type"),
        from_date=pair.get("fromDate"),
        to_date=pair.get("toDate"),
    )
    counts_sql = (
        f"SELECT {canonical_day} AS dtrack_partition, COUNT(*) AS dtrack_count "
        f"FROM {source.relation(from_date=pair.get('fromDate'), to_date=pair.get('toDate'))} "
        f"WHERE {source._where(bounds)} GROUP BY {canonical_day}"
    )
    manifest = build_manifest(
        run_id,
        name,
        pair,
        {"gate_decision": {"passed": True}, "partitions": {}},
    )
    selected = list(dict.fromkeys([left["date_col"], *manifest["key_cols"], *manifest["columns"]]))
    partitions = "&dtrack_partitions"
    relation = _partition_relation(left, partitions)
    date_predicate = f"{canonical_day} IN ({partitions})"
    window_where = source._where(date_predicate)
    sample = manifest["sample"]
    key_columns = [quote_ident(column, "oracle") for column in manifest["key_cols"]]
    key_aliases = _projection(manifest["key_cols"])
    selected_columns = [quote_ident(column, "oracle") for column in selected]
    type_index_by_name = {column: index for index, column in enumerate(selected, 1)}
    metadata_relation = source.relation()
    probes = []
    sample_setup = []
    for index, (column_name, column) in enumerate(zip(selected, selected_columns, strict=True), 1):
        probe_where = source._where(f"{column} IS NOT NULL")
        probes.append(
            "(SELECT REGEXP_SUBSTR(DUMP("
            f"{column}, 10), 'Typ=([0-9]+)', 1, 1, NULL, 1) "
            f"FROM {metadata_relation} WHERE {probe_where} FETCH FIRST 1 ROW ONLY) "
            f'AS "dtrack_type_{index}"'
        )
        alias = column_name.replace('"', '""')
        number_text = cast_text_sql(column, "oracle", "DB_TYPE_NUMBER")
        date_text = cast_text_sql(column, "oracle", "DB_TYPE_DATE")
        timestamp_text = cast_text_sql(column, "oracle", "DB_TYPE_TIMESTAMP")
        sample_setup.extend(
            [
                (
                    f"  %if %length(%superq(dtrack_type_{index})) = 0 %then "
                    f'%let dtrack_sample_{index}=CAST(NULL AS VARCHAR2(1)) AS "{alias}";'
                ),
                (
                    f"  %else %if %superq(dtrack_type_{index}) = 2 or "
                    f"%superq(dtrack_type_{index}) = 100 or "
                    f"%superq(dtrack_type_{index}) = 101 %then "
                    f'%let dtrack_sample_{index}={number_text} AS "{alias}";'
                ),
                (
                    f"  %else %if %superq(dtrack_type_{index}) = 12 %then "
                    f'%let dtrack_sample_{index}={date_text} AS "{alias}";'
                ),
                (
                    f"  %else %if %superq(dtrack_type_{index}) = 180 %then "
                    f'%let dtrack_sample_{index}={timestamp_text} AS "{alias}";'
                ),
                (
                    f"  %else %if %superq(dtrack_type_{index}) = 1 or "
                    f"%superq(dtrack_type_{index}) = 96 %then "
                    f'%let dtrack_sample_{index}={column} AS "{alias}";'
                ),
                "  %else %do;",
                (
                    f"    %put ERROR: {directory} column {index} has unsupported Oracle "
                    f"type %superq(dtrack_type_{index}).;"
                ),
                "    %abort cancel;",
                "  %end;",
            ]
        )
    type_sql = "SELECT " + ", ".join(probes) + " FROM DUAL"
    key_setup = []
    composite_parts = []
    stored_hash = left.get("sample_hash") or {}
    stored_hash_matches = (
        sample["mode"] == "fraction"
        and stored_hash
        and stored_hash.get("version") == 1
        and stored_hash.get("keys") == manifest["key_cols"]
        and stored_hash.get("families") == manifest["key_families"]
    )
    needs_key_sql = sample["mode"] == "fixed" or (
        sample["mode"] == "fraction" and not stored_hash_matches
    )
    if stored_hash and stored_hash.get("version") != 1:
        raise ValueError(f"{name}: unsupported materialized sample hash version")
    if needs_key_sql:
        for key_index, (key_name, column, family_spec) in enumerate(
            zip(
                manifest["key_cols"],
                key_columns,
                manifest["key_families"],
                strict=True,
            ),
            1,
        ):
            type_index = type_index_by_name[key_name]
            family = key_family(family_spec)
            variants = {
                "char": normalize_sql(column, family, "oracle", "left"),
                "number": normalize_sql(column, family, "oracle", "left", "DB_TYPE_NUMBER"),
                "date": normalize_sql(column, family, "oracle", "left", "DB_TYPE_DATE"),
                "timestamp": normalize_sql(column, family, "oracle", "left", "DB_TYPE_TIMESTAMP"),
            }
            key_setup.extend(
                [
                    (
                        f"  %if %superq(dtrack_type_{type_index}) = 181 or "
                        f"%superq(dtrack_type_{type_index}) = 231 %then %do;"
                    ),
                    (
                        f"    %put ERROR: {directory} key {key_index} has an unsupported "
                        "timezone-bearing Oracle type.;"
                    ),
                    "    %abort cancel;",
                    "  %end;",
                    (
                        f"  %if %superq(dtrack_type_{type_index}) = 2 %then "
                        f"%let dtrack_key_{key_index}={variants['number']};"
                    ),
                    (
                        f"  %else %if %superq(dtrack_type_{type_index}) = 12 %then "
                        f"%let dtrack_key_{key_index}={variants['date']};"
                    ),
                    (
                        f"  %else %if %superq(dtrack_type_{type_index}) = 180 %then "
                        f"%let dtrack_key_{key_index}={variants['timestamp']};"
                    ),
                    f"  %else %let dtrack_key_{key_index}={variants['char']};",
                ]
            )
            composite_parts.append(f"(&&dtrack_key_{key_index})")
    composite = " || '|' || ".join(composite_parts)
    if sample["mode"] == "fraction":
        threshold = int(sample["threshold"])
        if stored_hash_matches:
            sample_predicate = f"{quote_ident(stored_hash['column'], 'oracle')} < {threshold}"
        else:
            sample_predicate = f"{hash32_sql('&dtrack_composite', 'oracle')} < {threshold}"
    elif sample["mode"] == "fixed":
        sample_predicate = "&dtrack_composite IN (&dtrack_fixed_values)"
    else:
        sample_predicate = ""
    pull_where = window_where
    if sample_predicate:
        pull_where = f"({window_where}) AND ({sample_predicate})"

    fixed_block = ""
    if sample["mode"] == "fixed":
        rank = hash_hex_sql("&dtrack_composite", "oracle")
        selected_keys = ", ".join(key_columns)
        fixed_sql = (
            f"SELECT {key_aliases}, dtrack_norm FROM (SELECT {selected_keys}, "
            f"{rank} AS dtrack_rank, &dtrack_composite AS dtrack_norm FROM {relation} "
            f"WHERE {window_where} GROUP BY {selected_keys}) dtrack_keys "
            f"ORDER BY dtrack_rank, dtrack_norm, {selected_keys} "
            f"FETCH FIRST {int(sample['count'])} ROWS ONLY"
        )
        fixed_block = f"""
  proc sql;
    connect to oracle (&dtrack_connect_{token});
    create table work.dtrack_keys_{token} as
      select * from connection to oracle (
        {fixed_sql}
      );
    disconnect from oracle;
  quit;
  data work.dtrack_keys_export_{token};
    set work.dtrack_keys_{token}(keep={" ".join(manifest["key_cols"])});
  run;
  proc export data=work.dtrack_keys_export_{token}
    outfile="&dtrack_bundle./output/{directory}/keys/&dtrack_bucket..csv"
    dbms=csv replace;
  run;
  %let dtrack_fixed_values='__DTRACK_NO_KEYS__';
  proc sql noprint;
    select cats("'", tranwrd(strip(dtrack_norm), "'", "''"), "'")
      into :dtrack_fixed_values separated by ','
    from work.dtrack_keys_{token};
  quit;
"""

    excluded = pair.get("excluded_partitions") or []
    excluded_line = ""
    if excluded:
        values = ", ".join(f"'{value}'" for value in excluded)
        excluded_line = f"  if dtrack_day in ({values}) then delete;\n"
    bucket_expression = {
        "day": "dtrack_date",
        "week": "intnx('week.2', dtrack_date, 0, 'b')",
        "month": "intnx('month', dtrack_date, 0, 'b')",
        "quarter": "intnx('qtr', dtrack_date, 0, 'b')",
        "year": "intnx('year', dtrack_date, 0, 'b')",
    }[manifest["vintage"]]
    locals_list = " ".join(
        [
            *(f"dtrack_type_{i}" for i in range(1, len(selected) + 1)),
            *(f"dtrack_sample_{i}" for i in range(1, len(selected) + 1)),
            "dtrack_sample_projection",
        ]
    )
    type_assignments = "\n".join(
        f"    call symputx('dtrack_type_{i}', strip(dtrack_type_{i}), 'L');"
        for i in range(1, len(selected) + 1)
    )
    sample_projection = ", ".join(f"&dtrack_sample_{i}" for i in range(1, len(selected) + 1))
    composite_assignment = f"  %let dtrack_composite={composite};" if needs_key_sql else ""
    type_block = f"""
  %local {locals_list};
  proc sql;
    connect to oracle (&dtrack_connect_{token});
    create table work.dtrack_types_{token} as
      select * from connection to oracle (
        {type_sql}
      );
    disconnect from oracle;
  quit;
  data _null_;
    set work.dtrack_types_{token};
{type_assignments}
  run;
{chr(10).join(sample_setup)}
  %let dtrack_sample_projection={sample_projection};
{chr(10).join(key_setup)}
{composite_assignment}
"""

    connect_hint = str(left.get("conn_macro") or "the configured Oracle service").replace(
        "*/", "* /"
    )
    pair_hint = str(name).replace("*/", "* /").replace("\r", " ").replace("\n", " ")
    return f"""
/* Pair {pair_hint}; conn_macro: {connect_hint}. Edit only this connection clause if needed. */
%let dtrack_connect_{token}=path=CHANGE_ME authdomain=CHANGE_ME;

%macro dtrack_pull_{token};
{fixed_block}
  proc sql;
    connect to oracle (&dtrack_connect_{token});
    create table work.dtrack_window_{token} as
      select * from connection to oracle (
        SELECT &dtrack_sample_projection FROM {relation} WHERE {pull_where}
      );
    disconnect from oracle;
  quit;
  proc export data=work.dtrack_window_{token}
    outfile="&dtrack_bundle./output/{directory}/windows/&dtrack_bucket..csv"
    dbms=csv replace;
  run;
%mend;

%macro dtrack_pair_{token};
  %local dtrack_bucket_count dtrack_i dtrack_bucket dtrack_partitions dtrack_composite
         dtrack_fixed_values;
  proc sql;
    connect to oracle (&dtrack_connect_{token});
    create table work.dtrack_counts_{token} as
      select * from connection to oracle (
        {counts_sql}
      );
    disconnect from oracle;
  quit;
  proc sort data=work.dtrack_counts_{token}; by dtrack_partition; run;
  proc export data=work.dtrack_counts_{token}
    outfile="&dtrack_bundle./output/{directory}/counts.csv"
    dbms=csv replace;
  run;

  data work.dtrack_control_{token};
    set work.dtrack_counts_{token};
    length dtrack_day dtrack_bucket $10;
    dtrack_day=strip(vvalue(dtrack_partition));
    dtrack_date=input(dtrack_day, yymmdd10.);
    if missing(dtrack_date) then do;
      put "ERROR: non-ISO dtrack partition " dtrack_day;
      abort cancel;
    end;
{excluded_line}  dtrack_bucket=put({bucket_expression}, yymmdd10.);
    keep dtrack_day dtrack_bucket;
  run;
  proc sort data=work.dtrack_control_{token} nodupkey;
    by dtrack_bucket dtrack_day;
  run;
{type_block}
  proc sql noprint;
    select distinct dtrack_bucket into :dtrack_bucket1-
    from work.dtrack_control_{token} order by dtrack_bucket;
    %let dtrack_bucket_count=&sqlobs;
  quit;
  %do dtrack_i=1 %to &dtrack_bucket_count;
    %let dtrack_bucket=&&dtrack_bucket&dtrack_i;
    proc sql noprint;
      select cats("'", strip(dtrack_day), "'") into :dtrack_partitions separated by ','
      from work.dtrack_control_{token}
      where dtrack_bucket="&dtrack_bucket"
      order by dtrack_day;
    quit;
    %dtrack_pull_{token};
  %end;
%mend;
%dtrack_pair_{token};
"""


def export_sas_bundle(
    config: dict[str, Any],
    bundle: str | Path,
    *,
    run_id: str,
    pair_names: list[str] | None = None,
) -> dict[str, Any]:
    root = Path(bundle).resolve()
    request = _request(config, run_id, pair_names)
    if root.exists() and any(root.iterdir()):
        raise FileExistsError(f"SAS bundle directory is not empty: {root}")
    root.mkdir(parents=True, exist_ok=True)
    for row in request["pairs"].values():
        pair_root = root / "output" / row["directory"]
        (pair_root / "windows").mkdir(parents=True, exist_ok=True)
        (pair_root / "keys").mkdir(parents=True, exist_ok=True)
    (root / "request.json").write_text(
        json.dumps(request, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    bundle_path = str(root).replace("\\", "/")
    pair_scripts = [_render_pair(name, row, run_id) for name, row in request["pairs"].items()]
    completion = json.dumps(
        {"format": SAS_BUNDLE_FORMAT, "run_id": run_id, "config_hash": request["config_hash"]},
        sort_keys=True,
        separators=(",", ":"),
    ).replace('"', '""')
    script = f"""/* Generated by dtrack left-run --export-sas. */
options validvarname=any errorabend;
%let dtrack_bundle={bundle_path};
{"".join(pair_scripts)}
data _null_;
  file "&dtrack_bundle./output/_SUCCESS.json" lrecl=32767;
  put "{completion}";
run;
"""
    script_path = root / "left-run.sas"
    script_path.write_text(script, encoding="utf-8")
    return {
        "run_id": run_id,
        "bundle": str(root),
        "script": str(script_path),
        "pairs": list(request["pairs"]),
        "config_hash": request["config_hash"],
    }


def _read_counts(path: Path) -> dict[str, int]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        names = {str(name).casefold(): name for name in reader.fieldnames or []}
        if not {"dtrack_partition", "dtrack_count"} <= set(names):
            raise ValueError(f"{path}: counts CSV needs dtrack_partition,dtrack_count")
        counts: dict[str, int] = {}
        for row in reader:
            partition = canonical_date(row[names["dtrack_partition"]])
            counts[partition] = counts.get(partition, 0) + int(row[names["dtrack_count"]])
    return counts


def _csv_header(path: Path, expected: list[str]) -> None:
    if not path.is_file():
        raise FileNotFoundError(path)
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader, [])
        if [name.casefold() for name in header] != [name.casefold() for name in expected]:
            raise ValueError(f"{path}: expected CSV columns {expected}, got {header}")


def _csv_rows(path: Path, expected: list[str]) -> list[list[str]]:
    _csv_header(path, expected)
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        next(reader, None)
        return list(reader)


class SasBundleRuntime:
    def __init__(self, bundle: Path, directories: dict[str, str]):
        self.bundle = bundle
        self.directories = directories

    def pull_window(self, manifest, window):
        return (
            self.bundle
            / "output"
            / self.directories[manifest["pair"]]
            / "windows"
            / f"{window['bucket']}.csv"
        )

    @staticmethod
    def compute_stats(manifest, window, path):
        statistics = LeftRuntime.compute_stats(manifest, window, path)
        if manifest["sample"]["mode"] == "all" and statistics:
            first = statistics[manifest["columns"][0]]["n_total"]
            if first != window["n_left"]:
                raise ValueError(
                    f"{manifest['pair']} {window['bucket']}: SAS exported {first} rows, "
                    f"expected {window['n_left']}"
                )
        return statistics


def import_sas_bundle(
    config: dict[str, Any],
    bundle: str | Path,
    store,
    *,
    run_id: str | None = None,
    pair_names: list[str] | None = None,
    pull_workers: int = 1,
    stat_workers: int = 1,
    wip: int = 200,
    work_dir: str | Path | None = None,
) -> dict[str, Any]:
    root = Path(bundle).resolve()
    if work_dir is not None:
        use_temp(Path(work_dir) / "duckdb")
    request = json.loads((root / "request.json").read_text(encoding="utf-8"))
    if request.get("format") != SAS_BUNDLE_FORMAT:
        raise ValueError(f"unsupported SAS bundle format: {request.get('format')!r}")
    requested_pairs = list(request.get("pairs") or {})
    if pair_names and set(pair_names) != set(requested_pairs):
        raise ValueError("--pair selection must match the exported SAS bundle")
    resolved_run_id = run_id or request["run_id"]
    if resolved_run_id != request["run_id"]:
        raise ValueError("--run-id does not match the exported SAS bundle")
    current = _request(config, resolved_run_id, requested_pairs)
    if current["config_hash"] != request.get("config_hash"):
        raise ValueError("config changed since SAS export; generate a new SAS bundle")
    complete = json.loads((root / "output" / "_SUCCESS.json").read_text(encoding="utf-8"))
    expected_complete = {
        "format": SAS_BUNDLE_FORMAT,
        "run_id": resolved_run_id,
        "config_hash": request["config_hash"],
    }
    if complete != expected_complete:
        raise ValueError("SAS completion marker does not match request.json")

    pairs = active_pairs(config, requested_pairs)
    directories = {name: row["directory"] for name, row in request["pairs"].items()}
    results = {}
    manifests = []
    fixed_documents = {}
    for name, pair in pairs.items():
        pair_root = root / "output" / directories[name]
        counts = _read_counts(pair_root / "counts.csv")
        public = public_pair_config(pair)
        baseline = {
            "gate_decision": {"passed": True},
            "partitions": {
                partition: {"left": count, "right": None, "verdict": "left_only"}
                for partition, count in counts.items()
            },
        }
        manifest = build_manifest(resolved_run_id, name, public, baseline)
        manifests.append(manifest)
        expected_windows = {f"{window['bucket']}.csv" for window in manifest["windows"]}
        found_windows = {path.name for path in (pair_root / "windows").glob("*.csv")}
        if found_windows != expected_windows:
            raise ValueError(
                f"{name}: SAS windows differ from manifest; "
                f"missing={sorted(expected_windows - found_windows)}, "
                f"extra={sorted(found_windows - expected_windows)}"
            )
        selected = list(
            dict.fromkeys(
                [manifest["left"]["date_col"], *manifest["key_cols"], *manifest["columns"]]
            )
        )
        for window in manifest["windows"]:
            _csv_header(pair_root / "windows" / f"{window['bucket']}.csv", selected)
        if manifest["sample"]["mode"] == "fixed":
            expected_keys = {f"{window['bucket']}.csv" for window in manifest["windows"]}
            found_keys = {path.name for path in (pair_root / "keys").glob("*.csv")}
            if found_keys != expected_keys:
                raise ValueError(
                    f"{name}: SAS key files differ from manifest; "
                    f"missing={sorted(expected_keys - found_keys)}, "
                    f"extra={sorted(found_keys - expected_keys)}"
                )
            windows = {}
            window_ids = {}
            for window in manifest["windows"]:
                rows = _csv_rows(
                    pair_root / "keys" / f"{window['bucket']}.csv", manifest["key_cols"]
                )
                windows[window["bucket"]] = list(
                    dict.fromkeys(
                        tuple(None if value == "" else value for value in row) for row in rows
                    )
                )
                window_ids[window["bucket"]] = window_id(manifest, window)
            fixed_documents[name] = {
                "pair": name,
                "plan_id": manifest["plan_id"],
                "windows": {key: [list(row) for row in rows] for key, rows in windows.items()},
                "window_ids": window_ids,
            }
        results[name] = {
            "counts": counts,
            "preflight": {"ok": True, "source": "oracle", "executor": "sas"},
            "timing": {"left_count_s": None},
        }

    write_run_status(
        store,
        {"state": "left_sas_import", "run_id": resolved_run_id, "role": "left"},
        timezone_name=config.get("timezone"),
    )
    publish_row_check_results(store, resolved_run_id, pairs, results)
    for name, document in fixed_documents.items():
        store.put_json(fixed_keys_path(name), document)
    runtime = SasBundleRuntime(root, directories)
    published = ColCheckPipeline(
        store,
        pull_window=runtime.pull_window,
        compute_stats=runtime.compute_stats,
        pull_workers=pull_workers,
        stat_workers=stat_workers,
        wip=wip,
    ).run(manifests)
    return {
        "run_id": resolved_run_id,
        "pairs": list(pairs),
        "published_units": published,
    }
