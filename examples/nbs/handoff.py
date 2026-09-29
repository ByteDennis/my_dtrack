from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd

from dtrack_left.backends.sources import FileSource, source_from_config
from dtrack_left.checks.col_check import LeftRuntime
from dtrack_left.checks.row_check import public_pair_config
from dtrack_left.cli.config import resolve_connections
from dtrack_left.common.dates import canonical_date
from dtrack_left.common.hashing import sample_csv
from dtrack_left.common.manifest import build_manifest, unit_id
from dtrack_left.common.stats import compute_csv_statistics

NULL_TOKENS = {".", "NULL", "null", "NA", "N/A", "n/a", "nan", "NaN", "None", "#N/A", "(null)"}
THOUSANDS = re.compile(r"^-?\d{1,3}(,\d{3})+(\.\d+)?$")
SCIENTIFIC = re.compile(r"^-?\d(\.\d+)?[eE][+-]?\d+$")
SERIAL_DATE = re.compile(r"^[3-6]\d{4}(\.0+)?$")


def now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


# >>> a Right side that reads exported files (Athena UNLOAD, parquet, csv) instead of Databricks <<< #
class FileTarget:
    def __init__(self, config: dict, work_dir: str | Path):
        self.config = dict(config)
        self.work_dir = Path(work_dir)
        self.source = FileSource(self.config)

    def preflight(self) -> dict:
        return {**self.source.preflight(), "source": "file-target"}

    def partition_counts(self, from_date=None, to_date=None) -> dict[str, int]:
        return self.source.partition_counts(from_date, to_date)

    def window_statistics(self, manifest: dict, window: dict) -> dict:
        right_columns = [manifest["col_map"][column] for column in manifest["columns"]]
        output = self.work_dir / manifest["pair"] / window["bucket"] / "right.csv"
        path = self.source.pull_window(
            window["partitions"], right_columns, output, key_columns=manifest["right_key_cols"]
        )
        plan = manifest["sample"]
        sampled = sample_csv(
            path,
            key_columns=manifest["right_key_cols"],
            mode=plan["mode"],
            rate=plan.get("rate"),
            count=plan.get("count"),
            families=manifest["key_families"],
            key_tuples=window.get("key_tuples"),
        )
        overrides = manifest.get("col_type_overrides") or {}
        return compute_csv_statistics(
            sampled,
            right_columns,
            strict=manifest.get("strict", False),
            col_types={manifest["col_map"][key]: value for key, value in overrides.items()},
        )


# >>> SAS numeric dates count days from 1960-01-01; Excel serials count from 1899-12-30 <<< #
def sas_date(days: float) -> str:
    return (date(1960, 1, 1) + timedelta(days=int(float(days)))).isoformat()


def excel_date(serial: float) -> str:
    return (date(1899, 12, 30) + timedelta(days=int(float(serial)))).isoformat()


# >>> read any extract as text, the way the pipeline will see it <<< #
def read_text(path: str | Path, **options) -> pd.DataFrame:
    source = FileSource({"path": str(path), "date_col": "", "options": options})
    columns, rows = source.query(f"SELECT * FROM {source.relation()}")
    frame = pd.DataFrame(rows, columns=columns, dtype=object)
    return frame.map(lambda value: None if value is None else str(value))


# >>> findings that make two sides disagree before any statistic is computed <<< #
def check_handoff(frame: pd.DataFrame, *, date_col: str, columns, key_cols=()) -> pd.DataFrame:
    findings = []

    def add(check, column, mask, severity):
        count = int(mask.sum())
        if count:
            example = frame.loc[mask, column].iloc[0]
            findings.append((check, column, count, example, severity))

    for column in [date_col, *key_cols, *columns]:
        if column not in frame.columns:
            findings.append(("missing column", column, len(frame), "", "error"))
    present = [
        column for column in dict.fromkeys([date_col, *key_cols, *columns]) if column in frame
    ]
    for column in present:
        text = frame[column].astype("string")
        add("null token", column, text.isin(NULL_TOKENS), "error")
        add("padded whitespace", column, text.notna() & (text != text.str.strip()), "warn")
        add("thousands separator", column, text.fillna("").str.match(THOUSANDS), "error")
        add("scientific notation", column, text.fillna("").str.match(SCIENTIFIC), "error")
    if date_col in frame:
        dates = frame[date_col].astype("string")
        add("excel serial date", date_col, dates.fillna("").str.match(SERIAL_DATE), "error")

        def unparseable(value):
            try:
                canonical_date(value)
                return False
            except ValueError:
                return True

        add(
            "unparseable date",
            date_col,
            dates.notna() & dates.map(unparseable).astype(bool),
            "error",
        )
    keys = [key for key in key_cols if key in frame]
    if keys:
        add("duplicate key", keys[0], frame.duplicated([date_col, *keys], keep=False), "warn")
        for key in keys:
            digits = frame[key].astype("string").fillna("")
            widths = digits[digits.str.fullmatch(r"\d+")].str.len()
            if widths.nunique() > 1 and (digits.str.startswith("0")).any():
                findings.append(("mixed key widths", key, int(widths.nunique()), "", "warn"))
    return pd.DataFrame(findings, columns=["check", "column", "rows", "example", "severity"])


# >>> text-only clean copy: canonical dates, real nulls, no thousands separators, trimmed <<< #
def normalize_export(
    frame: pd.DataFrame,
    *,
    date_col: str,
    date_parser=canonical_date,
    thousands=(),
    zfill: dict | None = None,
) -> pd.DataFrame:
    clean = frame.astype("string").apply(lambda column: column.str.strip())
    clean = clean.mask(clean.isin(NULL_TOKENS))
    clean[date_col] = clean[date_col].map(
        lambda value: None if pd.isna(value) else date_parser(value)
    )
    for column in thousands:
        clean[column] = clean[column].str.replace(",", "", regex=False)
    for column, width in (zfill or {}).items():
        clean[column] = clean[column].str.zfill(width)
    return clean


# >>> write the extract plus _SOURCE.json (tool, query, rows, sha256) next to it <<< #
def write_extract(frame: pd.DataFrame, path: str | Path, **provenance) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix == ".parquet":
        frame.to_parquet(path, index=False)
    else:
        frame.to_csv(path, index=False)
    record = {
        "file": path.name,
        "rows": len(frame),
        "columns": list(frame.columns),
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "written_at": now(),
        **provenance,
    }
    path.with_name("_SOURCE.json").write_text(json.dumps(record, indent=2))
    return path


# >>> what decides a unit's content apart from its rows: sampling, keys, strictness, vintage, left source <<< #
def fingerprint(manifest: dict) -> str:
    fields = {
        key: manifest.get(key)
        for key in ("sample", "key_cols", "key_families", "strict", "vintage")
    }
    fields["left"] = manifest["left"]
    text = json.dumps(fields, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(text.encode()).hexdigest()[:16]


# >>> one pair from a (possibly left-only) config, with conn_macro profiles expanded exactly as a run does <<< #
def pair_from_config(config: dict, name: str) -> dict:
    copy = json.loads(json.dumps(config))
    return resolve_connections(
        {"connections": copy.get("connections") or {}, "pairs": {name: copy["pairs"][name]}}
    )["pairs"][name]


# >>> a left-only preload config gets a placeholder Right side and an identity col_map from `columns` <<< #
def complete_left_only(pair: dict) -> dict:
    col_map = pair.get("col_map") or {column: column for column in pair["columns"]}
    return {**pair, "right": pair.get("right") or {"source": "databricks"}, "col_map": col_map}


# >>> the manifest a run would build if every given partition matched: same windows, plan, columns <<< #
def preload_manifest(pair_name: str, pair: dict, counts: dict[str, int]) -> dict:
    public = public_pair_config(complete_left_only(pair))
    partitions = {day: {"verdict": "match", "left": n} for day, n in counts.items()}
    document = {"gate_decision": {"passed": True}, "partitions": partitions}
    manifest = build_manifest("preload", pair_name, public, document)
    if manifest["sample"]["mode"] == "fixed":
        raise ValueError(f"{pair_name}: fixed-N sampling cannot be preloaded; use a fraction")
    return manifest


def _unit_key(pair: str, bucket: str, column: str) -> str:
    return f"{pair}/units/{bucket}/{column}.json"


# >>> left partition counts at preload time, optionally limited to known partitions or a cutoff <<< #
def left_counts(pair: dict, *, until: str | None = None, partitions=None) -> dict[str, int]:
    counts = source_from_config(pair["left"]).partition_counts(
        pair.get("fromDate"), pair.get("toDate")
    )
    if until:
        counts = {day: n for day, n in counts.items() if day <= canonical_date(until)}
    if partitions is not None:
        wanted = {canonical_date(day) for day in partitions}
        counts = {day: n for day, n in counts.items() if day in wanted}
    return dict(sorted(counts.items()))


# >>> every (window, column) a preload should hold, and whether the store already has it fresh <<< #
def preload_status(store, pair_name: str, pair: dict, counts: dict[str, int]) -> pd.DataFrame:
    manifest = preload_manifest(pair_name, pair, counts)
    overrides = manifest.get("col_type_overrides") or {}
    rows = []
    for window in manifest["windows"]:
        wanted = {day: counts[day] for day in window["partitions"]}
        for column in manifest["columns"]:
            key = _unit_key(pair_name, window["bucket"], column)
            status = "missing"
            if store.exists(key):
                unit = store.get_json(key)
                status = "fresh"
                if unit["fingerprint"] != fingerprint(manifest):
                    status = "stale: settings"
                elif unit["partition_counts"] != wanted:
                    status = "stale: partitions"
                elif unit.get("col_type") != overrides.get(column):
                    status = "stale: col_type"
            rows.append(
                {
                    "bucket": window["bucket"],
                    "column": column,
                    "partitions": len(wanted),
                    "status": status,
                }
            )
    return pd.DataFrame(rows)


# >>> compute and upload units for windows that are missing or stale (or forced), keeping replaced ones in _history <<< #
def preload_units(
    store,
    pair_name: str,
    pair: dict,
    work_dir: str | Path,
    counts: dict[str, int],
    *,
    force: dict[str, list[str]] | None = None,
) -> pd.DataFrame:
    manifest = preload_manifest(pair_name, pair, counts)
    status = preload_status(store, pair_name, pair, counts)
    forced = {(bucket, column) for bucket, columns in (force or {}).items() for column in columns}
    todo = status[
        (status.status != "fresh") | status.apply(lambda r: (r.bucket, r.column) in forced, axis=1)
    ]
    runtime = LeftRuntime(work_dir)
    overrides = manifest.get("col_type_overrides") or {}
    written = []
    for window in manifest["windows"]:
        columns = set(todo.loc[todo.bucket == window["bucket"], "column"])
        if not columns:
            continue
        stats = runtime.compute_stats(manifest, window, runtime.pull_window(manifest, window))
        for column in sorted(columns):
            key = _unit_key(pair_name, window["bucket"], column)
            if store.exists(key):
                store.put_json(
                    f"{pair_name}/_history/{now()}/{window['bucket']}/{column}.json",
                    store.get_json(key),
                )
            store.put_json(
                key,
                {
                    "pair": pair_name,
                    "bucket": window["bucket"],
                    "column": column,
                    "partition_counts": {day: counts[day] for day in window["partitions"]},
                    "fingerprint": fingerprint(manifest),
                    "col_type": overrides.get(column),
                    "stats": stats[column],
                    "produced_at": now(),
                },
            )
            written.append({"bucket": window["bucket"], "column": column, "action": "written"})
    return pd.DataFrame(written, columns=["bucket", "column", "action"])


# >>> commit every preloaded unit that still fits this run's manifests; the rest stays for col-check <<< #
def adopt_preloaded(run_store, preload_store) -> pd.DataFrame:
    if not run_store.exists("row_check/_CLOSED"):
        raise RuntimeError(
            "the row check is not closed yet: adopt after the Right side releases manifests"
        )
    indexed = {row["unit_id"]: row for _key, row in run_store.list_json("_INDEX/")}
    sequence = max((row["sequence"] for row in indexed.values()), default=0) + 1
    decisions = []
    for key, manifest in run_store.list_json("manifest/"):
        if key.endswith(("_CLOSED", ".keys.json")):
            continue
        pair = manifest["pair"]
        live = run_store.get_json(f"row_check/{pair}.left.json")["counts"]
        overrides = manifest.get("col_type_overrides") or {}
        for window in manifest["windows"]:
            wanted = {day: live[day] for day in window["partitions"]}
            for column in manifest["columns"]:
                uid = unit_id(pair, window["bucket"], column)
                row = {"pair": pair, "bucket": window["bucket"], "column": column}
                preload_key = _unit_key(pair, window["bucket"], column)
                if uid in indexed:
                    decisions.append({**row, "decision": "already indexed"})
                    continue
                if manifest["sample"]["mode"] == "fixed" or not preload_store.exists(preload_key):
                    decisions.append({**row, "decision": "compute: no preload"})
                    continue
                unit = preload_store.get_json(preload_key)
                reason = None
                if unit["fingerprint"] != fingerprint(manifest):
                    reason = "compute: settings changed"
                elif set(unit["partition_counts"]) != set(wanted):
                    reason = "compute: partitions differ"
                elif unit["partition_counts"] != wanted:
                    reason = "compute: counts changed"
                elif unit.get("col_type") != overrides.get(column):
                    reason = "compute: col_type changed"
                if reason:
                    decisions.append({**row, "decision": reason})
                    continue
                run_store.commit_unit(
                    sequence,
                    uid,
                    unit["stats"],
                    {
                        "pair": pair,
                        "window": window["bucket"],
                        "column": column,
                        "right_column": manifest["col_map"][column],
                    },
                )
                sequence += 1
                decisions.append({**row, "decision": f"adopted ({unit['produced_at']})"})
    return pd.DataFrame(decisions)
