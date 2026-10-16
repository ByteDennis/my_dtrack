from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from dtrack_left.common.dates import (
    canonical_date,
    partition_where,
    quote_ident,
    resolve_date_type,
)
from dtrack_left.common.hashing import FAMILIES, NULL, KeyFamily, collapse_decimal, key_family
from dtrack_left.common.manifest import sample_plan as resolve_sample_plan
from dtrack_left.common.sampling import sample_predicate_sql

NORM_SQL = "dtrack_norm_sql"


# >>> the pair's key columns on both sides and the family of each, optionally overridden per Left key <<< #
def key_spec(pair: dict[str, Any], families: dict[str, Any] | None = None):
    keys = [part.strip() for part in str(pair.get("key") or "").split(",") if part.strip()]
    if not keys:
        raise ValueError("the pair has no key; set pair['key'] (e.g. 'ACCT_ID' or 'A,B')")
    configured = {**(pair.get("key_families") or {}), **(families or {})}
    right = [pair["col_map"].get(key, key) for key in keys]
    resolved = []
    date_col = str(pair["left"].get("date_col") or "").lower()
    for key in keys:
        family = configured.get(key)
        if (
            pair.get("partition_mode", "date") == "date"
            and family is None
            and key.lower() == date_col
        ):
            family = {
                "family": "date_type",
                "left_date_type": resolve_date_type(pair["left"].get("date_type")).spec(
                    portable=True
                ),
                "right_date_type": resolve_date_type(pair["right"].get("date_type")).spec(
                    portable=True
                ),
            }
        resolved.append(key_family(family))
    return keys, right, resolved


# >>> Left keys selected by the same query-side sample predicate as production Stage 2 <<< #
def pull_left_keys(
    source,
    pair,
    partitions: list[str],
    output: str | Path,
    keys: list[str],
    families: list[Any] | None = None,
    *,
    sample: dict[str, Any] | None = None,
    key_tuples: list[list[object]] | None = None,
) -> Path:
    left_families = [
        family.spec() if isinstance(family, KeyFamily) else family for family in families or []
    ]
    return source.pull_window(
        partitions,
        [],
        Path(output),
        key_columns=keys,
        sample_plan=sample,
        key_families=left_families,
        side="left",
        key_tuples=key_tuples,
    )


# >>> Right keys selected by production Spark sampling, plus Spark's normalised composite for parity <<< #
def pull_right_keys(
    target,
    partitions,
    output,
    right_keys: list[str],
    families: list[KeyFamily],
    *,
    sample: dict[str, Any] | None = None,
    key_tuples: list[list[object]] | None = None,
):
    if target.config.get("partition_mode", "date") == "date":
        date_col = target.config["date_col"]
        predicate = partition_where(
            date_col, partitions, dialect="spark", date_type=target.config.get("date_type")
        )
    else:
        date_col = None
        predicate = None
    sampled = sample_predicate_sql(
        sample,
        [quote_ident(key, "spark") for key in right_keys],
        families,
        "spark",
        "right",
        key_tuples,
    )
    predicate = " AND ".join(f"({item})" for item in (predicate, sampled) if item)
    selected = list(dict.fromkeys([*([date_col] if date_col else []), *right_keys]))
    projection = [quote_ident(column, "spark") for column in selected]
    composite = ", ".join(
        family.sql(quote_ident(key, "spark"), "right") for key, family in zip(right_keys, families)
    )
    projection.append(f"concat_ws('|', {composite}) AS {NORM_SQL}")
    sql = (
        f"SELECT {', '.join(projection)} FROM {target.relation()} WHERE {target._where(predicate)}"
    )
    target.warehouse.to_csv(sql, output, [*selected, NORM_SQL], operation="key_check")
    return Path(output)


def _day(value: object) -> str:
    try:
        return canonical_date(value)
    except (TypeError, ValueError):
        return str(value)


def _loose(value: object) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    text = str(value).strip()
    try:
        return canonical_date(text)
    except ValueError:
        pass
    text = re.sub(r"[^0-9a-z.]", "", collapse_decimal(text.lower()))
    return re.sub(r"^0+(?=[0-9])", "", text)


# >>> why two raw key values that should be the same row normalise differently, and which family fixes it <<< #
def explain(left: object, right: object) -> tuple[str, str]:
    a, b = ("" if v is None else str(v) for v in (left, right))
    if a == b:
        return "same raw text", "-"
    if a.strip() == b.strip():
        return "surrounding whitespace", "exact / string"
    if a.strip().lower() == b.strip().lower():
        return "letter case", "string"
    for name in ("date", "numeric"):
        family = FAMILIES[name]
        if family.transform(a) and family.transform(a) == family.transform(b):
            reason = "date format" if name == "date" else "leading/trailing zeros or sign"
            return reason, name
    if re.sub(r"[^0-9]", "", a) and re.sub(r"[^0-9]", "", a) == re.sub(r"[^0-9]", "", b):
        return "punctuation around digits", "digits"
    if _loose(a) == _loose(b):
        return "case + punctuation", '["trim", "lower", "alnum"]'
    return "different value", "-"


@dataclass
class KeyCheck:
    pair: str
    window: tuple[str, str]
    left_keys: list[str]
    right_keys: list[str]
    families: list[KeyFamily]
    left: pd.DataFrame
    right: pd.DataFrame
    sample: dict[str, Any] = field(default_factory=lambda: {"mode": "all"})
    selected_keys: list[list[object]] = field(default_factory=list)
    artifacts: dict[str, str] = field(default_factory=dict)
    matched: int = 0
    left_only: pd.DataFrame = field(default_factory=pd.DataFrame)
    right_only: pd.DataFrame = field(default_factory=pd.DataFrame)
    sql_mismatch: pd.DataFrame = field(default_factory=pd.DataFrame)
    suggestions: pd.DataFrame = field(default_factory=pd.DataFrame)

    def summary(self) -> dict[str, Any]:
        return {
            "pair": self.pair,
            "window": "..".join(self.window),
            "sample mode": self.sample["mode"],
            "selected fixed keys": len(self.selected_keys),
            "left rows": len(self.left),
            "right rows": len(self.right),
            "left keys": self.left["norm"].nunique(),
            "right keys": self.right["norm"].nunique(),
            "matched keys": self.matched,
            "left only": len(self.left_only),
            "right only": len(self.right_only),
            "sql != python": len(self.sql_mismatch),
            "ok": not (len(self.left_only) or len(self.right_only) or len(self.sql_mismatch)),
        }


def _normalise(frame, columns, families, side, date_col=None):
    frame = frame.copy()
    parts = [frame[c].map(lambda v, f=f: f.normalize(v, side)) for c, f in zip(columns, families)]
    frame["norm"] = pd.concat(parts, axis=1).agg("|".join, axis=1) if len(frame) else []
    loose = [frame[c].map(_loose) for c in columns]
    frame["loose"] = pd.concat(loose, axis=1).agg("|".join, axis=1) if len(frame) else []
    if date_col:
        frame["day"] = frame[date_col].map(_day) if len(frame) else []
    else:
        frame["day"] = "all"
    return frame


def _distinct(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    grouped = frame.groupby("norm", sort=True)
    first = grouped.first()[[*columns, "loose"]]
    first["rows"] = grouped.size()
    first["days"] = grouped["day"].agg(lambda days: ",".join(sorted(set(days))[:3]))
    return first.reset_index()


# >>> per key column: share of distinct Left values whose normalised form exists on the Right, per family <<< #
def suggest(left, right, left_keys, right_keys, current) -> pd.DataFrame:
    rows = []
    for lk, rk, family in zip(left_keys, right_keys, current):
        lv, rv = left[lk].dropna().unique(), right[rk].dropna().unique()
        candidates = {family.name if family.spec() == family.name else "configured": family}
        candidates |= {name: f for name, f in FAMILIES.items() if name not in candidates}
        for name, candidate in candidates.items():
            rset = {candidate.normalize(v, "right") for v in rv}
            rset.discard(NULL)
            hits = sum(candidate.normalize(v, "left") in rset for v in lv)
            rows.append(
                {
                    "left key": lk,
                    "right key": rk,
                    "family": name,
                    "current": candidate is family,
                    "left values found on right": f"{hits}/{len(lv)}",
                    "rate": round(hits / len(lv), 4) if len(lv) else None,
                }
            )
    frame = pd.DataFrame(rows)
    return frame.sort_values(["left key", "rate"], ascending=[True, False]) if rows else frame


# >>> compare the Left and Right key sets of one window; only_keys narrows both sides to some Left keys <<< #
def compare_keys(
    pair_name: str,
    pair: dict[str, Any],
    left_csv: str | Path,
    right_csv: str | Path,
    *,
    window: tuple[str, str],
    families: dict[str, Any] | None = None,
    only_keys: list[list[object]] | None = None,
) -> KeyCheck:
    left_keys, right_keys, resolved = key_spec(pair, families)
    left = pd.read_csv(left_csv, dtype=object, keep_default_na=False, na_values=[""])
    right = pd.read_csv(right_csv, dtype=object, keep_default_na=False, na_values=[""])
    left = _normalise(left, left_keys, resolved, "left", pair["left"].get("date_col"))
    right = _normalise(right, right_keys, resolved, "right", pair["right"].get("date_col"))
    if only_keys is not None:
        wanted = {"|".join(_loose(v) for v in row) for row in only_keys}
        left, right = left[left["loose"].isin(wanted)], right[right["loose"].isin(wanted)]
    check = KeyCheck(pair_name, window, left_keys, right_keys, resolved, left, right)
    lset, rset = set(left["norm"]), set(right["norm"])
    check.matched = len(lset & rset)
    check.left_only = _distinct(left[~left["norm"].isin(rset)], left_keys)
    check.right_only = _distinct(right[~right["norm"].isin(lset)], right_keys)
    if NORM_SQL in right:
        differ = right[right[NORM_SQL].fillna("") != right["norm"]]
        check.sql_mismatch = differ[[*right_keys, "norm", NORM_SQL]].drop_duplicates()
    check.suggestions = suggest(left, right, left_keys, right_keys, resolved)
    return check


# >>> run the exact production sample on both backends; fixed N keys are chosen once by Left and reused <<< #
def run_sample_check(
    pair_name: str,
    pair: dict[str, Any],
    source,
    target,
    partitions: list[str],
    output: str | Path,
    *,
    window: tuple[str, str] | None = None,
    families: dict[str, Any] | None = None,
    only_keys: list[list[object]] | None = None,
    max_examples: int = 200,
) -> KeyCheck:
    if not partitions:
        raise ValueError(f"{pair_name}: the requested window has no Left partitions")
    work = Path(output)
    work.mkdir(parents=True, exist_ok=True)
    left_keys, right_keys, resolved = key_spec(pair, families)
    left_family_specs = [family.spec() for family in resolved]
    plan = resolve_sample_plan(pair.get("sample"))
    selected_keys: list[list[object]] = []
    artifacts: dict[str, str] = {}
    if plan["mode"] == "fixed":
        fixed_csv = source.sample_keys(
            partitions,
            left_keys,
            work / "fixed.keys.csv",
            count=plan["count"],
            key_families=left_family_specs,
        )
        selected = pd.read_csv(fixed_csv, dtype=object, keep_default_na=False)
        selected_keys = selected[left_keys].drop_duplicates().values.tolist()
        artifacts["fixed_keys"] = str(Path(fixed_csv))
    left_csv = pull_left_keys(
        source,
        pair,
        partitions,
        work / "left.sample.csv",
        left_keys,
        left_family_specs,
        sample=plan,
        key_tuples=selected_keys,
    )
    right_csv = pull_right_keys(
        target,
        partitions,
        work / "right.sample.csv",
        right_keys,
        resolved,
        sample=plan,
        key_tuples=selected_keys,
    )
    check = compare_keys(
        pair_name,
        pair,
        left_csv,
        right_csv,
        window=window or (partitions[0], partitions[-1]),
        families=families,
        only_keys=only_keys,
    )
    check.sample = plan
    check.selected_keys = selected_keys
    report = write_report(check, work / f"{pair_name}.keys.txt", max_examples=max_examples)
    check.artifacts = {
        **artifacts,
        "left_sample": str(left_csv),
        "right_sample": str(right_csv),
        "report": str(report),
        "unmatched": str(report.with_suffix(".csv")),
    }
    return check


def _fmt(value: object) -> str:
    return (
        "NULL" if value is None or (isinstance(value, float) and math.isnan(value)) else repr(value)
    )


def _block(index, row, partner, check, side) -> list[str]:
    head = f"#{index}  {side} key seen {row['rows']} row(s) on day(s) {row['days']}"
    if partner is None:
        return [head, "    no near match on the other side: the row itself is missing", ""]
    lines = [head + f"   near match: {partner['rows']} row(s) on {partner['days']}"]
    left_row, right_row = (row, partner) if side == "LEFT" else (partner, row)
    width = max(len(k) for k in [*check.left_keys, *check.right_keys]) + 2
    lines.append(
        f"    {'left/right column':<{2 * width + 3}} {'LEFT raw':<24} {'RIGHT raw':<24} "
        f"{'LEFT norm':<20} {'RIGHT norm':<20} why -> try family"
    )
    for lk, rk, family in zip(check.left_keys, check.right_keys, check.families):
        lraw, rraw = left_row[lk], right_row[rk]
        lnorm, rnorm = family.normalize(lraw, "left"), family.normalize(rraw, "right")
        mark = "  " if lnorm == rnorm else "!!"
        why, fix = ("ok", "") if lnorm == rnorm else explain(lraw, rraw)
        lines.append(
            f"  {mark}{lk:<{width}} / {rk:<{width}} {_fmt(lraw):<24} {_fmt(rraw):<24} "
            f"{lnorm:<20} {rnorm:<20} {why}{' -> ' + fix if fix else ''}"
        )
    return [*lines, ""]


# >>> plain-text report: header, summary, every unmatched key with its nearest counterpart, SQL parity, suggestions <<< #
def write_report(check: KeyCheck, path: str | Path, max_examples: int = 200) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    families = "  ".join(f"{k}={f.spec()}" for k, f in zip(check.left_keys, check.families))
    lines = [
        f"dtrack key check · pair {check.pair} · {check.window[0]}..{check.window[1]}",
        f"keys      LEFT {','.join(check.left_keys)}  ->  RIGHT {','.join(check.right_keys)}",
        f"families  {families}",
        "",
        *(f"  {name:<14} {value}" for name, value in check.summary().items()),
        "",
    ]
    by_loose = {
        "LEFT": check.right_only.drop_duplicates("loose").set_index("loose"),
        "RIGHT": check.left_only.drop_duplicates("loose").set_index("loose"),
    }
    for side, frame in (("LEFT", check.left_only), ("RIGHT", check.right_only)):
        near = frame["loose"].isin(by_loose[side].index).sum() if len(frame) else 0
        lines.append(f"== {side} ONLY: {len(frame)} key(s), {near} with a near match ==")
        for index, (_, row) in enumerate(frame.head(max_examples).iterrows(), 1):
            partner = (
                by_loose[side].loc[row["loose"]] if row["loose"] in by_loose[side].index else None
            )
            lines += _block(index, row, partner, check, side)
        if len(frame) > max_examples:
            lines.append(f"... {len(frame) - max_examples} more (see the CSV next to this file)")
        lines.append("")
    lines.append(f"== RIGHT: Spark SQL vs Python right_transform: {len(check.sql_mismatch)} ==")
    for _, row in check.sql_mismatch.head(max_examples).iterrows():
        raw = ", ".join(_fmt(row[k]) for k in check.right_keys)
        lines.append(f"  raw {raw:<40} python {row['norm']!r:<30} sql {row[NORM_SQL]!r}")
    lines += ["", "== family suggestions (share of distinct Left values found on the Right) =="]
    for _, row in check.suggestions.iterrows():
        mark = "*" if row["current"] else " "
        lines.append(
            f" {mark} {row['left key']:<20} {row['family']:<12} "
            f"{row['left values found on right']:>14}  {row['rate']}"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    columns = ["norm", "rows", "days"]
    unmatched = pd.concat(
        [
            check.left_only.assign(side="left_only")[["side", *check.left_keys, *columns]],
            check.right_only.assign(side="right_only")[["side", *check.right_keys, *columns]],
        ]
    )
    unmatched.to_csv(path.with_suffix(".csv"), index=False)
    return path
