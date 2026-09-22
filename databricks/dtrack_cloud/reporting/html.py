from __future__ import annotations

import html
import json
from datetime import UTC, datetime
from typing import Any

CELL = 'style="border:1px solid #ccc; padding:8px;"'
HEAD = 'style="border:1px solid #ccc; padding:8px; background:#f3f3f3; text-align:left;"'
SMALL = 'style="padding:2px; font-size:10px;"'
TH = 'style="border-bottom:1px solid #ddd; padding:3px; text-align:left;"'
MORE = 'style="padding:2px; font-style:italic; font-size:10px; color:#999;"'


def _e(value: object) -> str:
    return html.escape("—" if value is None or value == "" else str(value))


# >>> 1,234,567 (1.23 M) style row counts <<< #
def format_count(count: int) -> str:
    if count >= 1_000_000_000:
        return f"{count:,} ({count / 1_000_000_000:.2f} B)"
    return f"{count:,} ({count / 1_000_000:.2f} M)"


def _seconds(value: object) -> str:
    return "—" if value in (None, "") else f"{float(value):.1f}s"


def _pair_header(pair: str, span: int) -> str:
    return (
        f'            <tr><td colspan="{span}" style="border:1px solid #ccc; padding:8px; '
        f'background:#e8f0fe; font-weight:600;">{_e(pair)}</td></tr>\n'
    )


def _details_open(span: int, summary_text: str) -> str:
    return (
        f'            <tr><td colspan="{span}" style="border:1px solid #ccc; padding:4px;">\n'
        '                <details style="margin:0;">\n'
        '                    <summary style="cursor:pointer; padding:4px; list-style:none; '
        f'font-weight:500; font-size:12px;">{summary_text}</summary>\n'
        '                    <div style="padding:8px; font-size:12px;">\n'
    )


DETAILS_CLOSE = "                    </div>\n                </details>\n            </td></tr>\n"


def _date_table(
    title: str, left_label: str, right_label: str, rows: list[tuple], hidden: int
) -> str:
    out = f'<p style="margin:4px 0; font-weight:600; text-align:center;">{_e(title)}</p>\n'
    out += '<table style="border-collapse:collapse; font-size:11px; width:100%;">\n'
    out += (
        f"<tr><th {TH}>Date</th><th {TH}>{_e(left_label)}</th>"
        f"<th {TH}>{_e(right_label)}</th><th {TH}>Diff</th></tr>\n"
    )
    for dt, left, right in rows:
        diff = (right or 0) - (left or 0)
        out += (
            f"<tr><td {SMALL}>{_e(dt)}</td><td {SMALL}>{(left or 0):,}</td>"
            f"<td {SMALL}>{(right or 0):,}</td><td {SMALL}>{diff:+,}</td></tr>\n"
        )
    if hidden > 0:
        out += f'<tr><td colspan="4" {MORE}>... and {hidden} more</td></tr>\n'
    return out + "</table>\n"


# >>> one Stage 1 document -> table rows for the row-count report <<< #
def row_count_section(document: dict[str, Any]) -> str:
    pair = document["pair"]
    left, right = document["left"], document["right"]
    left_label = str(left.get("source") or "left")
    right_label = str(right.get("source") or "databricks")
    partitions = document["partitions"]
    timing = document.get("timing") or {}
    by_verdict: dict[str, list] = {"match": [], "count_diff": [], "left_only": [], "right_only": []}
    for dt in sorted(partitions):
        row = partitions[dt]
        by_verdict[row["verdict"]].append((dt, row.get("left"), row.get("right")))
    left_dates = sorted(dt for dt, row in partitions.items() if row.get("left") is not None)
    right_dates = sorted(dt for dt, row in partitions.items() if row.get("right") is not None)
    total_left = sum(int(row["left"]) for row in partitions.values() if row.get("left") is not None)
    total_right = sum(
        int(row["right"]) for row in partitions.values() if row.get("right") is not None
    )
    out = _pair_header(pair, 6)
    for label, side, dates, total, took in (
        (left_label, left, left_dates, total_left, timing.get("left_count_s")),
        (right_label, right, right_dates, total_right, timing.get("right_count_s")),
    ):
        out += (
            f"            <tr><td {CELL}>{_e(label)}</td><td {CELL}>{_e(side.get('date_col'))}</td>"
            f"<td {CELL}>{_e(dates[0] if dates else None)}</td>"
            f"<td {CELL}>{_e(dates[-1] if dates else None)}</td>"
            f"<td {CELL}>{format_count(total)}</td><td {CELL}>{_seconds(took)}</td></tr>\n"
        )
    n_match, n_mismatch = len(by_verdict["match"]), len(by_verdict["count_diff"])
    n_left, n_right = len(by_verdict["left_only"]), len(by_verdict["right_only"])
    mismatch_style = "color:#c62828; font-weight:600;" if n_mismatch else "color:green;"
    summary = (
        f"{_e(left_label)} only: {n_left}, {_e(right_label)} only: {n_right}, "
        f'<span style="color:green;">days match: {n_match}</span>, '
        f'<span style="{mismatch_style}">days mismatch: {n_mismatch}</span>'
    )
    out += _details_open(6, summary)
    if not (n_mismatch or n_left or n_right):
        out += '<p style="margin:8px 0; color:green;">✓ All dates match perfectly!</p>\n'
    else:
        upper_left, upper_right = left_label.upper(), right_label.upper()
        out += '<table style="width:100%; border:none; border-spacing:0;"><tr style="vertical-align:top;">\n'
        blocks = (
            (f"Mismatched Dates ({n_mismatch})", by_verdict["count_diff"], "No mismatches", ""),
            (
                f"{upper_left}-Only Dates ({n_left})",
                by_verdict["left_only"],
                f"No {upper_left}-Only dates",
                "border-left:1px solid #ddd;",
            ),
            (
                f"{upper_right}-Only Dates ({n_right})",
                by_verdict["right_only"],
                f"No {upper_right}-Only dates",
                "border-left:1px solid #ddd;",
            ),
        )
        for title, rows, empty, bar in blocks:
            out += f'<td style="width:32%; padding:0 12px; border:none; {bar}">\n'
            if rows:
                out += _date_table(title, upper_left, upper_right, rows[:10], len(rows) - 10)
            else:
                out += f'<p style="margin:4px 0; color:#999;">{_e(empty)}</p>\n'
            out += "</td>\n"
        out += "</tr></table>\n"
    return out + DETAILS_CLOSE


STAT_DETAILS = (
    ("n_total", "n_total_diff"),
    ("n_missing", "n_missing_diff"),
    ("n_unique", "n_unique_diff"),
    ("mean", "mean_match"),
    ("std", "std_match"),
    ("min", "min_match"),
    ("max", "max_match"),
    ("length_avg", "length_avg_match"),
    ("length_max", "length_max_match"),
    ("hash_sum", "hash_sum_match"),
)


def _stat_differs(row: dict[str, Any], key: str) -> bool:
    value = row.get(key)
    return value is False if key.endswith("_match") else bool(value)


def _fmt(value: object, stat: str = "") -> str:
    if value in (None, ""):
        return "—"
    if stat == "hash_sum":
        return str(value)
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    return f"{int(number):,}" if number == int(number) else f"{number:,.4f}"


# >>> one Pair's comparison rows -> table rows for the column-statistics report <<< #
def column_stats_section(
    pair: str,
    rows: list[dict[str, Any]],
    left_label: str,
    right_label: str,
    date_cols: tuple[str, str],
) -> str:
    vintages = sorted({row["vintage"] for row in rows}, reverse=True)
    columns = sorted({row["column"] for row in rows})
    diff_rows = [row for row in rows if row.get("verdict") == "diff"]
    cols_with_diffs = sorted({row["column"] for row in diff_rows})
    n_cols, n_diff = len(columns), len(cols_with_diffs)
    shown = ", ".join(cols_with_diffs[:4]) + (", ..." if n_diff > 4 else "") if n_diff else "None"
    out = _pair_header(pair, 7)
    for label, date_col in ((left_label, date_cols[0]), (right_label, date_cols[1])):
        out += (
            f"            <tr><td {CELL}>{_e(label)}</td><td {CELL}>{_e(date_col)}</td>"
            f"<td {CELL}>{_e(vintages[-1] if vintages else None)}</td>"
            f"<td {CELL}>{_e(vintages[0] if vintages else None)}</td>"
            f"<td {CELL}>{n_cols}</td><td {CELL}>{len(vintages)}</td><td {CELL}>—</td></tr>\n"
        )
    diff_style = "color:#c62828; font-weight:600;" if n_diff else "color:green;"
    summary = (
        f'{n_cols} columns: <span style="color:green;">{n_cols - n_diff} match</span>, '
        f'<span style="{diff_style}">{n_diff} diff</span> ({_e(shown)})'
    )
    out += _details_open(7, summary)
    if not n_diff:
        out += (
            '<p style="margin:8px 0; color:green;">✓ All columns match across all vintages!</p>\n'
        )
        return out + DETAILS_CLOSE
    blocks = []
    for column in cols_with_diffs:
        per_vintage = [row for row in diff_rows if row["column"] == column]
        first = per_vintage[0]
        right_col = first.get("right_col") or column
        display = f"{column} → {right_col}" if right_col != column else column
        block = (
            f'<p style="margin:4px 0 2px 0; font-weight:600; color:#1f2933; font-size:11px;">'
            f"{_e(display)} ({_e(first.get('col_type'))}) - {len(per_vintage)} diffs:</p>\n"
            '<div style="margin-left:12px; font-size:10px; line-height:1.5;">\n'
        )
        for row in sorted(per_vintage, key=lambda item: item["vintage"], reverse=True)[:3]:
            parts = []
            for stat, key in STAT_DETAILS:
                if _stat_differs(row, key):
                    parts.append(
                        f"{stat} ({_fmt(row.get(f'{stat}_left'), stat)}"
                        f"→{_fmt(row.get(f'{stat}_right'), stat)})"
                    )
            block += f"├─ {_e(row['vintage'])}: {_e(', '.join(parts))}<br>\n"
        if len(per_vintage) > 3:
            block += f"└─ ... +{len(per_vintage) - 3} more<br>\n"
        blocks.append(block + "</div>\n")
    per_group = (len(blocks) + 2) // 3
    out += '<table style="width:100%; border:none;"><tr style="vertical-align:top;">\n'
    for index in range(3):
        out += '<td style="width:33%; padding-right:10px; border:none;">\n'
        out += "".join(blocks[index * per_group : (index + 1) * per_group])
        out += "</td>\n"
    out += "</tr></table>\n"
    return out + DETAILS_CLOSE


def _table(headers: list[str], widths: list[int], sections: list[str]) -> str:
    out = (
        '    <table style="border-collapse:collapse; width:100%; font-family:Segoe UI, Arial, '
        'sans-serif; font-size:12.5px; table-layout:fixed;">\n        <colgroup>\n'
    )
    out += "".join(f'            <col style="width:{width}px;">\n' for width in widths)
    out += "        </colgroup>\n        <thead><tr>\n"
    out += "".join(f"            <th {HEAD}>{_e(name)}</th>\n" for name in headers)
    out += "        </tr></thead>\n        <tbody>\n"
    out += "".join(sections)
    return out + "        </tbody>\n    </table>\n"


# >>> wrap row-count sections in the report table <<< #
def row_count_table(sections: list[str]) -> str:
    return _table(
        ["Environment", "Date Variable", "Min Date", "Max Date", "Row Count", "Query Runtime"],
        [120, 140, 110, 110, 160, 120],
        sections,
    )


# >>> wrap column-statistics sections in the report table <<< #
def column_stats_table(sections: list[str], vintage: str = "month") -> str:
    labels = {
        "day": "# days",
        "week": "# weeks",
        "month": "# months",
        "quarter": "# quarters",
        "year": "# years",
    }
    return _table(
        [
            "Environment",
            "Date Variable",
            "Min Date",
            "Max Date",
            "# Columns",
            labels.get(vintage, "# vintages"),
            "Query Runtime",
        ],
        [120, 140, 110, 110, 100, 100, 120],
        sections,
    )


CSS = """
    <style>
        body { margin: 0; padding: 12px; font-family: Segoe UI, Arial, sans-serif; font-size: 12.5px; }
        summary::-webkit-details-marker { display: none; }
        details[open] summary::before { content: '▼ '; }
        details summary::before { content: '▶ '; }
        div.page-title { font-family: "Segoe UI", Arial, sans-serif; font-size: 28px; font-weight: 600; color: #1f2933; margin-bottom: 6px; }
        div.page-title .update-note { margin-top: 4px; font-size: 13px; color: #6b7280; font-weight: 400; }
        h2.section { font-size: 18px; color: #1f2933; margin: 24px 0 8px 0; }
        .calendar-icon { width: 14px; height: 14px; margin-right: 6px; vertical-align: -2px; stroke: #9CA3AF; stroke-width: 2; fill: none; }
    </style>
"""


# >>> self-contained HTML document with the original dtrack page chrome <<< #
def wrap_document(title: str, sections: list[str], subtitle: str | None = None) -> str:
    now = datetime.now(UTC).strftime("%b %d, %Y %H:%M UTC")
    note = f" | {html.escape(subtitle)}" if subtitle else ""
    out = (
        '<!DOCTYPE html>\n<html lang="en">\n<head>\n    <meta charset="utf-8">\n'
        '    <meta name="viewport" content="width=device-width, initial-scale=1.0">\n'
        f"    <title>{html.escape(title)}</title>\n{CSS}</head>\n<body>\n"
        f'    <div class="page-title">\n        {html.escape(title)}\n'
        '        <div class="update-note">\n'
        '            <svg class="calendar-icon" viewBox="0 0 24 24">'
        '<rect x="3" y="4" width="18" height="18" rx="2" ry="2" />'
        '<line x1="16" y1="2" x2="16" y2="6" /><line x1="8" y1="2" x2="8" y2="6" />'
        '<line x1="3" y1="10" x2="21" y2="10" /></svg>\n'
        f"            Latest update: {now}{note}\n        </div>\n    </div>\n\n"
    )
    out += "".join(sections)
    return out + "\n</body>\n</html>\n"


# >>> summary block rendered above the tables: run status, findings, dead Units <<< #
def summary_section(summary: dict[str, Any]) -> str:
    findings = summary.get("findings") or {}
    dead = summary.get("errors") or []
    status = summary.get("status")
    color = "green" if status == "DONE" else "#c62828"
    out = (
        f'    <p style="font-size:13px;">run <code>{_e(summary.get("run_id"))}</code> '
        f'<span style="color:{color}; font-weight:600;">{_e(status)}</span> · '
        f"{summary.get('pairs')} pair(s) · "
        f"row_count_diff {findings.get('row_count_diff', 0)} · "
        f"partition_missing {findings.get('partition_missing', 0)} · "
        f"col_stat_diff {findings.get('col_stat_diff', 0)} · "
        f"units {summary.get('units_completed', 0)}/{summary.get('units_expected', 0)}"
        f"{' · dead ' + str(len(dead)) if dead else ''}</p>\n"
    )
    if dead:
        out += '    <pre style="font-size:11px; color:#c62828;">'
        out += _e(json.dumps(dead, indent=2, ensure_ascii=False))
        out += "</pre>\n"
    return out
