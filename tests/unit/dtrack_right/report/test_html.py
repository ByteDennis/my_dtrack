import re

from support import STATS

from dtrack_right.checks.col_check import compare_statistics
from dtrack_right.report import html


def test_scalars():
    assert html.format_count(1_234_567) == "1,234,567 (1.23 M)"
    assert html.format_count(2_500_000_000) == "2,500,000,000 (2.50 B)"
    assert html._e(None) == "—" and html._e("<b>") == "&lt;b&gt;"
    assert html._seconds("") == "—" and html._seconds(1.26) == "1.3s"
    assert html._fmt(None) == "—" and html._fmt("123", "hash_sum") == "123"
    assert html._fmt("n/a") == "n/a" and html._fmt(3.0) == "3" and html._fmt(1.5) == "1.5000"


def _document(partitions, **extra):
    return {
        "pair": "p",
        "left": {"source": "oracle", "date_col": "DT"},
        "right": {"source": "databricks", "date_col": "dt"},
        "partitions": partitions,
        **extra,
    }


def test_perfect_rows():
    section = html.row_count_section(
        _document({"2026-01-01": {"left": 2, "right": 2, "verdict": "match"}})
    )
    assert "All dates match perfectly" in section and "—" in section


def test_row_limit():
    partitions = {
        f"2026-01-{day:02d}": {"left": 1, "right": 2, "verdict": "count_diff"}
        for day in range(1, 13)
    }
    partitions["2026-02-01"] = {"left": None, "right": 4, "verdict": "right_only"}
    section = html.row_count_section(_document(partitions, timing={"left_count_s": 2}))
    assert "Mismatched Dates (12)" in section and "... and 2 more" in section
    assert "DATABRICKS-Only Dates (1)" in section and "No ORACLE-Only dates" in section
    assert "2.0s" in section and "+1" in section


def _diff(column, vintage, **right):
    row = compare_statistics(
        pair="p", window=vintage, column=column, left=STATS, right={**STATS, **right}
    )
    row["right_col"] = column.lower()
    return row


def test_column_diffs():
    rows = [_diff(f"C{i}", "2026-01-01", mean=9.0) for i in range(5)]
    rows += [_diff("C0", f"2026-0{m}-01", hash_sum="5") for m in range(2, 6)]
    section = html.column_stats_section("p", rows, "oracle", "databricks", ("DT", "dt"))
    assert "5 diff</span> (C0, C1, C2, C3, ...)" in section
    assert "C0 → c0 (numeric) - 5 diffs" in section and "... +2 more" in section
    assert "hash_sum (3→5)" in section and "mean (1.5000→9)" in section


def test_clean_columns():
    row = compare_statistics(pair="p", window="2026-01-01", column="C", left=STATS, right=STATS)
    section = html.column_stats_section("p", [row], "oracle", "databricks", ("DT", "dt"))
    assert "All columns match across all vintages" in section


def test_summary_status():
    ok = html.summary_section(
        {
            "status": "DONE",
            "pairs": 2,
            "findings": {"row_count_diff": 1, "partition_missing": 2, "col_stat_diff": 2},
            "units_completed": 11,
            "units_expected": 11,
        }
    )
    assert "background:#2e7d32" in ok and ">DONE</span>" in ok and "DEAD" not in ok
    sep = '<span style="margin:0 5px;">|</span>'
    assert (
        f"2 pair(s){sep}ROW CHECK=3 issues (2 MISSING){sep}COL CHECK=2 issues{sep}UNITS=11/11</p>"
        in ok
    )
    partial = html.summary_section({"status": "PARTIAL", "pairs": 1})
    assert "background:#ef6c00" in partial and "ROW CHECK=0 issues (0 MISSING)" in partial
    bad = html.summary_section(
        {"status": "FAILED", "findings": {}, "errors": [{"unit_id": "u", "error": "<x>"}]}
    )
    assert "DEAD=1 units" in bad and "&lt;x&gt;" in bad and "background:#c62828" in bad


def test_document_escape():
    page = html.wrap_document("A<B", ["<p>body</p>"], subtitle="run <1>")
    assert "<title>A&lt;B</title>" in page and "run &lt;1&gt;" in page
    assert page.rstrip().endswith("</html>") and "<p>body</p>" in page
    stamp = r"Latest run: \d{4}-\d{2}-\d{2} \d{2}:\d{2} E[SD]T"
    assert re.search(stamp + re.escape(html.SEP + "run &lt;1&gt;"), page)
    assert "|" not in html.wrap_document("T", [])
