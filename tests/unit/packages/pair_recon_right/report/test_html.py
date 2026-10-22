import re

from support import STATS

from pair_recon_right.engine.checks.col_check import compare_statistics
from pair_recon_right.engine.report import html


def _document(partitions, **extra):
    return {
        "pair": "p",
        "left": {"source": "oracle", "date_col": "DT"},
        "right": {"source": "databricks", "date_col": "dt"},
        "partitions": partitions,
        **extra,
    }


def test_scalars():
    assert html.format_count(1_234_567) == "1,234,567 (1.23 M)"
    assert html.format_count(2_500_000_000) == "2,500,000,000 (2.50 B)"
    assert html._e(None) == "—" and html._e("<b>") == "&lt;b&gt;"
    assert html._seconds(1.26) == "1.3s"
    assert html._fmt("123", "hash_sum") == "123" and html._fmt(1.5) == "1.5000"


def test_row_sections_use_current_result_field():
    perfect = html.row_count_section(
        _document({"2026-01-01": {"left": 2, "right": 2, "result": "match"}})
    )
    assert "All dates match perfectly" in perfect

    partitions = {
        f"2026-01-{day:02d}": {"left": 1, "right": 2, "result": "count_diff"}
        for day in range(1, 13)
    }
    partitions["2026-02-01"] = {"left": None, "right": 4, "result": "right_only"}
    section = html.row_count_section(_document(partitions, timing={"left_count_s": 2}))
    assert "Mismatched Dates (12)" in section and "... and 2 more" in section
    assert "DATABRICKS-Only Dates (1)" in section and "2.0s" in section


def test_column_diffs_and_clean_columns():
    rows = []
    for index in range(5):
        row = compare_statistics(
            pair="p",
            window="2026-01-01",
            column=f"C{index}",
            left=STATS,
            right={**STATS, "mean": 9.0},
        )
        row["right_col"] = f"c{index}"
        rows.append(row)
    section = html.column_stats_section("p", rows, "oracle", "databricks", ("DT", "dt"))
    assert "5 diff</span> (C0, C1, C2, C3, ...)" in section
    assert "C0 → c0 (numeric) - 1 diffs" in section and "mean (1.5000→9)" in section

    same = compare_statistics(pair="p", window="2026-01-01", column="C", left=STATS, right=STATS)
    clean = html.column_stats_section("p", [same], "oracle", "databricks", ("DT", "dt"))
    assert "All columns match across all vintages" in clean


def test_summary_and_document_timezone(monkeypatch):
    summary = html.summary_section(
        {
            "status": "DONE",
            "pairs": 1,
            "findings": {"row_count_diff": 1, "partition_missing": 2, "col_stat_diff": 2},
            "units_completed": 11,
            "units_expected": 11,
        }
    )
    assert "background:#2e7d32" in summary and ">DONE</span>" in summary
    assert "ROW CHECK=3 issues (2 MISSING)" in summary

    monkeypatch.setenv("PAIR_RECON_TIMEZONE", "Asia/Tokyo")
    page = html.wrap_document("A<B", ["<p>body</p>"], subtitle="run <1>")
    assert "<title>A&lt;B</title>" in page and "run &lt;1&gt;" in page
    assert re.search(r"Latest run: \d{4}-\d{2}-\d{2} \d{2}:\d{2} JST", page)
