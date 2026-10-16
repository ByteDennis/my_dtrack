from support import STATS

from dtrack_right.checks.col_check import compare_statistics
from dtrack_right.report import excel

DOCUMENT = {
    "pair": "a_rather_long_pair_name_over_31",
    "left": {"source": "oracle", "schema": "OWN", "table": "A", "date_col": "DT"},
    "right": {"source": None, "path": "/Volumes/c/s/v/a", "date_col": "dt"},
    "bounds": {"from": "2026-01-01", "to": "2026-01-03"},
    "partitions": {
        "2026-01-01": {"left": 1, "right": 1, "verdict": "match"},
        "2026-01-02": {"left": 1, "right": 3, "verdict": "count_diff", "delta": 2},
        "2026-01-03": {"left": 1, "right": None, "verdict": "left_only"},
    },
    "summary": {"match": 1, "count_diff": 1, "left_only": 1, "right_only": 0},
    "gate_decision": {"passed": True},
}


def test_cell_values():
    assert excel._clean("a\x00b\x1fc") == "abc" and excel._clean(3) == 3
    assert excel._number("", "mean") is None
    assert excel._number("12345678901234567890", "hash_sum") == "12345678901234567890"
    assert excel._number("2.5", "mean") == 2.5 and excel._number("x\x01", "min") == "x"
    row = {"n_total_diff": 1, "mean_match": False, "std_match": None}
    assert excel._differs(row, "n_total") and excel._differs(row, "mean")
    assert not excel._differs(row, "std") and not excel._differs(row, "col_type")


def test_relation_name():
    assert excel._relation(DOCUMENT["left"]) == "OWN.A"
    assert excel._relation(DOCUMENT["right"]) == "/Volumes/c/s/v/a"


def test_row_sheet():
    workbook = excel.build_workbook([DOCUMENT], {})
    sheet = workbook["a_rather_long_pair_name_over_31"[:31]]
    assert workbook.sheetnames == ["a_rather_long_pair_name_over_31"[:31]]
    assert sheet["A1"].value == "ORACLE:" and sheet["A2"].value == "DATABRICKS:"
    dates = [sheet.cell(row=r, column=1).value for r in range(12, 15)]
    assert dates == ["2026-01-02", "2026-01-03", "2026-01-01"]
    assert sheet.cell(row=12, column=1).fill.start_color.rgb.endswith("FFC7CE")


def test_all_mode():
    document = {
        **DOCUMENT,
        "partition_mode": "all",
        "left": {"source": "oracle", "table": "A"},
        "right": {"source": "databricks", "table": "a"},
        "bounds": {"from": None, "to": None},
        "partitions": {"all": {"left": 2, "right": 2, "verdict": "match"}},
    }
    sheet = excel.build_workbook([document], {})[DOCUMENT["pair"][:31]]
    assert sheet["A3"].value == "Partition Basis:" and sheet["B3"].value == "all rows"
    assert sheet["B4"].value == "not applicable"


def test_col_sheet():
    diff = compare_statistics(
        pair="p", window="2026-02-01", column="X", left=STATS, right={**STATS, "mean": 9.0}
    )
    same = compare_statistics(pair="p", window="2026-01-01", column="Y", left=STATS, right=STATS)
    diff["right_col"] = "x_r"
    workbook = excel.build_workbook([{**DOCUMENT, "pair": "p"}], {"p": [diff, same]})
    sheet = workbook["p_col"]
    values = [[cell.value for cell in row] for row in sheet.iter_rows()]
    assert values[0][:2] == ["Vintage: 2026-01-01", "PASS"]
    fail_row = next(i for i, row in enumerate(values) if row[0] == "Vintage: 2026-02-01")
    assert values[fail_row][1] == "FAIL" and any("x_r" in row for row in values[fail_row:])
    mean_row = next(
        i for i in range(fail_row, len(values)) if values[i][0] == "Mean" and values[i][1] == 1.5
    )
    assert sheet.cell(row=mean_row + 1, column=2).fill.start_color.rgb.endswith("FFC7CE")
    assert "Length avg" not in [row[0] for row in values]
