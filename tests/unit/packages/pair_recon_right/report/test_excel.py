from support import STATS

from pair_recon_right.engine.checks.col_check import compare_statistics
from pair_recon_right.engine.report import excel

DOCUMENT = {
    "pair": "a_rather_long_pair_name_over_31",
    "left": {"source": "oracle", "schema": "OWN", "table": "A", "date_col": "DT"},
    "right": {"source": None, "path": "/Volumes/c/s/v/a", "date_col": "dt"},
    "bounds": {"from": "2026-01-01", "to": "2026-01-03"},
    "partitions": {
        "2026-01-01": {"left": 1, "right": 1, "result": "match"},
        "2026-01-02": {"left": 1, "right": 3, "result": "count_diff", "delta": 2},
        "2026-01-03": {"left": 1, "right": None, "result": "left_only"},
    },
    "summary": {"match": 1, "count_diff": 1, "left_only": 1, "right_only": 0},
    "gate_decision": {"passed": True},
}


def test_cell_values_and_relation_names():
    assert excel._clean("a\x00b\x1fc") == "abc" and excel._clean(3) == 3
    assert excel._number("", "mean") is None
    assert excel._number("12345678901234567890", "hash_sum") == "12345678901234567890"
    assert excel._number("2.5", "mean") == 2.5 and excel._number("x\x01", "min") == "x"
    row = {"n_total_diff": 1, "mean_match": False, "std_match": None}
    assert excel._differs(row, "n_total") and excel._differs(row, "mean")
    assert not excel._differs(row, "std") and not excel._differs(row, "col_type")
    assert excel._relation(DOCUMENT["left"]) == "OWN.A"
    assert excel._relation(DOCUMENT["right"]) == "/Volumes/c/s/v/a"


def test_row_sheet_uses_current_result_field():
    workbook = excel.build_workbook([DOCUMENT], {})
    sheet = workbook["Row Check"]

    assert workbook.sheetnames == ["Row Check"]
    assert sheet["A1"].value == "ORACLE:" and sheet["A2"].value == "DATABRICKS:"
    labels = [sheet.cell(row=row, column=1).value for row in range(1, 10)]
    assert "Gate:" not in labels and labels[8] is None
    dates = [sheet.cell(row=row, column=1).value for row in range(11, 14)]
    assert dates == ["2026-01-02", "2026-01-03", "2026-01-01"]
    assert sheet.cell(row=11, column=1).fill.start_color.rgb.endswith("FFC7CE")


def test_col_sheet_preserves_diff_styling():
    diff = compare_statistics(
        pair="p", window="2026-02-01", column="X", left=STATS, right={**STATS, "mean": 9.0}
    )
    same = compare_statistics(pair="p", window="2026-01-01", column="Y", left=STATS, right=STATS)
    diff["right_col"] = "x_r"
    workbook = excel.build_workbook([{**DOCUMENT, "pair": "p"}], {"p": [diff, same]})
    sheet = workbook["Column Check"]
    values = [[cell.value for cell in row] for row in sheet.iter_rows()]

    assert values[0][:2] == ["Vintage: 2026-02-01", "FAIL"]
    fail_row = next(i for i, row in enumerate(values) if row[0] == "Vintage: 2026-02-01")
    assert values[fail_row][1] == "FAIL" and any("x_r" in row for row in values[fail_row:])
    mean_row = next(
        i for i in range(fail_row, len(values)) if values[i][0] == "Mean" and values[i][1] == 1.5
    )
    assert sheet.cell(row=mean_row + 1, column=2).fill.start_color.rgb.endswith("FFC7CE")
    assert next(i for i, row in enumerate(values) if row[0] == "Vintage: 2026-01-01") > fail_row
