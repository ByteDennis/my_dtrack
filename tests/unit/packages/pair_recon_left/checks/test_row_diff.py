import csv
import gzip
import io

from openpyxl import load_workbook

from pair_recon_left.common.checks.row_diff import compare_rows, row_difference_workbook
from pair_recon_left.common.model.hashing import default_normalization_rules


def _gzip_rows(header, rows):
    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(header)
    writer.writerows(rows)
    return gzip.compress(output.getvalue().encode("utf-8"))


MANIFEST = {
    "pair": "orders",
    "key_cols": ["ID", "EFF_DT"],
    "right_key_cols": ["id", "dw_bus_dt"],
    "key_families": [
        "numeric",
        {
            "type": "date",
            "left_date_type": "date",
            "right_date_type": {"type": "string", "format": "compact"},
        },
    ],
    "left": {"source": "oracle", "table": "ORDERS"},
    "right": {"source": "databricks", "table": "orders"},
    "normalization_rules": default_normalization_rules(),
    "sample_rows": 50,
}
DOCUMENT = {
    "pair": "orders",
    "vintage": "2026-08-02",
    "key_columns": ["ID", "EFF_DT"],
    "right_key_columns": ["id", "dw_bus_dt"],
    "pairs": [
        {"left_column": "AMOUNT", "right_column": "amount"},
        {"left_column": "STATUS", "right_column": "status"},
    ],
    "right_rows": 5,
    "right_file": "rows.csv.gz",
}


def _fixture(tmp_path):
    left = tmp_path / "left.csv"
    left.write_text(
        "ID,EFF_DT,AMOUNT,STATUS\n"
        "1,2026-08-02,0.19,Open \n"
        "2,2026-08-02,2.50,closed\n"
        "3,2026-08-02,7,x\n"
        "3,2026-08-02,8,x\n"
        "5,2026-08-02,1.00,\n",
        encoding="utf-8",
    )
    right = _gzip_rows(
        ["_key", "k0", "k1", "v0", "v1", "n0", "n1"],
        [
            ["1|2026-08-02", "1", "20260802", ".19", "OPEN", ".19", "open"],
            ["2|2026-08-02", "2", "20260802", "2.5", "closed", "2.5", "closed"],
            ["3|2026-08-02", "3", "20260802", "7", "x", "7", "x"],
            ["4|2026-08-02", "4", "20260802", "4", "new", "4", "new"],
            ["5|2026-08-02", "5", "20260802", "1", None, "1", None],
        ],
    )
    return left, right


def test_compare_rows_joins_on_the_normalized_key_and_keeps_differences(tmp_path):
    left, right = _fixture(tmp_path)
    result = compare_rows(left, right, DOCUMENT, MANIFEST, numeric={"AMOUNT"}, limit=50)
    assert result["totals"] == {"different": 1, "left_only": 1, "right_only": 1}
    assert result["total"] == 3
    rows = {tuple(row[1:3]): row for row in result["rows"]}
    different = rows[("1", "2026-08-02")]
    assert different[0] == "different"
    assert different[3:7] == ("0.19", "Open ", ".19", "OPEN")
    assert different[7:] == (True, False)
    assert rows[("3", "2026-08-02")][0] == "left_only"
    assert rows[("3", "2026-08-02")][3:5] == ("8", "x")
    assert rows[("4", "20260802")][0] == "right_only"
    assert rows[("4", "20260802")][5:7] == ("4", "new")
    limited = compare_rows(left, right, DOCUMENT, MANIFEST, numeric={"AMOUNT"}, limit=1)
    assert limited["total"] == 3 and len(limited["rows"]) == 1


def test_row_difference_workbook_marks_differing_cells(tmp_path):
    left, right = _fixture(tmp_path)
    result = compare_rows(left, right, DOCUMENT, MANIFEST, numeric={"AMOUNT"}, limit=50)
    body = row_difference_workbook(result, left_label="ORDERS", right_label="orders")
    sheet = load_workbook(io.BytesIO(body))["Rows"]
    assert sheet.cell(1, 1).value == (
        "3 of 3 differing rows (different 1, left only 1, right only 1)"
    )
    assert sheet.cell(1, 4).value == "ORDERS" and sheet.cell(1, 8).value == "orders"
    assert [sheet.cell(2, column).value for column in range(1, 10)] == [
        "ID",
        "EFF_DT",
        "status",
        "AMOUNT",
        "STATUS",
        None,
        None,
        "amount",
        "status",
    ]
    assert [sheet.cell(3, column).value for column in range(1, 10)] == [
        "1",
        "2026-08-02",
        "different",
        "0.19",
        "Open ",
        None,
        None,
        ".19",
        "OPEN",
    ]
    assert sheet["D3"].font.color.rgb == sheet["H3"].font.color.rgb == "009C0006"
    assert sheet["E3"].font.color is None or sheet["E3"].font.color.rgb != "009C0006"
    assert sheet.cell(4, 3).value == "left only" and sheet.cell(5, 3).value == "right only"
