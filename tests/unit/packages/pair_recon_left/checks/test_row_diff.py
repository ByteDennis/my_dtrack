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
        "5,2026-08-02,1.00,\n"
        "6,2026-08-02,01.20,x\n",
        encoding="utf-8",
    )
    right = _gzip_rows(
        ["_key", "k0", "k1", "v0", "v1", "n0", "n1"],
        [
            ["1|2026-08-02", "1", "20260802", ".19", "OPEN", "0.19", "open"],
            ["2|2026-08-02", "2", "20260802", "2.5", "closed", "2.5", "closed"],
            ["3|2026-08-02", "3", "20260802", "7", "x", "7", "x"],
            ["4|2026-08-02", "4", "20260802", "4", "new", "4", "new"],
            ["5|2026-08-02", "5", "20260802", "1", None, "1", None],
            ["6|2026-08-02", "6", "20260802", "1.3", "x", "1.3", "x"],
        ],
    )
    return left, right


def test_compare_rows_joins_on_the_normalized_key_and_keeps_differences(tmp_path):
    left, right = _fixture(tmp_path)
    result = compare_rows(left, right, DOCUMENT, MANIFEST, numeric={"AMOUNT"}, limit=50)
    assert result["totals"] == {"different": 1, "left_only": 1, "right_only": 1}
    assert result["total"] == 3
    rows = {tuple(row[1:3]): row for row in result["rows"]}
    assert ("1", "2026-08-02") not in rows
    different = rows[("6", "2026-08-02")]
    assert different[0] == "different"
    assert different[3:7] == ("01.20", "x", "1.3", "x")
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
    assert [sheet.cell(5, column).value for column in range(1, 10)] == [
        "6",
        "2026-08-02",
        "different",
        "01.20",
        "x",
        None,
        None,
        "1.3",
        "x",
    ]
    assert sheet["D5"].font.color.rgb == sheet["H5"].font.color.rgb == "009C0006"
    assert sheet["E5"].font.color is None or sheet["E5"].font.color.rgb != "009C0006"
    assert sheet.cell(3, 3).value == "left only" and sheet.cell(4, 3).value == "right only"


def test_all_window_sorts_sampled_keys_before_limiting_to_50(tmp_path):
    left = tmp_path / "left.csv"
    keys = [f"{number:03}" for number in range(61)]
    left.write_text("id,value\n" + "".join(f"{key},left\n" for key in reversed(keys)))
    right = _gzip_rows(
        ["_key", "k0", "v0", "n0"],
        [
            [key, key, "left" if key == "000" else "right", "left" if key == "000" else "right"]
            for key in keys
        ],
    )
    manifest = {
        "partition": "all",
        "key_cols": ["id"],
        "key_families": ["string"],
        "left": {"source": "csv"},
    }
    document = {"key_columns": ["id"], "pairs": [{"left_column": "value", "right_column": "value"}]}
    result = compare_rows(left, right, document, manifest, numeric=set(), limit=50)
    assert result["total"] == 60
    assert [row[1] for row in result["rows"]] == keys[1:51]
