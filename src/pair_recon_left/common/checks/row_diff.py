from __future__ import annotations

import io
import uuid
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from ..model.hashing import active_normalization_rules
from ..runtime.stats import _quote, normalized_value_sql
from ..sql.sampling import composite_sql

STATUS = {"different": "different", "left_only": "left only", "right_only": "right only"}


# >>> join the local Left window CSV with the Right rows of the differing columns on the normalized key; rows whose normalized values differ, plus one-sided keys, come back first N in key order <<< #
def compare_rows(
    left_path: str | Path,
    right_body: bytes,
    document: dict[str, Any],
    manifest: dict[str, Any],
    *,
    numeric: set[str],
    limit: int,
    per_column: bool = False,
) -> dict[str, Any]:
    from ..runtime.duck import connect, temp_directory

    pairs = document["pairs"]
    keys = list(document.get("key_columns") or manifest.get("key_cols") or [])
    left_rules = active_normalization_rules(
        manifest.get("normalization_rules"), (manifest.get("left") or {}).get("source") or "csv"
    )
    comparisons = manifest.get("column_types") or {}
    key_sql = composite_sql(
        [_quote(key) for key in keys], manifest.get("key_families") or [], "duckdb", "left"
    )
    left_values = [
        normalized_value_sql(
            _quote(row["left_column"]),
            "left",
            comparisons.get(row["left_column"]),
            left_rules,
            manifest.get("normalization_date"),
            collapse=row["left_column"] in numeric,
        )
        for row in pairs
    ]
    count = len(pairs)
    with TemporaryDirectory(prefix="pair_recon-rows-") as folder:
        right_path = Path(folder) / "right.csv.gz"
        right_path.write_bytes(right_body)
        workspace = Path(temp_directory()) / f"rows-{uuid.uuid4().hex}.duckdb"
        workspace.parent.mkdir(parents=True, exist_ok=True)
        try:
            with connect(str(workspace), resource_workers=1) as connection:
                connection.execute(
                    "CREATE TABLE left_raw AS SELECT * FROM "
                    f"read_csv({_literal(left_path)}, all_varchar=true, header=true)"
                )
                connection.execute(
                    "CREATE TABLE right_raw AS SELECT * FROM "
                    f"read_csv({_literal(right_path)}, all_varchar=true, header=true)"
                )
                left_select = ", ".join(
                    [
                        f"{key_sql} AS _key",
                        f"row_number() OVER (PARTITION BY {key_sql} ORDER BY rowid) AS _n",
                        *[f"{_quote(key)} AS k{i}" for i, key in enumerate(keys)],
                        *[f"{_quote(row['left_column'])} AS v{i}" for i, row in enumerate(pairs)],
                        *[f"{value} AS n{i}" for i, value in enumerate(left_values)],
                    ]
                )
                right_select = ", ".join(
                    [
                        "_key",
                        "row_number() OVER (PARTITION BY _key ORDER BY rowid) AS _n",
                        *[f"k{i}" for i in range(len(keys))],
                        *[f"v{i}" for i in range(count)],
                        *[f"n{i}" for i in range(count)],
                    ]
                )
                connection.execute(f"CREATE TABLE left_rows AS SELECT {left_select} FROM left_raw")
                connection.execute(
                    f"CREATE TABLE right_rows AS SELECT {right_select} FROM right_raw"
                )
                if per_column:
                    aligned = [
                        "coalesce(l._key, r._key) AS _key",
                        "coalesce(l._n, r._n) AS _n",
                        "l._key IS NULL OR r._key IS NULL AS one_sided",
                        *[f"coalesce(l.k{i}, r.k{i}) AS k{i}" for i in range(len(keys))],
                    ]
                    for index in range(count):
                        aligned.extend(
                            [
                                f"l.v{index} AS lv{index}",
                                f"r.v{index} AS rv{index}",
                                f"l.n{index} IS DISTINCT FROM r.n{index} AS d{index}",
                            ]
                        )
                    connection.execute(
                        f"CREATE TABLE aligned_rows AS SELECT {', '.join(aligned)} "
                        "FROM left_rows l FULL OUTER JOIN right_rows r "
                        "ON l._key = r._key AND l._n = r._n"
                    )
                    for table in ("left_raw", "right_raw", "left_rows", "right_rows"):
                        connection.execute(f"DROP TABLE {table}")
                    column_rows = {}
                    for index, pair in enumerate(pairs):
                        selected = ", ".join(
                            [
                                *[f"k{i}" for i in range(len(keys))],
                                f"lv{index}",
                                f"rv{index}",
                            ]
                        )
                        column_rows[pair["left_column"]] = connection.execute(
                            f"SELECT {selected} FROM aligned_rows "
                            f"WHERE one_sided OR d{index} ORDER BY _key, _n LIMIT {int(limit)}"
                        ).fetchall()
                    return {"keys": keys, "pairs": pairs, "column_rows": column_rows}
                differs = " OR ".join(f"l.n{i} IS DISTINCT FROM r.n{i}" for i in range(count))
                joined = (
                    "FROM left_rows l FULL OUTER JOIN right_rows r "
                    "ON l._key = r._key AND l._n = r._n "
                    f"WHERE l._key IS NULL OR r._key IS NULL OR {differs or 'false'}"
                )
                status = (
                    "CASE WHEN l._key IS NULL THEN 'right_only' "
                    "WHEN r._key IS NULL THEN 'left_only' ELSE 'different' END"
                )
                totals = dict(
                    connection.execute(
                        f"SELECT {status} AS status, count(*) {joined} GROUP BY 1"
                    ).fetchall()
                )
                columns = ", ".join(
                    [
                        f"{status} AS status",
                        *[f"coalesce(l.k{i}, r.k{i}) AS k{i}" for i in range(len(keys))],
                        *[f"l.v{i} AS lv{i}" for i in range(count)],
                        *[f"r.v{i} AS rv{i}" for i in range(count)],
                        *[f"l.n{i} IS DISTINCT FROM r.n{i} AS d{i}" for i in range(count)],
                    ]
                )
                rows = connection.execute(
                    f"SELECT {columns} {joined} "
                    f"ORDER BY coalesce(l._key, r._key), coalesce(l._n, r._n) LIMIT {int(limit)}"
                ).fetchall()
        finally:
            for suffix in ("", ".wal"):
                Path(str(workspace) + suffix).unlink(missing_ok=True)
    return {
        "keys": keys,
        "pairs": pairs,
        "totals": {name: int(totals.get(name, 0)) for name in STATUS},
        "total": int(sum(totals.values())),
        "rows": rows,
        "limit": int(limit),
    }


def _literal(path: str | Path) -> str:
    return "'" + str(Path(path)).replace("'", "''") + "'"


# >>> samples.xlsx: keys, status, Left values, two spacer columns, Right values; cells whose normalized values differ are red <<< #
def row_difference_workbook(result: dict[str, Any], *, left_label: str, right_label: str) -> bytes:
    from openpyxl import Workbook
    from openpyxl.styles import Alignment, Font, PatternFill
    from openpyxl.utils import get_column_letter

    keys, pairs, rows = result["keys"], result["pairs"], result["rows"]
    count = len(pairs)
    left_columns = [row["left_column"] for row in pairs]
    right_columns = [row["right_column"] for row in pairs]
    columns = [*keys, "status", *left_columns, "", "", *right_columns]
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Rows"
    sheet.sheet_view.showGridLines = False
    sheet.freeze_panes = "A3"
    left_start = len(keys) + 2
    right_start = left_start + count + 2
    banner_fill = PatternFill("solid", fgColor="1F4E78")
    banner_font = Font(color="FFFFFF", bold=True)
    difference_font = Font(color="9C0006")
    totals = result["totals"]
    summary = (
        f"{len(rows)} of {result['total']} differing rows "
        f"(different {totals['different']}, left only {totals['left_only']}, "
        f"right only {totals['right_only']})"
    )
    for start, end, label in (
        (1, len(keys) + 1, summary),
        (left_start, left_start + count - 1, left_label),
        (right_start, right_start + count - 1, right_label),
    ):
        if start < end:
            sheet.merge_cells(start_row=1, start_column=start, end_row=1, end_column=end)
        cell = sheet.cell(1, start, label)
        cell.fill, cell.font = banner_fill, banner_font
        cell.alignment = Alignment(horizontal="center")
        for column in range(start + 1, end + 1):
            sheet.cell(1, column).fill = banner_fill
    for column, value in enumerate(columns, start=1):
        cell = sheet.cell(2, column, value)
        cell.alignment = Alignment(horizontal="center")
        if column <= len(keys):
            cell.font = Font(bold=True)
    for number, row in enumerate(rows, start=3):
        status, *values = row
        key_values = values[: len(keys)]
        left_values = values[len(keys) : len(keys) + count]
        right_values = values[len(keys) + count : len(keys) + 2 * count]
        flags = values[len(keys) + 2 * count :]
        for column, value in enumerate(key_values, start=1):
            sheet.cell(number, column, _text(value))
        sheet.cell(number, len(keys) + 1, STATUS[status])
        for offset in range(count):
            left_cell = sheet.cell(number, left_start + offset, _text(left_values[offset]))
            right_cell = sheet.cell(number, right_start + offset, _text(right_values[offset]))
            if flags[offset] or status != "different":
                left_cell.font = right_cell.font = difference_font
    for column, value in enumerate(columns, start=1):
        if not value:
            sheet.column_dimensions[get_column_letter(column)].width = 3
            continue
        widths = (len(str(sheet.cell(row, column).value or "")) for row in range(2, len(rows) + 3))
        sheet.column_dimensions[get_column_letter(column)].width = min(40, max(12, max(widths) + 2))
    output = io.BytesIO()
    workbook.save(output)
    return output.getvalue()


def _text(value: object) -> str:
    return "" if value is None else str(value)
