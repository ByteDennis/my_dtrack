from __future__ import annotations

import csv
import io
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ..checks.row_check import row_check_csv_bytes
from ..common.protocol import (
    col_check_close_document,
    list_failures,
    list_plans,
    list_results,
    list_row_results,
    pair_segment,
)
from .excel import build_workbook
from .html import (
    column_stats_section,
    column_stats_table,
    row_count_section,
    row_count_table,
    summary_section,
    wrap_document,
)

log = logging.getLogger("dtrack.right.report")


# >>> comparison rows -> CSV bytes with a stable column order <<< #
def compare_col_csv_bytes(rows: list[dict[str, Any]]) -> bytes:
    fields = sorted({key for row in rows for key in row})
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=fields)
    writer.writeheader()
    writer.writerows(sorted(rows, key=lambda row: (row["vintage"], row["column"])))
    return output.getvalue().encode()


# >>> read Pair row/column results and the col-check closure from the work store <<< #
def collect(store) -> dict[str, Any]:
    documents = [value for _key, value in list_row_results(store)]
    results = [value for _key, value in list_results(store)]
    dead = [value for _key, value in list_failures(store)]
    manifests = {value["pair"]: value for _key, value in list_plans(store)}
    closed = col_check_close_document(store)
    return {
        "documents": sorted(documents, key=lambda doc: doc["pair"]),
        "results": results,
        "dead": dead,
        "manifests": manifests,
        "closed": closed,
    }


def _summary(run_id: str, state: dict[str, Any], started: datetime) -> dict[str, Any]:
    documents, results, dead = state["documents"], state["results"], state["dead"]
    expected = int((state["closed"] or {}).get("expected_units") or 0)
    if not expected:
        expected = sum(item["expected_units"] for item in state["manifests"].values())
    completed = len(results) + len(dead)
    row_count_diff = sum(doc["summary"].get("count_diff", 0) for doc in documents)
    missing = sum(
        doc["summary"].get("left_only", 0) + doc["summary"].get("right_only", 0)
        for doc in documents
    )
    if dead or (expected and completed < expected) or not state["closed"]:
        status = "PARTIAL"
    else:
        status = "DONE"
    return {
        "run_id": run_id,
        "status": status,
        "generated_at": started.isoformat(),
        "pairs": len(documents),
        "released_pairs": sorted(state["manifests"]),
        "units_expected": expected,
        "units_completed": completed,
        "findings": {
            "row_count_diff": row_count_diff,
            "partition_missing": missing,
            "col_stat_diff": sum(row.get("verdict") == "diff" for row in results),
        },
        "errors": [{"unit_id": item.get("unit_id"), "error": item.get("error")} for item in dead],
    }


# >>> write summary.json, per-pair CSVs, HTML and XLSX into out_dir; return the file map <<< #
def build_report(
    store,
    out_dir: str | Path,
    *,
    title: str = "Reconciliation Report",
    subtitle: str | None = None,
) -> dict[str, Path]:
    started = datetime.now(UTC)
    state = collect(store)
    documents = state["documents"]
    if not documents:
        raise ValueError("no row_check documents found in the work store")
    run_id = documents[0]["run_id"]
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    results_by_pair: dict[str, list[dict[str, Any]]] = {}
    for row in state["results"]:
        results_by_pair.setdefault(row["pair"], []).append(row)
    summary = _summary(run_id, state, started)
    files: dict[str, Path] = {}
    row_sections, col_sections = [], []
    for document in documents:
        pair = document["pair"]
        pair_dir = out / "pairs" / pair_segment(pair)
        pair_dir.mkdir(parents=True, exist_ok=True)
        row_name = f"pairs/{pair_segment(pair)}/row_check.csv"
        files[row_name] = pair_dir / "row_check.csv"
        files[row_name].write_bytes(row_check_csv_bytes(document))
        rows = results_by_pair.get(pair) or []
        if rows:
            col_name = f"pairs/{pair_segment(pair)}/col_check.csv"
            files[col_name] = pair_dir / "col_check.csv"
            files[col_name].write_bytes(compare_col_csv_bytes(rows))
        left_label = str(document["left"].get("source") or "left")
        right_label = str(document["right"].get("source") or "databricks")
        row_sections.append(row_count_section(document))
        if rows:
            col_sections.append(
                column_stats_section(
                    pair,
                    rows,
                    left_label,
                    right_label,
                    (document["left"].get("date_col"), document["right"].get("date_col")),
                )
            )
    vintage = next(iter(state["manifests"].values()), {}).get("vintage", "month")
    sections = [
        summary_section(summary),
        '<h2 class="section">Stage 1 · Row counts by partition</h2>\n',
        row_count_table(row_sections),
    ]
    if col_sections:
        sections += [
            '<h2 class="section">Stage 2 · Sampled column statistics</h2>\n',
            column_stats_table(col_sections, vintage=vintage),
        ]
    files["index.html"] = out / "index.html"
    files["index.html"].write_text(
        wrap_document(title, sections, subtitle=subtitle or f"RUN {run_id}"), encoding="utf-8"
    )
    files["report.xlsx"] = out / "report.xlsx"
    build_workbook(documents, results_by_pair).save(files["report.xlsx"])
    summary["files"] = sorted(files)
    files["summary.json"] = out / "summary.json"
    files["summary.json"].write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n")
    for name, path in sorted(files.items()):
        log.info("wrote %s (%d bytes)", path, path.stat().st_size)
    log.info(
        "report %s: status=%s findings=%s units=%d/%d",
        run_id,
        summary["status"],
        summary["findings"],
        summary["units_completed"],
        summary["units_expected"],
    )
    return files


CONTENT_TYPES = {
    ".json": "application/json",
    ".csv": "text/csv",
    ".html": "text/html; charset=utf-8",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
}
