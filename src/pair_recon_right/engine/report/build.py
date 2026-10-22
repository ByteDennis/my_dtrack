from __future__ import annotations

import csv
import io
import json
import logging
from pathlib import Path
from typing import Any

from ..checks.row_check import row_check_csv_bytes
from ..common.model.protocol import (
    active_unit_ids,
    col_check_close_document,
    current_plan_ids,
    list_failures,
    list_plans,
    list_results,
    list_row_results,
    timestamp,
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

log = logging.getLogger("pair_recon.right.report")


def compare_col_csv_bytes(rows: list[dict[str, Any]]) -> bytes:
    fields = sorted({key for row in rows for key in row})
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=fields)
    writer.writeheader()
    writer.writerows(sorted(rows, key=lambda row: (row["vintage"], row["column"])))
    return output.getvalue().encode()


def collect(store) -> dict[str, Any]:
    documents = [value for _key, value in list_row_results(store)]
    manifests = {value["pair"]: value for _key, value in list_plans(store)}
    active = active_unit_ids(store)
    scoped = bool(manifests)
    results = [
        value for _key, value in list_results(store) if not scoped or value.get("unit_id") in active
    ]
    dead = [
        value
        for _key, value in list_failures(store)
        if not scoped or value.get("unit_id") in active or value.get("source")
    ]
    closed = col_check_close_document(store)
    if not manifests or (closed and closed.get("plan_ids") != current_plan_ids(store)):
        closed = None
    return {
        "documents": documents,
        "results": results,
        "dead": dead,
        "manifests": manifests,
        "closed": closed,
    }


def _summary(run_id: str, state: dict[str, Any], generated_at: str) -> dict[str, Any]:
    documents, results, dead = state["documents"], state["results"], state["dead"]
    expected = int((state["closed"] or {}).get("expected_units") or 0)
    if not expected:
        expected = sum(int(item.get("expected_units") or 0) for item in state["manifests"].values())
    completed = len(results) + len(dead)
    row_count_diff = sum(doc["summary"].get("count_diff", 0) for doc in documents)
    missing = sum(
        doc["summary"].get("left_only", 0) + doc["summary"].get("right_only", 0)
        for doc in documents
    )
    columns_expected = bool(state["manifests"])
    if dead or (expected and completed < expected) or (columns_expected and not state["closed"]):
        status = "PARTIAL"
    else:
        status = "DONE"
    return {
        "run_id": run_id,
        "status": status,
        "generated_at": generated_at,
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


def build_report(
    store,
    out_dir: str | Path,
    *,
    title: str = "Reconciliation Report",
    subtitle: str | None = None,
) -> dict[str, Path]:
    state = collect(store)
    documents = state["documents"]
    if not documents:
        raise ValueError("no row_check documents found in the work store")
    if len(documents) != 1:
        raise ValueError("flat report layout requires exactly one pair")
    document = documents[0]
    run_id = document["run_id"]
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    rows = state["results"]
    results_by_pair = {document["pair"]: rows} if rows else {}
    summary = _summary(run_id, state, timestamp(store))

    files: dict[str, Path] = {"row_check.csv": out / "row_check.csv"}
    files["row_check.csv"].write_bytes(row_check_csv_bytes(document))
    if rows:
        files["col_check.csv"] = out / "col_check.csv"
        files["col_check.csv"].write_bytes(compare_col_csv_bytes(rows))

    left_label = str(document["left"].get("source") or "left")
    right_label = str(document["right"].get("source") or "databricks")
    sections = [
        summary_section(summary),
        '<h2 class="section">Stage 1 · Row counts by partition</h2>\n',
        row_count_table([row_count_section(document)]),
    ]
    if rows:
        vintage = next(iter(state["manifests"].values()), {}).get("vintage", "month")
        sections += [
            '<h2 class="section">Stage 2 · Sampled column statistics</h2>\n',
            column_stats_table(
                [
                    column_stats_section(
                        document["pair"],
                        rows,
                        left_label,
                        right_label,
                        (document["left"].get("date_col"), document["right"].get("date_col")),
                        document.get("partition_mode", "date"),
                    )
                ],
                vintage=vintage,
            ),
        ]

    files["index.html"] = out / "index.html"
    files["index.html"].write_text(
        wrap_document(title, sections, subtitle=subtitle or f"RUN {run_id}"),
        encoding="utf-8",
    )
    files["report.xlsx"] = out / "report.xlsx"
    build_workbook(documents, results_by_pair).save(files["report.xlsx"])
    summary["files"] = sorted(files)
    files["summary.json"] = out / "summary.json"
    files["summary.json"].write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    for name, path in sorted(files.items()):
        log.info("wrote %s (%d bytes)", name, path.stat().st_size)
    return files


CONTENT_TYPES = {
    ".json": "application/json",
    ".csv": "text/csv",
    ".html": "text/html; charset=utf-8",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
}
