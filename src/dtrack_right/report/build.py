from __future__ import annotations

import csv
import io
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ..checks.row_check import row_check_csv_bytes
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


# >>> read row_check/, results/, dead/ and manifest/_CLOSED from the work store <<< #
def collect(store) -> dict[str, Any]:
    documents = [
        value
        for key, value in store.list_json("row_check/")
        if key.endswith(".json") and not key.endswith(".left.json") and key != "row_check/_CLOSED"
    ]
    results = [value for _key, value in store.list_json("results/")]
    dead = [value for _key, value in store.list_json("dead/")]
    manifests = {
        value["pair"]: value
        for key, value in store.list_json("manifest/")
        if key != "manifest/_CLOSED" and not key.endswith(".keys.json")
    }
    closed = store.get_json("manifest/_CLOSED") if store.exists("manifest/_CLOSED") else None
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
        files[f"{pair}_compare_row.csv"] = out / f"{pair}_compare_row.csv"
        files[f"{pair}_compare_row.csv"].write_bytes(row_check_csv_bytes(document))
        rows = results_by_pair.get(pair) or []
        if rows:
            files[f"{pair}_compare_col.csv"] = out / f"{pair}_compare_col.csv"
            files[f"{pair}_compare_col.csv"].write_bytes(compare_col_csv_bytes(rows))
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


# >>> immutable history/<run_id>/ first, then the single latest/pointer.json <<< #
def publish_report(
    report_store, run_id: str, files: dict[str, Path], summary: dict[str, Any]
) -> list[str]:
    base = f"history/{run_id}/"
    keys = []
    for name, path in sorted(files.items()):
        key = base + name
        report_store.put_bytes(
            key,
            path.read_bytes(),
            content_type=CONTENT_TYPES.get(path.suffix, "application/octet-stream"),
        )
        keys.append(key)
        log.info("published %s", key)
    pointer = {
        "run_id": run_id,
        "finished_at": datetime.now(UTC).isoformat(),
        "status": summary["status"],
        "findings": sum(summary["findings"].values()),
        "base": base,
    }
    report_store.put_json("latest/pointer.json", pointer)
    keys.append("latest/pointer.json")
    log.info("published latest/pointer.json -> %s", base)
    return keys
