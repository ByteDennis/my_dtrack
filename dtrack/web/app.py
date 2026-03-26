"""FastAPI web UI for dtrack — all routes in one file."""

import io
import json
import os
import sqlite3
import sys
from contextlib import redirect_stdout, redirect_stderr
from datetime import datetime, timedelta
from pathlib import Path
import glob

from fastapi import FastAPI, Request, Query, UploadFile, File
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------

app = FastAPI(title="dtrack")

_dir = Path(__file__).parent
app.mount("/static", StaticFiles(directory=_dir / "static"), name="static")
templates = Jinja2Templates(directory=_dir / "templates")

# These are set by serve() before uvicorn.run()
_db_path: str = ""
_db_dir: str = ""  # directory containing the database — all relative paths resolve from here
_config_path: str = ""
_original_config_path: str = ""  # saved when switching to testing mode
_testing_mode: bool = False


def _resolve(rel_path: str) -> str:
    """Resolve a path relative to the database directory."""
    p = os.path.join(_db_dir, rel_path)
    return os.path.normpath(p)


def _cfg():
    from ..config import load_unified_config
    return load_unified_config(_config_path)


# ---------------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("pairs.html", {"request": request})

@app.get("/pairs", response_class=HTMLResponse)
async def pairs_page(request: Request):
    return templates.TemplateResponse("pairs.html", {"request": request})

@app.get("/load_row", response_class=HTMLResponse)
async def load_row_page(request: Request):
    return templates.TemplateResponse("load_row.html", {"request": request})

@app.get("/row_compare", response_class=HTMLResponse)
async def row_compare_page(request: Request):
    return templates.TemplateResponse("row_compare.html", {"request": request})

@app.get("/col_mapping", response_class=HTMLResponse)
async def col_mapping_page(request: Request):
    return templates.TemplateResponse("col_mapping.html", {"request": request})


# ---------------------------------------------------------------------------
# API routes
# ---------------------------------------------------------------------------

@app.get("/api/status")
async def api_status():
    """Return pairs + metadata + date ranges."""
    from ..db import list_table_pairs, get_metadata, get_column_meta
    from ..config import load_unified_config, get_all_tables_from_unified

    pairs = list_table_pairs(_db_path)
    config = load_unified_config(_config_path)

    result = []
    for pair in pairs:
        pname = pair["pair_name"]
        meta_left = get_metadata(_db_path, pair["table_left"])
        meta_right = get_metadata(_db_path, pair["table_right"])
        cols_left = get_column_meta(_db_path, pair["table_left"])
        cols_right = get_column_meta(_db_path, pair["table_right"])

        pair_cfg = config.get("pairs", {}).get(pname, {})

        result.append({
            "pair_name": pname,
            "table_left": pair["table_left"],
            "table_right": pair["table_right"],
            "source_left": pair.get("source_left", ""),
            "source_right": pair.get("source_right", ""),
            "skip": pair_cfg.get("skip", False),
            "left": {
                "min_date": (meta_left or {}).get("min_date_loaded"),
                "max_date": (meta_left or {}).get("max_date_loaded"),
                "data_type": (meta_left or {}).get("data_type"),
                "row_count": (meta_left or {}).get("row_count_total"),
                "col_count": len(cols_left),
            },
            "right": {
                "min_date": (meta_right or {}).get("min_date_loaded"),
                "max_date": (meta_right or {}).get("max_date_loaded"),
                "data_type": (meta_right or {}).get("data_type"),
                "row_count": (meta_right or {}).get("row_count_total"),
                "col_count": len(cols_right),
            },
            "col_mappings": len(pair.get("col_mappings", {}) or {}),
        })

    return {"pairs": result, "db_path": _db_path, "config_path": _config_path}


@app.post("/api/init")
async def api_init():
    """Initialize or refresh the database."""
    from ..db import init_database, refresh_database

    if os.path.exists(_db_path):
        actions = refresh_database(_db_path)
        return {"action": "refreshed", "details": actions}
    else:
        init_database(_db_path)
        return {"action": "created", "db_path": _db_path}


@app.post("/api/extract")
async def api_extract(request: Request):
    """Run gen-sas or gen-aws extraction."""
    body = await request.json()
    platform = body.get("platform", "aws")  # "sas" or "aws"
    extract_type = body.get("type", "row")  # "row" or "col"
    from_date = body.get("from_date")
    to_date = body.get("to_date")
    outdir = _resolve(body.get("outdir", "./csv/" if platform == "aws" else "./sas/"))
    max_workers = body.get("max_workers")

    buf = io.StringIO()
    try:
        with redirect_stdout(buf), redirect_stderr(buf):
            if platform == "aws":
                from ..platforms.athena import extract_aws
                extract_aws(_config_path, outdir, types=[extract_type],
                            db_path=_db_path, from_date=from_date, to_date=to_date,
                            max_workers=int(max_workers) if max_workers else None)
            else:
                from ..platforms.oracle import gen_sas
                gen_sas(_config_path, outdir, types=[extract_type],
                        db_path=_db_path, from_date=from_date, to_date=to_date)
        return {"ok": True, "output": buf.getvalue()}
    except Exception as e:
        return JSONResponse(status_code=500, content={
            "ok": False, "error": str(e), "output": buf.getvalue()
        })


@app.post("/api/extract/run-sql")
async def api_run_sql_file(request: Request):
    """Execute queries from extract_row.sql via Athena.

    Streams progress as Server-Sent Events (SSE).  Each completed query
    emits a 'progress' event; the final message is a 'done' event with
    summary.  Progress also prints to the terminal (stderr).

    Body: {outdir, max_workers, type}
    """
    import queue
    import threading

    body = await request.json()
    extract_type = body.get("type", "row")
    outdir = _resolve(body.get("outdir", "./csv/"))
    max_workers = body.get("max_workers")

    sql_path = os.path.join(outdir, f"extract_{extract_type}.sql")
    if not os.path.exists(sql_path):
        return JSONResponse(status_code=404, content={
            "ok": False, "error": f"SQL file not found: {sql_path}"
        })

    progress_q = queue.Queue()

    def _on_progress(result):
        progress_q.put(result)

    def _run():
        try:
            from ..platforms.athena import run_sql_file
            results = run_sql_file(
                sql_path, outdir,
                max_workers=int(max_workers) if max_workers else None,
                db_path=_db_path,
                on_progress=_on_progress,
            )
            ok_count = sum(1 for r in results if r.get("ok"))
            fail_count = len(results) - ok_count
            progress_q.put({"_done": True, "ok": fail_count == 0,
                            "results": results, "total": len(results),
                            "succeeded": ok_count, "failed": fail_count})
        except Exception as e:
            progress_q.put({"_done": True, "ok": False, "error": str(e)})

    async def _stream():
        thread = threading.Thread(target=_run, daemon=True)
        thread.start()
        while True:
            # Poll the queue (non-blocking in async context)
            import asyncio
            try:
                msg = progress_q.get_nowait()
            except queue.Empty:
                await asyncio.sleep(0.2)
                continue

            if msg.get("_done"):
                msg.pop("_done")
                yield f"event: done\ndata: {json.dumps(msg)}\n\n"
                break
            else:
                yield f"event: progress\ndata: {json.dumps(msg)}\n\n"

    return StreamingResponse(_stream(), media_type="text/event-stream")


@app.post("/api/generate")
async def api_generate_files(request: Request):
    """Generate SAS and SQL files without running extraction.

    Writes extract_row.sas (for Oracle/SAS tables) and extract_row.sql
    (for AWS tables) to their respective output directories.
    """
    body = await request.json()
    extract_type = body.get("type", "row")
    from_date = body.get("from_date")
    to_date = body.get("to_date")
    sas_outdir = _resolve(body.get("sas_outdir", "./sas/"))
    aws_outdir = _resolve(body.get("aws_outdir", "./csv/"))
    pair_names = body.get("pair_names")  # None = all pairs

    buf = io.StringIO()
    results = {"sas_file": None, "sql_file": None}

    try:
        with redirect_stdout(buf), redirect_stderr(buf):
            from ..config import load_unified_config
            from ..platforms.oracle import load_tables_from_config, inject_where_from_config

            config = load_unified_config(_config_path)

            # Filter config to selected pairs before loading tables
            if pair_names:
                filtered_config = dict(config)
                filtered_config["pairs"] = {
                    k: v for k, v in config.get("pairs", {}).items()
                    if k in pair_names
                }
            else:
                filtered_config = config

            all_tables = load_tables_from_config(filtered_config)
            inject_where_from_config(all_tables, filtered_config)

            # Inject date bounds
            if from_date or to_date:
                for tbl in all_tables:
                    date_col = tbl.get('date_col', '')
                    if not date_col:
                        continue
                    date_type = (tbl.get('date_type') or '').lower()
                    source = tbl.get('source', '').lower()
                    bounds = []
                    if source == 'aws':
                        from ..platforms.athena import _format_athena_date_bound
                        if from_date:
                            bounds.append(f"{date_col} >= {_format_athena_date_bound(from_date, date_type, is_upper=False)}")
                        if to_date:
                            bounds.append(f"{date_col} <= {_format_athena_date_bound(to_date, date_type, is_upper=True)}")
                    else:
                        from ..platforms.oracle import _format_date_bound, is_sas_table
                        is_sas = is_sas_table(tbl)
                        if from_date:
                            bounds.append(f"{date_col} >= {_format_date_bound(from_date, date_type, is_sas, is_upper=False)}")
                        if to_date:
                            bounds.append(f"{date_col} <= {_format_date_bound(to_date, date_type, is_sas, is_upper=True)}")
                    if bounds:
                        extra = " AND ".join(bounds)
                        existing = tbl.get('where', '').strip()
                        tbl['where'] = f"({existing}) AND {extra}" if existing else extra

            # Generate SAS file — use filtered config if pairs selected
            oracle_tables = [t for t in all_tables
                             if t.get('source', '').lower() in ('oracle', 'sas', 'hadoop')]
            if oracle_tables:
                from ..platforms.oracle import gen_sas
                if pair_names:
                    import tempfile
                    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
                        json.dump(filtered_config, tmp, indent=2)
                        tmp_path = tmp.name
                    try:
                        gen_sas(tmp_path, sas_outdir, types=[extract_type],
                                db_path=_db_path, from_date=from_date, to_date=to_date)
                    finally:
                        os.unlink(tmp_path)
                else:
                    gen_sas(_config_path, sas_outdir, types=[extract_type],
                            db_path=_db_path, from_date=from_date, to_date=to_date)
                results["sas_file"] = os.path.join(sas_outdir, f"extract_{extract_type}.sas")

            # Generate SQL file
            aws_tables = [t for t in all_tables
                          if t.get('source', '').lower() == 'aws']
            if aws_tables:
                from ..platforms.athena import _write_combined_sql
                os.makedirs(aws_outdir, exist_ok=True)
                _write_combined_sql(aws_tables, aws_outdir, extract_type)
                results["sql_file"] = os.path.join(aws_outdir, f"extract_{extract_type}.sql")

            # Discover columns for AWS tables (Oracle/SAS handled by gen_sas above)
            if aws_tables:
                from ..platforms.oracle import _discover_and_write_columns
                _discover_and_write_columns(aws_tables, aws_outdir, _db_path)

        return {"ok": True, "output": buf.getvalue(), **results}
    except Exception as e:
        return JSONResponse(status_code=500, content={
            "ok": False, "error": str(e), "output": buf.getvalue()
        })


@app.post("/api/load")
async def api_load(request: Request):
    """Run load-row or load-col."""
    body = await request.json()
    load_type = body.get("type", "row")  # "row" or "col"
    folder = _resolve(body.get("folder", "./csv/"))

    buf = io.StringIO()
    try:
        with redirect_stdout(buf), redirect_stderr(buf):
            from ..config import load_unified_config, get_all_tables_from_unified
            from ..platforms.base import qualified_name

            config = load_unified_config(_config_path)
            tables = get_all_tables_from_unified(config)

            if load_type == "row":
                from ..loader import load_row_counts
                from ..db import get_row_counts, insert_column_meta
                import csv as csv_mod
                for tbl in tables:
                    qname = qualified_name(tbl)
                    csv_path = os.path.join(folder, f"{qname}_row.csv")
                    if not os.path.exists(csv_path):
                        continue
                    load_row_counts(
                        db_path=_db_path, file_or_folder=csv_path,
                        table_name=qname, mode="upsert",
                        source=tbl.get("source"),
                        date_col=tbl.get("date_col"),
                        where_clause=tbl.get("where", ""),
                    )
                    rows = get_row_counts(_db_path, qname)
                    print(f"{qname}: {len(rows)} date buckets loaded")

                    # Also load column metadata if available
                    col_csv = os.path.join(folder, f"{qname}_columns.csv")
                    if os.path.exists(col_csv):
                        columns = {}
                        with open(col_csv, 'r', newline='') as f:
                            reader = csv_mod.DictReader(f)
                            for row in reader:
                                col_name = row.get('column_name') or row.get('COLUMN_NAME', '')
                                dtype = row.get('data_type') or row.get('DATA_TYPE', '')
                                if col_name:
                                    columns[col_name] = dtype
                        if columns:
                            insert_column_meta(_db_path, qname, columns, source=tbl.get("source"))
                            print(f"{qname}: {len(columns)} columns loaded")
            else:
                from ..loader import load_precomputed_col_stats
                from ..db import get_metadata
                for tbl in tables:
                    qname = qualified_name(tbl)
                    csv_path = os.path.join(folder, f"{qname}_col.csv")
                    if not os.path.exists(csv_path):
                        continue
                    table_vintage = tbl.get("vintage")
                    if not table_vintage:
                        meta = get_metadata(_db_path, qname)
                        table_vintage = (meta.get("vintage") or "day") if meta else "day"
                    count = load_precomputed_col_stats(
                        db_path=_db_path, csv_path=csv_path,
                        table_name=qname, mode="upsert",
                        source=tbl.get("source"), vintage=table_vintage,
                    )
                    print(f"{qname}: {count} stat rows loaded")

        return {"ok": True, "output": buf.getvalue()}
    except Exception as e:
        return JSONResponse(status_code=500, content={
            "ok": False, "error": str(e), "output": buf.getvalue()
        })


@app.post("/api/compare/row")
async def api_compare_row(request: Request):
    """Run compare-row and return results."""
    body = await request.json()
    from_date = body.get("from_date")
    to_date = body.get("to_date")

    buf = io.StringIO()
    try:
        with redirect_stdout(buf), redirect_stderr(buf):
            from argparse import Namespace
            from ..cli import cmd_compare_row
            args = Namespace(
                project_db=_db_path, config=_config_path,
                from_date=from_date, to_date=to_date,
                yes=True, html=None, title=None, subtitle=None,
            )
            cmd_compare_row(args)
        return {"ok": True, "output": buf.getvalue()}
    except Exception as e:
        return JSONResponse(status_code=500, content={
            "ok": False, "error": str(e), "output": buf.getvalue()
        })


@app.post("/api/compare/col")
async def api_compare_col(request: Request):
    """Run compare-col and return results."""
    body = await request.json()
    from_date = body.get("from_date")
    to_date = body.get("to_date")

    buf = io.StringIO()
    try:
        with redirect_stdout(buf), redirect_stderr(buf):
            from argparse import Namespace
            from ..cli import cmd_compare_col
            args = Namespace(
                project_db=_db_path, config=_config_path,
                from_date=from_date, to_date=to_date,
                no_date_filter=False, vintage=None,
                yes=True, html=None, title=None, subtitle=None,
            )
            cmd_compare_col(args)
        return {"ok": True, "output": buf.getvalue()}
    except Exception as e:
        return JSONResponse(status_code=500, content={
            "ok": False, "error": str(e), "output": buf.getvalue()
        })


@app.post("/api/query")
async def api_query(request: Request):
    """Run a SQL query against the database (read + write)."""
    body = await request.json()
    sql = body.get("sql", "").strip()

    if not sql:
        return JSONResponse(status_code=400, content={"error": "Empty SQL"})

    try:
        conn = sqlite3.connect(_db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(sql)

        first_word = sql.split()[0].upper() if sql else ""
        if first_word in ("SELECT", "PRAGMA", "EXPLAIN", "WITH"):
            rows = cursor.fetchall()
            conn.close()
            if not rows:
                return {"columns": [], "rows": []}
            columns = list(rows[0].keys())
            data = [dict(row) for row in rows]
            return {"columns": columns, "rows": data}
        else:
            rowcount = cursor.rowcount
            conn.commit()
            conn.close()
            return {"columns": [], "rows": [],
                    "message": f"{first_word} executed ({rowcount} row(s) affected)"}
    except sqlite3.Error as e:
        return JSONResponse(status_code=400, content={"error": str(e)})


@app.get("/api/report/{report_type}")
async def api_report(report_type: str):
    """Serve generated HTML report."""
    output_dir = _resolve("output")
    filename = f"compare_{report_type}.html"
    path = os.path.join(output_dir, filename)
    if os.path.exists(path):
        return FileResponse(path, media_type="text/html")
    return JSONResponse(status_code=404, content={"error": f"Report not found: {filename}"})


# ---------------------------------------------------------------------------
# Config management
# ---------------------------------------------------------------------------

@app.get("/api/config")
async def api_get_config():
    """Return the current config JSON."""
    try:
        with open(_config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return JSONResponse(status_code=404, content={"error": "Config not found"})


@app.post("/api/config/upload")
async def api_upload_config(file: UploadFile = File(...)):
    """Upload and replace the config JSON file.

    Also registers pairs and metadata into the database.
    """
    try:
        content = await file.read()
        config = json.loads(content)

        # Validate it has a pairs section
        if "pairs" not in config:
            return JSONResponse(status_code=400, content={
                "error": "Invalid config: missing 'pairs' key"
            })

        # Write to disk
        with open(_config_path, 'w') as f:
            json.dump(config, f, indent=2)

        # Register pairs into the database
        registered = _sync_config_pairs(config)

        return {"ok": True, "pairs": len(config["pairs"]), "registered": registered}
    except json.JSONDecodeError as e:
        return JSONResponse(status_code=400, content={"error": f"Invalid JSON: {e}"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.put("/api/config")
async def api_update_config(request: Request):
    """Partial update to config (merge into existing)."""
    body = await request.json()
    try:
        from ..config import load_unified_config, save_unified_config
        config = load_unified_config(_config_path)

        # Merge pairs
        if "pairs" in body:
            for pname, pcfg in body["pairs"].items():
                if pname in config["pairs"]:
                    config["pairs"][pname].update(pcfg)
                else:
                    config["pairs"][pname] = pcfg

        # Merge top-level metadata and settings
        if "metadata" in body:
            config.setdefault("metadata", {}).update(body["metadata"])
        if "settings" in body:
            config.setdefault("settings", {}).update(body["settings"])

        save_unified_config(config, _config_path)
        _sync_config_pairs(config)

        return {"ok": True}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


# ---------------------------------------------------------------------------
# Pair management (new UI)
# ---------------------------------------------------------------------------

@app.post("/api/pairs/reload")
async def api_reload_pairs():
    """Wipe all pairs and reload from the current config JSON on disk."""
    try:
        from ..config import load_unified_config
        config = load_unified_config(_config_path)

        # Wipe existing pairs in config, then re-read from file
        # (this forces a clean slate from whatever config file is active)
        n_pairs = len(config.get("pairs", {}))

        # Re-sync into database
        registered = _sync_config_pairs(config)

        return {"ok": True, "pairs": n_pairs, "registered": registered,
                "config_path": _config_path}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/api/pairs/list")
async def api_list_pairs():
    """List all pairs with their full configuration."""
    try:
        from ..config import load_unified_config
        config = load_unified_config(_config_path)

        pairs_list = []
        for pair_name, pair_cfg in config.get("pairs", {}).items():
            # Normalize processed: list → newline-joined string for web UI
            for side in ("left", "right"):
                side_cfg = pair_cfg.get(side, {})
                p = side_cfg.get("processed", "")
                if isinstance(p, list):
                    side_cfg["processed"] = "\n".join(p)
            pairs_list.append({
                "name": pair_name,
                "description": pair_cfg.get("description", ""),
                "left": pair_cfg.get("left", {}),
                "right": pair_cfg.get("right", {}),
                "mode": pair_cfg.get("mode", "incremental"),
                "dateRangeMode": pair_cfg.get("dateRangeMode", "global"),
                "fromDate": pair_cfg.get("fromDate", ""),
                "toDate": pair_cfg.get("toDate", ""),
                "overlap": pair_cfg.get("overlap", 7),
                "lastLoaded": pair_cfg.get("lastLoaded", ""),
                "selected": False,
                "expanded": False,
                "validated": True,
            })

        return {"pairs": pairs_list}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.put("/api/pairs/{pair_name}")
async def api_update_pair(pair_name: str, request: Request):
    """Update an existing pair configuration."""
    body = await request.json()

    try:
        from ..config import load_unified_config, save_unified_config
        from ..platforms.base import qualified_name
        from ..db import register_table_pair, update_metadata
        config = load_unified_config(_config_path)

        if pair_name not in config.get("pairs", {}):
            return JSONResponse(status_code=404, content={"error": f"Pair '{pair_name}' not found"})

        pair_cfg = config["pairs"][pair_name]

        # Update only valid pair-config keys (not frontend-only fields like pair_name)
        _PAIR_KEYS = {"left", "right", "col_map", "description", "mode",
                       "fromDate", "toDate", "dateRangeMode", "overlap",
                       "skip", "ignore_rows", "ignore_columns",
                       "col_type_overrides", "where_map", "time_map",
                       "comment_map", "diff_map"}
        for key in _PAIR_KEYS:
            if key in body:
                pair_cfg[key] = body[key]

        for side in ("left", "right"):
            if side in pair_cfg:
                _prepare_side(pair_cfg[side])

        save_unified_config(config, _config_path)

        # Inject name for DB registration (auto-derived, not persisted)
        left = pair_cfg.get("left", {})
        right = pair_cfg.get("right", {})
        left["name"] = pair_name
        right["name"] = pair_name
        if left.get("source") and right.get("source"):
            table_left = qualified_name(left)
            table_right = qualified_name(right)
            register_table_pair(
                _db_path, pair_name, table_left, table_right,
                source_left=left.get("source"),
                source_right=right.get("source"),
                col_mappings=pair_cfg.get("col_map") or None,
            )
            for tbl_cfg in [left, right]:
                qname = qualified_name(tbl_cfg)
                update_metadata(_db_path, {
                    "table_name": qname,
                    "source": tbl_cfg.get("source"),
                    "source_table": tbl_cfg.get("table"),
                    "date_var": tbl_cfg.get("date_col"),
                    "data_type": "row",
                })

        return {"ok": True, "pair_name": pair_name}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/api/scan/csv")
async def api_scan_csv(dir: str = "./output/", type: str = "row"):
    """Scan directory for CSV files."""
    try:
        from pathlib import Path

        dir_path = Path(_resolve(dir))
        if not dir_path.exists():
            return {"files": []}

        pattern = f"*_{type}.csv"
        files = []

        for csv_file in dir_path.glob(pattern):
            stats = csv_file.stat()
            parts = csv_file.stem.split('_')

            # Parse filename: "customer_daily_left_row.csv"
            side = 'left' if 'left' in parts else 'right' if 'right' in parts else 'unknown'
            pair = '_'.join(p for p in parts if p not in ['left', 'right', 'row', 'col'])

            # Count rows and dates (basic check)
            try:
                with open(csv_file) as f:
                    lines = f.readlines()
                    rows = len(lines) - 1  # minus header
                    dates = rows  # Approximate for now
            except:
                rows = 0
                dates = 0

            from datetime import datetime
            modified = datetime.fromtimestamp(stats.st_mtime)
            time_diff = datetime.now() - modified
            if time_diff.seconds < 60:
                modified_str = f"{time_diff.seconds}s ago"
            elif time_diff.seconds < 3600:
                modified_str = f"{time_diff.seconds // 60}m ago"
            elif time_diff.days == 0:
                modified_str = f"{time_diff.seconds // 3600}h ago"
            else:
                modified_str = f"{time_diff.days}d ago"

            files.append({
                "file": csv_file.name,
                "path": str(csv_file),
                "pair": pair,
                "side": side,
                "rows": rows,
                "dates": dates,
                "modified": modified_str,
                "selected": True
            })

        return {"files": files}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.post("/api/load/row")
async def api_load_row_files(request: Request):
    """Load row CSV files into database."""
    body = await request.json()
    files = body.get("files", [])
    mode = body.get("mode", "upsert")

    try:
        from ..loader import load_row_counts
        from ..config import load_unified_config, get_all_tables_from_unified
        from ..platforms.base import qualified_name

        config = load_unified_config(_config_path)
        loaded = 0

        for file_path in files:
            # Infer table info from filename
            filename = Path(file_path).stem

            # Find matching table in config
            for tbl in get_all_tables_from_unified(config):
                qname = qualified_name(tbl)
                if qname.lower() in filename.lower():
                    load_row_counts(
                        db_path=_db_path,
                        file_or_folder=file_path,
                        table_name=qname,
                        mode=mode,
                        source=tbl.get("source"),
                        date_col=tbl.get("date_col"),
                        where_clause=tbl.get("where", ""),
                    )
                    loaded += 1
                    break

        return {"ok": True, "loaded": loaded}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.post("/api/load/row/upload")
async def api_load_row_upload(
    file: UploadFile = File(...),
    table_name: str = "",
    mode: str = "upsert",
):
    """Upload a CSV file and load row counts into a specific table.

    Accepts multipart form data with:
      - file: the CSV file
      - table_name: target table name (e.g. 'oracle_cust_daily')
      - mode: 'upsert' (default) or 'replace'
    """
    import tempfile

    if not table_name:
        return JSONResponse(status_code=400, content={"error": "table_name is required"})

    try:
        # Write uploaded file to temp location
        content = await file.read()
        with tempfile.NamedTemporaryFile(
            mode='wb', suffix='.csv', delete=False
        ) as tmp:
            tmp.write(content)
            tmp_path = tmp.name

        # Count existing rows for this table to compute new vs updated
        from ..db import get_row_counts, get_metadata
        existing_rows = {}
        try:
            for dt, count in get_row_counts(_db_path, table_name):
                existing_rows[dt] = count
        except Exception:
            pass  # table may not exist yet

        # Find table config for metadata
        from ..config import load_unified_config, get_all_tables_from_unified
        from ..platforms.base import qualified_name

        config = load_unified_config(_config_path)
        tbl_cfg = None
        for tbl in get_all_tables_from_unified(config):
            if qualified_name(tbl).lower() == table_name.lower():
                tbl_cfg = tbl
                break

        from ..loader import load_row_counts
        # date_col=None lets the CSV parser auto-detect (date_value, dt, date, etc.)
        # date_var_override stores the source DB column name in metadata
        load_row_counts(
            db_path=_db_path,
            file_or_folder=tmp_path,
            table_name=table_name,
            mode=mode,
            source=tbl_cfg.get("source") if tbl_cfg else None,
            date_col=None,
            date_var_override=tbl_cfg.get("date_col") if tbl_cfg else None,
            where_clause=tbl_cfg.get("where", "") if tbl_cfg else "",
        )

        # Compute new vs updated dates
        new_rows = {}
        try:
            for dt, count in get_row_counts(_db_path, table_name):
                new_rows[dt] = count
        except Exception:
            pass

        new_dates = len(set(new_rows.keys()) - set(existing_rows.keys()))
        updated_dates = len(set(new_rows.keys()) & set(existing_rows.keys()))

        # Clean up temp file
        os.unlink(tmp_path)

        return {
            "ok": True,
            "loaded": len(new_rows),
            "new_dates": new_dates,
            "updated_dates": updated_dates,
        }
    except Exception as e:
        import traceback
        return JSONResponse(status_code=500, content={
            "ok": False,
            "error": f"{type(e).__name__}: {e}",
        })


@app.post("/api/load/columns/upload")
async def api_load_columns_upload(
    file: UploadFile = File(...),
    table_name: str = "",
    source: str = "",
):
    """Upload a column metadata CSV and load into _column_meta.

    CSV should have headers: COLUMN_NAME, DATA_TYPE (or column_name, data_type).
    """
    import csv as csv_mod

    if not table_name:
        return JSONResponse(status_code=400, content={"error": "table_name is required"})

    try:
        from ..db import insert_column_meta

        content = await file.read()
        text = content.decode("utf-8")
        reader = csv_mod.DictReader(io.StringIO(text))
        columns = {}
        for row in reader:
            col_name = row.get('column_name') or row.get('COLUMN_NAME', '')
            dtype = row.get('data_type') or row.get('DATA_TYPE', '')
            if col_name:
                columns[col_name] = dtype

        if not columns:
            return JSONResponse(status_code=400, content={"error": "No columns found in CSV"})

        count = insert_column_meta(_db_path, table_name, columns, source=source or None)
        return {"ok": True, "loaded": count, "table_name": table_name}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.post("/api/load/row/path")
async def api_load_row_path(request: Request):
    """Load a server-side CSV file by path into a specific table.

    Body: {path, table_name, mode}
    """
    body = await request.json()
    csv_path = body.get("path", "")
    if csv_path and not os.path.isabs(csv_path):
        csv_path = _resolve(csv_path)
    table_name = body.get("table_name", "")
    mode = body.get("mode", "upsert")

    if not csv_path or not table_name:
        return JSONResponse(status_code=400, content={
            "error": "path and table_name are required"
        })

    if not os.path.exists(csv_path):
        return JSONResponse(status_code=404, content={
            "error": f"File not found: {csv_path}"
        })

    try:
        from ..db import get_row_counts
        from ..config import load_unified_config, get_all_tables_from_unified
        from ..platforms.base import qualified_name

        # Count existing rows
        existing_rows = {}
        try:
            for dt, count in get_row_counts(_db_path, table_name):
                existing_rows[dt] = count
        except Exception:
            pass

        # Find table config
        config = load_unified_config(_config_path)
        tbl_cfg = None
        for tbl in get_all_tables_from_unified(config):
            if qualified_name(tbl).lower() == table_name.lower():
                tbl_cfg = tbl
                break

        from ..loader import load_row_counts
        # date_col=None lets the CSV parser auto-detect (date_value, dt, date, etc.)
        load_row_counts(
            db_path=_db_path,
            file_or_folder=csv_path,
            table_name=table_name,
            mode=mode,
            source=tbl_cfg.get("source") if tbl_cfg else None,
            date_col=None,
            date_var_override=tbl_cfg.get("date_col") if tbl_cfg else None,
            where_clause=tbl_cfg.get("where", "") if tbl_cfg else "",
        )

        # Compute new vs updated
        new_rows = {}
        try:
            for dt, count in get_row_counts(_db_path, table_name):
                new_rows[dt] = count
        except Exception:
            pass

        new_dates = len(set(new_rows.keys()) - set(existing_rows.keys()))
        updated_dates = len(set(new_rows.keys()) & set(existing_rows.keys()))

        return {
            "ok": True,
            "loaded": len(new_rows),
            "new_dates": new_dates,
            "updated_dates": updated_dates,
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"ok": False, "error": str(e)})


@app.get("/api/scan/folder")
async def api_scan_folder(dir: str = "./csv/"):
    """Scan a server-side folder for CSV files and return file info."""
    try:
        dir_path = Path(_resolve(dir))
        if not dir_path.exists():
            return JSONResponse(status_code=404, content={
                "error": f"Folder not found: {dir}"
            })

        files = []
        for csv_file in sorted(dir_path.glob("*.csv")):
            # Read first few lines for row count and date range
            rows = 0
            min_date = ''
            max_date = ''
            try:
                with open(csv_file, 'r') as f:
                    import csv as csv_mod
                    reader = csv_mod.DictReader(f)
                    headers = [h.lower().strip() for h in (reader.fieldnames or [])]
                    date_aliases = {'dt', 'date', 'rpg_dt', 'eff_dt', 'run_date',
                                    'snap_dt', 'snapshot_dt', 'date_value'}
                    date_col = None
                    for h in headers:
                        if h in date_aliases:
                            date_col = h
                            break

                    dates = []
                    for row in reader:
                        rows += 1
                        if date_col and row.get(date_col):
                            dates.append(row[date_col].strip())

                    if dates:
                        dates.sort()
                        min_date = dates[0]
                        max_date = dates[-1]
            except Exception:
                pass

            files.append({
                "name": csv_file.name,
                "path": str(csv_file.resolve()),
                "rows": rows,
                "minDate": min_date,
                "maxDate": max_date,
            })

        return {"files": files}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


# ---------------------------------------------------------------------------
# Row Compare API
# ---------------------------------------------------------------------------

# NOTE: Export routes must be defined BEFORE {pair_name} routes to avoid
# FastAPI matching "export" as a pair_name parameter.

@app.post("/api/compare/row/export/html")
async def api_compare_row_export_html(request: Request):
    """Generate full HTML report for all pairs.

    Body (optional): {
        "from_date": "", "to_date": "",
        "pairs": {
            "pair_name": {
                "excluded_dates": [...],
                "comment_left": "", "comment_right": "",
                "time_left": "", "time_right": ""
            }, ...
        }
    }

    When pairs dict is provided, uses live UI state for exclusions/annotations.
    Otherwise falls back to saved DB/config state.
    """
    body = await request.json() if request.headers.get("content-type", "").startswith("application/json") else {}
    from_date = body.get("from_date", "")
    to_date = body.get("to_date", "")
    title = body.get("title", "") or "Row Count Comparison"
    subtitle = body.get("subtitle", "") or None
    live_pairs = body.get("pairs", {})

    try:
        from ..db import list_table_pairs, get_metadata, get_row_comparison
        from ..compare import compare_row_counts
        from ..html_export import generate_row_count_html, create_row_count_table, wrap_html_document
        from ..config import load_unified_config

        pairs = list_table_pairs(_db_path)
        config = load_unified_config(_config_path)
        sections = []

        for pair in pairs:
            pname = pair["pair_name"]
            pair_cfg = config.get("pairs", {}).get(pname, {})
            if pair_cfg.get("skip"):
                continue

            comparison = compare_row_counts(
                _db_path, pair["table_left"], pair["table_right"],
                from_date=from_date or None, to_date=to_date or None,
            )

            # Use live UI state if provided, otherwise fall back to saved
            live = live_pairs.get(pname, {})
            if live.get("excluded_dates") is not None:
                excluded = set(live["excluded_dates"])
            else:
                saved = get_row_comparison(_db_path, pname)
                excluded = set(saved["excluded_dates"]) if saved and saved.get("excluded_dates") else set()

            if excluded:
                comparison["matching"] = [(dt, c) for dt, c in comparison["matching"] if dt not in excluded]
                comparison["mismatched"] = [(dt, l, r) for dt, l, r in comparison["mismatched"] if dt not in excluded]
                comparison["only_left"] = [(dt, c) for dt, c in comparison["only_left"] if dt not in excluded]
                comparison["only_right"] = [(dt, c) for dt, c in comparison["only_right"] if dt not in excluded]

            meta_left = get_metadata(_db_path, pair["table_left"])
            meta_right = get_metadata(_db_path, pair["table_right"])

            # Annotations: prefer live UI state, fall back to config
            if live:
                comment_left = live.get("comment_left", "")
                comment_right = live.get("comment_right", "")
                time_left = live.get("time_left", "")
                time_right = live.get("time_right", "")
                time_map = {"left": time_left, "right": time_right} if (time_left or time_right) else None
            else:
                comment_map = pair_cfg.get("comment_map", {}).get("row", {})
                comment_left = comment_map.get("left", "")
                comment_right = comment_map.get("right", "")
                time_map = pair_cfg.get("time_map", {}).get("row", {}) or None

            html = generate_row_count_html(
                pair_name=pname,
                source_left=pair.get("source_left", "left"),
                source_right=pair.get("source_right", "right"),
                table_left=pair["table_left"],
                table_right=pair["table_right"],
                comparison=comparison,
                metadata_left=meta_left,
                metadata_right=meta_right,
                comment_left=comment_left,
                comment_right=comment_right,
                time_map=time_map,
                left_cfg=pair_cfg.get("left"),
                right_cfg=pair_cfg.get("right"),
                description=pair_cfg.get("description", ""),
            )
            sections.append(html)

        table = create_row_count_table(sections)
        doc = wrap_html_document(title, [table], subtitle=subtitle)

        return HTMLResponse(content=doc)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/api/compare/row/export/csv/{pair_name}")
async def api_compare_row_export_csv(pair_name: str, from_date: str = "", to_date: str = ""):
    """Generate CSV export for one pair's row comparison."""
    import csv as csv_mod

    try:
        from ..db import get_table_pair, get_row_comparison
        from ..compare import compare_row_counts

        pair = get_table_pair(_db_path, pair_name)
        if not pair:
            return JSONResponse(status_code=404, content={"error": f"Pair '{pair_name}' not found"})

        comparison = compare_row_counts(
            _db_path, pair["table_left"], pair["table_right"],
            from_date=from_date or None, to_date=to_date or None,
        )

        saved = get_row_comparison(_db_path, pair_name)
        excluded = set(saved["excluded_dates"]) if saved and saved.get("excluded_dates") else set()

        summary = comparison["summary"]
        left_min, left_max = summary["date_range_left"]
        right_min, right_max = summary["date_range_right"]
        overlap_start = overlap_end = None
        if left_min and right_min and left_max and right_max:
            overlap_start = max(left_min, right_min)
            overlap_end = min(left_max, right_max)
            if overlap_start > overlap_end:
                overlap_start = overlap_end = None

        buf = io.StringIO()
        writer = csv_mod.writer(buf)
        writer.writerow(["category", "in_overlap", "excluded", "date", "left_count", "right_count", "diff"])

        rows = []
        for dt, count in comparison["matching"]:
            in_ov = overlap_start and overlap_end and overlap_start <= dt <= overlap_end
            rows.append(("match", in_ov, dt in excluded, dt, count, count, 0))
        for dt, l, r in comparison["mismatched"]:
            in_ov = overlap_start and overlap_end and overlap_start <= dt <= overlap_end
            rows.append(("mismatch", in_ov, dt in excluded, dt, l, r, r - l))
        for dt, count in comparison["only_left"]:
            in_ov = overlap_start and overlap_end and overlap_start <= dt <= overlap_end
            rows.append(("left_only", in_ov, dt in excluded, dt, count, "", ""))
        for dt, count in comparison["only_right"]:
            in_ov = overlap_start and overlap_end and overlap_start <= dt <= overlap_end
            rows.append(("right_only", in_ov, dt in excluded, dt, "", count, ""))

        rows.sort(key=lambda r: r[3])
        for row in rows:
            writer.writerow(row)

        buf.seek(0)
        return StreamingResponse(
            iter([buf.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={pair_name}_row_compare.csv"},
        )
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/api/compare/row/export/count-csv/{pair_name}")
async def api_compare_row_export_count_csv(
    pair_name: str, side: str = "left", from_date: str = "", to_date: str = ""
):
    """Generate a single-side count CSV: DATE_COL (SOURCE), COUNT (SOURCE)."""
    import csv as csv_mod

    try:
        from ..db import get_table_pair, get_metadata
        from ..compare import compare_row_counts

        pair = get_table_pair(_db_path, pair_name)
        if not pair:
            return JSONResponse(status_code=404, content={"error": f"Pair '{pair_name}' not found"})

        if side not in ("left", "right"):
            return JSONResponse(status_code=400, content={"error": "side must be 'left' or 'right'"})

        comparison = compare_row_counts(
            _db_path, pair["table_left"], pair["table_right"],
            from_date=from_date or None, to_date=to_date or None,
        )

        # Determine column header names
        tbl_key = "table_left" if side == "left" else "table_right"
        src_key = "source_left" if side == "left" else "source_right"
        meta = get_metadata(_db_path, pair[tbl_key]) or {}
        date_var = (meta.get("date_var") or "DT").upper()
        source = (pair.get(src_key) or side).upper()

        # Collect (date, count) rows for the requested side
        rows = []
        for dt, count in comparison["matching"]:
            rows.append((dt, count))
        for dt, l, r in comparison["mismatched"]:
            rows.append((dt, l if side == "left" else r))
        if side == "left":
            for dt, count in comparison["only_left"]:
                rows.append((dt, count))
        else:
            for dt, count in comparison["only_right"]:
                rows.append((dt, count))

        rows.sort(key=lambda r: r[0])

        buf = io.StringIO()
        writer = csv_mod.writer(buf)
        writer.writerow([f"{date_var} ({source})", f"COUNT ({source})"])
        for dt, count in rows:
            writer.writerow([dt, count])

        buf.seek(0)
        return StreamingResponse(
            iter([buf.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={pair_name}_{side}_counts.csv"},
        )
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/api/compare/row/export/excel/{pair_name}")
async def api_compare_row_export_excel(
    pair_name: str, from_date: str = "", to_date: str = ""
):
    """Generate Excel with two sheets (left counts, right counts) for one pair."""
    try:
        from openpyxl import Workbook
        from ..db import get_table_pair, get_metadata
        from ..compare import compare_row_counts

        pair = get_table_pair(_db_path, pair_name)
        if not pair:
            return JSONResponse(status_code=404, content={"error": f"Pair '{pair_name}' not found"})

        comparison = compare_row_counts(
            _db_path, pair["table_left"], pair["table_right"],
            from_date=from_date or None, to_date=to_date or None,
        )

        def _build_side(side):
            tbl_key = "table_left" if side == "left" else "table_right"
            src_key = "source_left" if side == "left" else "source_right"
            meta = get_metadata(_db_path, pair[tbl_key]) or {}
            date_var = (meta.get("date_var") or "DT").upper()
            source = (pair.get(src_key) or side).upper()

            rows = []
            for dt, count in comparison["matching"]:
                rows.append((dt, count))
            for dt, l, r in comparison["mismatched"]:
                rows.append((dt, l if side == "left" else r))
            if side == "left":
                for dt, count in comparison["only_left"]:
                    rows.append((dt, count))
            else:
                for dt, count in comparison["only_right"]:
                    rows.append((dt, count))
            rows.sort(key=lambda r: r[0])
            return date_var, source, rows

        wb = Workbook()

        for idx, side in enumerate(("left", "right")):
            date_var, source, rows = _build_side(side)
            if idx == 0:
                ws = wb.active
                ws.title = source
            else:
                ws = wb.create_sheet(title=source)
            ws.append([f"{date_var} ({source})", f"COUNT ({source})"])
            for dt, count in rows:
                ws.append([dt, count])

        buf = io.BytesIO()
        wb.save(buf)
        buf.seek(0)
        return StreamingResponse(
            iter([buf.getvalue()]),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={pair_name}_counts.xlsx"},
        )
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/api/compare/row/{pair_name}")
async def api_compare_row_pair(pair_name: str, from_date: str = "", to_date: str = ""):
    """Run row comparison for a single pair and return structured JSON."""
    try:
        from ..db import get_table_pair, get_metadata, get_row_comparison
        from ..compare import compare_row_counts
        from ..config import load_unified_config

        pair = get_table_pair(_db_path, pair_name)
        if not pair:
            return JSONResponse(status_code=404, content={"error": f"Pair '{pair_name}' not found"})

        comparison = compare_row_counts(
            _db_path, pair["table_left"], pair["table_right"],
            from_date=from_date or None, to_date=to_date or None,
        )

        meta_left = get_metadata(_db_path, pair["table_left"]) or {}
        meta_right = get_metadata(_db_path, pair["table_right"]) or {}

        # Load saved comparison state
        saved = get_row_comparison(_db_path, pair_name)

        # Load annotations from config
        config = load_unified_config(_config_path)
        pair_cfg = config.get("pairs", {}).get(pair_name, {})
        comment_map = pair_cfg.get("comment_map", {}).get("row", {})
        time_map = pair_cfg.get("time_map", {}).get("row", {})

        # Build date rows
        summary = comparison["summary"]
        left_min, left_max = summary["date_range_left"]
        right_min, right_max = summary["date_range_right"]

        # Calculate overlap
        overlap_start = overlap_end = None
        if left_min and right_min and left_max and right_max:
            overlap_start = max(left_min, right_min)
            overlap_end = min(left_max, right_max)
            if overlap_start > overlap_end:
                overlap_start = overlap_end = None

        dates = []
        for dt, count in comparison["matching"]:
            in_overlap = overlap_start and overlap_end and overlap_start <= dt <= overlap_end
            dates.append({"dt": dt, "left": count, "right": count, "diff": 0, "status": "match", "in_overlap": bool(in_overlap)})
        for dt, l, r in comparison["mismatched"]:
            in_overlap = overlap_start and overlap_end and overlap_start <= dt <= overlap_end
            dates.append({"dt": dt, "left": l, "right": r, "diff": r - l, "status": "mismatch", "in_overlap": bool(in_overlap)})
        for dt, count in comparison["only_left"]:
            in_overlap = overlap_start and overlap_end and overlap_start <= dt <= overlap_end
            dates.append({"dt": dt, "left": count, "right": None, "diff": None, "status": "left_only", "in_overlap": bool(in_overlap)})
        for dt, count in comparison["only_right"]:
            in_overlap = overlap_start and overlap_end and overlap_start <= dt <= overlap_end
            dates.append({"dt": dt, "left": None, "right": count, "diff": None, "status": "right_only", "in_overlap": bool(in_overlap)})

        dates.sort(key=lambda d: d["dt"])

        return {
            "pair_name": pair_name,
            "table_left": pair["table_left"],
            "table_right": pair["table_right"],
            "source_left": pair.get("source_left", ""),
            "source_right": pair.get("source_right", ""),
            "summary": {
                "count_left": summary["count_left"],
                "count_right": summary["count_right"],
                "total_left": summary["total_left"],
                "total_right": summary["total_right"],
                "date_range_left": list(summary["date_range_left"]),
                "date_range_right": list(summary["date_range_right"]),
                "overlap_start": overlap_start,
                "overlap_end": overlap_end,
                "n_match": len(comparison["matching"]),
                "n_mismatch": len(comparison["mismatched"]),
                "n_left_only": len(comparison["only_left"]),
                "n_right_only": len(comparison["only_right"]),
            },
            "dates": dates,
            "saved": {
                "excluded_dates": saved["excluded_dates"] if saved else [],
                "matching_dates": saved["matching_dates"] if saved else [],
            },
            "annotations": {
                "comment_left": comment_map.get("left", ""),
                "comment_right": comment_map.get("right", ""),
                "time_left": time_map.get("left", ""),
                "time_right": time_map.get("right", ""),
            },
            "meta_left": {
                "date_var": meta_left.get("date_var"),
                "vintage": meta_left.get("vintage"),
            },
            "meta_right": {
                "date_var": meta_right.get("date_var"),
                "vintage": meta_right.get("vintage"),
            },
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.put("/api/compare/row/{pair_name}")
async def api_save_row_comparison(pair_name: str, request: Request):
    """Save row comparison state: excluded dates, comments, times."""
    body = await request.json()
    try:
        from ..db import save_row_comparison, get_table_pair
        from ..config import load_unified_config, save_unified_config

        pair = get_table_pair(_db_path, pair_name)
        if not pair:
            return JSONResponse(status_code=404, content={"error": f"Pair '{pair_name}' not found"})

        excluded_dates = body.get("excluded_dates", [])
        matching_dates = body.get("matching_dates", [])
        comment_left = body.get("comment_left", "")
        comment_right = body.get("comment_right", "")
        time_left = body.get("time_left", "")
        time_right = body.get("time_right", "")

        # Compute overlap from matching dates
        overlap_start = min(matching_dates) if matching_dates else None
        overlap_end = max(matching_dates) if matching_dates else None

        save_row_comparison(
            _db_path, pair_name,
            overlap_start=overlap_start,
            overlap_end=overlap_end,
            matching_dates=matching_dates,
            excluded_dates=excluded_dates,
        )

        # Save annotations to config
        config = load_unified_config(_config_path)
        pair_cfg = config.get("pairs", {}).get(pair_name, {})
        if pair_cfg:
            pair_cfg.setdefault("comment_map", {})["row"] = {
                "left": comment_left, "right": comment_right,
            }
            pair_cfg.setdefault("time_map", {})["row"] = {
                "left": time_left, "right": time_right,
            }
            save_unified_config(config, _config_path)

        return {"ok": True, "matching": len(matching_dates), "excluded": len(excluded_dates)}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.post("/api/compare/row/{pair_name}/preview")
async def api_compare_row_preview(pair_name: str, request: Request):
    """Generate HTML preview for one pair's row comparison."""
    body = await request.json()
    try:
        from ..db import get_table_pair, get_metadata
        from ..compare import compare_row_counts
        from ..html_export import generate_row_count_html
        from ..config import load_unified_config

        pair = get_table_pair(_db_path, pair_name)
        if not pair:
            return JSONResponse(status_code=404, content={"error": f"Pair '{pair_name}' not found"})

        from_date = body.get("from_date")
        to_date = body.get("to_date")
        excluded_dates = set(body.get("excluded_dates", []))

        comparison = compare_row_counts(
            _db_path, pair["table_left"], pair["table_right"],
            from_date=from_date, to_date=to_date,
        )

        # Filter out excluded dates from all categories
        if excluded_dates:
            comparison["matching"] = [(dt, c) for dt, c in comparison["matching"] if dt not in excluded_dates]
            comparison["mismatched"] = [(dt, l, r) for dt, l, r in comparison["mismatched"] if dt not in excluded_dates]
            comparison["only_left"] = [(dt, c) for dt, c in comparison["only_left"] if dt not in excluded_dates]
            comparison["only_right"] = [(dt, c) for dt, c in comparison["only_right"] if dt not in excluded_dates]

        meta_left = get_metadata(_db_path, pair["table_left"])
        meta_right = get_metadata(_db_path, pair["table_right"])

        config = load_unified_config(_config_path)
        pair_cfg = config.get("pairs", {}).get(pair_name, {})

        time_map = {}
        tm = body.get("time_left") or body.get("time_right")
        if body.get("time_left") or body.get("time_right"):
            time_map = {"left": body.get("time_left", ""), "right": body.get("time_right", "")}

        html = generate_row_count_html(
            pair_name=pair_name,
            source_left=pair.get("source_left", "left"),
            source_right=pair.get("source_right", "right"),
            table_left=pair["table_left"],
            table_right=pair["table_right"],
            comparison=comparison,
            metadata_left=meta_left,
            metadata_right=meta_right,
            comment_left=body.get("comment_left", ""),
            comment_right=body.get("comment_right", ""),
            time_map=time_map or None,
            left_cfg=pair_cfg.get("left"),
            right_cfg=pair_cfg.get("right"),
            description=pair_cfg.get("description", ""),
        )

        # Wrap in a minimal table
        from ..html_export import create_row_count_table
        table_html = create_row_count_table([html])

        return {"html": table_html}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/api/status/row")
async def api_status_row():
    """Get row count loading status."""
    try:
        from ..db import list_table_pairs, get_metadata

        pairs = list_table_pairs(_db_path)
        status = []

        for pair in pairs:
            for side in ['left', 'right']:
                table = pair[f"table_{side}"]
                meta = get_metadata(_db_path, table) or {}

                status.append({
                    "pair": pair["pair_name"],
                    "side": side,
                    "loaded": meta.get("row_count_total", 0),
                    "min_date": meta.get("min_date_loaded"),
                    "max_date": meta.get("max_date_loaded"),
                })

        return {"status": status, "lastLoad": datetime.now().isoformat()}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.post("/api/clear/row")
async def api_clear_row():
    """Clear all row count data."""
    try:
        conn = sqlite3.connect(_db_path)
        conn.execute("DELETE FROM _row_counts")
        conn.execute("UPDATE _metadata SET row_count_total = 0, min_date_loaded = NULL, max_date_loaded = NULL")
        conn.commit()
        conn.close()

        return {"ok": True}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


# ---------------------------------------------------------------------------
# Pair management (legacy - keep for compatibility)
# ---------------------------------------------------------------------------

@app.post("/api/pairs")
async def api_add_pair(request: Request):
    """Add a new pair to the config and register in DB.

    Body: {
        "pair_name": "cust_daily",
        "left": {"source": "oracle", "table": "CUST_DAILY", "conn_macro": "pb23", "date_col": "RPT_DT"},
        "right": {"source": "aws", "table": "cust_daily", "conn_macro": "mydb", "date_col": "rpt_dt"},
        "col_map": {}
    }
    """
    body = await request.json()
    pair_name = body.get("pair_name", "").strip()
    if not pair_name:
        return JSONResponse(status_code=400, content={"error": "pair_name is required"})

    left = body.get("left", {})
    right = body.get("right", {})
    if not left.get("table") or not right.get("table"):
        return JSONResponse(status_code=400, content={
            "error": "Both left.table and right.table are required"
        })

    try:
        from ..config import load_unified_config, save_unified_config
        from ..platforms.base import qualified_name
        from ..db import register_table_pair, update_metadata

        config = load_unified_config(_config_path)

        _prepare_side(left)
        _prepare_side(right)

        # Build pair config — only persist valid pair-config keys
        pair_cfg = {
            "left": left,
            "right": right,
            "col_map": body.get("col_map", {}),
        }
        for key in ("description", "mode", "fromDate", "toDate",
                     "dateRangeMode", "overlap"):
            if key in body:
                pair_cfg[key] = body[key]
        config.setdefault("pairs", {})[pair_name] = pair_cfg
        save_unified_config(config, _config_path)

        # Inject name for DB registration (auto-derived, not persisted)
        left["name"] = pair_name
        right["name"] = pair_name
        table_left = qualified_name(left)
        table_right = qualified_name(right)
        register_table_pair(
            _db_path, pair_name, table_left, table_right,
            source_left=left.get("source"),
            source_right=right.get("source"),
            col_mappings=body.get("col_map") or None,
        )

        # Create metadata stubs so they show up in status
        for tbl_cfg in [left, right]:
            qname = qualified_name(tbl_cfg)
            update_metadata(_db_path, {
                "table_name": qname,
                "source": tbl_cfg.get("source"),
                "source_table": tbl_cfg.get("table"),
                "date_var": tbl_cfg.get("date_col"),
                "data_type": "row",
            })

        return {"ok": True, "pair_name": pair_name,
                "table_left": table_left, "table_right": table_right}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.delete("/api/pairs/{pair_name}")
async def api_delete_pair(pair_name: str, purge: int = Query(0)):
    """Remove a pair from config. With purge=1, also remove all DB data."""
    try:
        from ..config import load_unified_config
        config = load_unified_config(_config_path)

        found_in_config = pair_name in config.get("pairs", {})

        if found_in_config:
            del config["pairs"][pair_name]
            with open(_config_path, 'w') as f:
                json.dump(config, f, indent=2)

        found_in_db = False
        if purge:
            from ..db import delete_pair
            try:
                delete_pair(_db_path, pair_name)
                found_in_db = True
            except ValueError:
                pass  # pair may not be registered in DB

        if not found_in_config and not found_in_db:
            return JSONResponse(status_code=404, content={"error": f"Pair '{pair_name}' not found"})

        return {"ok": True, "deleted": pair_name}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


def _prepare_side(tbl_cfg):
    """Prepare a left/right config for saving.

    - Strips 'name' (auto-derived from pair_name on load)
    - Converts processed string to list for readable JSON
    """
    tbl_cfg.pop("name", None)
    # Store processed as list for readable JSON
    p = tbl_cfg.get("processed", "")
    if isinstance(p, str) and p.strip():
        tbl_cfg["processed"] = [line for line in p.split("\n") if line.strip()]
    elif not p:
        tbl_cfg.pop("processed", None)


def _sync_config_pairs(config):
    """Register all pairs from config into the database. Returns count."""
    from ..platforms.base import qualified_name
    from ..db import register_table_pair, update_metadata

    count = 0
    for pair_name, pair_cfg in config.get("pairs", {}).items():
        if pair_cfg.get("skip"):
            continue
        left = pair_cfg.get("left", {})
        right = pair_cfg.get("right", {})
        if not left.get("source") or not right.get("source"):
            continue

        # Auto-derive name from pair_name (may not be set if config was loaded raw)
        left.setdefault("name", pair_name)
        right.setdefault("name", pair_name)

        table_left = qualified_name(left)
        table_right = qualified_name(right)
        register_table_pair(
            _db_path, pair_name, table_left, table_right,
            source_left=left.get("source"),
            source_right=right.get("source"),
            col_mappings=pair_cfg.get("col_map") or None,
        )

        for tbl_cfg in [left, right]:
            qname = qualified_name(tbl_cfg)
            update_metadata(_db_path, {
                "table_name": qname,
                "source": tbl_cfg.get("source"),
                "source_table": tbl_cfg.get("table"),
                "date_var": tbl_cfg.get("date_col"),
            })
        count += 1
    return count


# ---------------------------------------------------------------------------
# HITL annotation routes (where_map, time_map, comment_map)
# ---------------------------------------------------------------------------

@app.get("/api/pairs/{pair_name}/annotations")
async def api_get_annotations(pair_name: str):
    """Get where_map, time_map, comment_map for a pair."""
    try:
        from ..config import load_unified_config
        config = load_unified_config(_config_path)

        pair_cfg = config.get("pairs", {}).get(pair_name)
        if not pair_cfg:
            return JSONResponse(status_code=404, content={"error": f"Pair '{pair_name}' not found"})

        return {
            "pair_name": pair_name,
            "where_map": pair_cfg.get("where_map", {"left": "", "right": ""}),
            "time_map": pair_cfg.get("time_map", {
                "row": {"left": "", "right": ""},
                "col": {"left": "", "right": ""},
            }),
            "comment_map": pair_cfg.get("comment_map", {
                "row": {"left": "", "right": ""},
                "col": {"left": "", "right": ""},
            }),
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.put("/api/pairs/{pair_name}/annotations")
async def api_update_annotations(pair_name: str, request: Request):
    """Update where_map, time_map, comment_map for a pair.

    Body (all fields optional):
    {
        "where_map": {"left": "...", "right": "..."},
        "time_map": {"row": {"left": "42s", "right": "112s"}, "col": {...}},
        "comment_map": {"row": {"left": "note", "right": ""}, "col": {...}}
    }
    """
    body = await request.json()
    try:
        from ..config import load_unified_config, save_unified_config
        config = load_unified_config(_config_path)

        if pair_name not in config.get("pairs", {}):
            return JSONResponse(status_code=404, content={"error": f"Pair '{pair_name}' not found"})

        pair_cfg = config["pairs"][pair_name]

        if "where_map" in body:
            pair_cfg["where_map"] = body["where_map"]
        if "time_map" in body:
            pair_cfg.setdefault("time_map", {}).update(body["time_map"])
        if "comment_map" in body:
            pair_cfg.setdefault("comment_map", {}).update(body["comment_map"])

        save_unified_config(config, _config_path)

        # Sync where_map to database
        if "where_map" in body:
            from ..db import sync_config_to_db
            sync_config_to_db(_db_path, config)

        return {"ok": True}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


# ---------------------------------------------------------------------------
# Query preview
# ---------------------------------------------------------------------------

@app.post("/api/preview")
async def api_preview(request: Request):
    """Generate preview SQL for a pair's left and right sides.

    Body: {pair_name, left: {...}, right: {...}, fromDate, toDate}
    Returns: {left: {sql, output_file}, right: {sql, output_file}}
    """
    body = await request.json()
    pair_name = body.get("pair_name", "pair")
    from_date = body.get("fromDate", "")
    to_date = body.get("toDate", "")
    njob_left = body.get("njob_left", 0)
    njob_right = body.get("njob_right", 0)

    result = {}
    for side in ("left", "right"):
        cfg = body.get(side, {})
        if not cfg.get("table"):
            continue
        parallel = njob_left if side == "left" else njob_right
        result[side] = _build_preview_sql(
            pair_name, side, cfg, from_date, to_date, parallel=parallel
        )

    return result


def _build_preview_sql(pair_name, side, cfg, from_date, to_date, parallel=0):
    """Build preview SQL for one side of a pair."""
    source = (cfg.get("source") or "").lower()
    date_col = cfg.get("date_col", "")
    date_type = (cfg.get("date_type") or "").lower()
    vintage_raw = cfg.get("vintage", "")
    where_clause = cfg.get("where", "")
    processed = cfg.get("processed", "")
    table = cfg.get("table", "")
    conn_macro = cfg.get("conn_macro", "")

    is_oracle = source == "oracle"
    is_sas = source == "sas"
    is_aws = source == "aws"

    # --- SELECT/GROUP BY expression (with TRUNC/datepart for bucketing) ---
    select_expr = _preview_select_expr(date_col, date_type, source)
    if vintage_raw:
        select_expr = vintage_raw

    # --- WHERE uses raw column (no TRUNC), adjusts literals instead ---
    where_col = date_col

    # --- Resolve table and CTE ---
    cte_prefix = ""
    query_table = table

    if is_sas:
        query_table = f"{conn_macro}.{table}" if conn_macro else table
    elif is_aws:
        if processed:
            alias = f"{pair_name}_{side}"
            cte_prefix = f"WITH {alias} AS (\n  {processed}\n)\n"
            query_table = alias
        else:
            query_table = f"{conn_macro}.{table}" if conn_macro else table
    else:  # oracle
        if processed:
            alias = cfg.get("name", f"{pair_name}_{side}")
            cte_prefix = f"WITH {alias} AS ({processed}) "
            query_table = alias

    # --- WHERE conditions ---
    conditions = []
    _lit = lambda d: _preview_date_literal(d, date_type, is_sas, source)
    if from_date and to_date:
        conditions.append(f"{where_col} BETWEEN {_lit(from_date)} AND {_lit(to_date)}")
    elif from_date:
        conditions.append(f"{where_col} >= {_lit(from_date)}")
    elif to_date:
        conditions.append(f"{where_col} <= {_lit(to_date)}")
    if where_clause:
        conditions.append(f"({where_clause})")

    where_str = ""
    if conditions:
        where_str = "WHERE " + "\n      AND ".join(conditions)

    # --- Parallel hint ---
    parallel_int = int(parallel) if parallel else 0
    ora_hint = f" /*+ PARALLEL({parallel_int}) */" if parallel_int > 1 and (is_oracle or is_sas) else ""
    aws_hint = f"  -- max_workers={parallel_int}" if parallel_int > 1 and is_aws else ""

    # --- Build SQL by platform ---
    output_file = f"{pair_name}_{side}_row"

    if is_aws:
        sql = f"""{cte_prefix}SELECT
  {select_expr} AS date_value,
  COUNT(*) AS row_count
FROM {query_table}
{where_str}
GROUP BY {select_expr}
ORDER BY date_value;{aws_hint}""".strip()
        return {"sql": sql, "output_file": f"{output_file}.sql"}

    elif is_sas:
        # SAS setup + proc sql on local dataset
        setup = ""
        if processed:
            setup = f"{processed}\n\n"
        indent_where = where_str.replace("\n", "\n    ") if where_str else ""
        sql = f"""{setup}proc sql;
  create table work.{output_file} as
  select
    {select_expr} as date_value,
    count(*) as row_count
  from {query_table}
  {indent_where}
  group by {select_expr};
quit;""".strip()
        return {"sql": sql, "output_file": f"{output_file}.sas"}

    else:
        # Oracle via SAS proc sql passthrough
        indent_where = ("    " + where_str.replace("\n", "\n    ")) if where_str else ""
        inner_sql = f"""{cte_prefix}SELECT{ora_hint}
      {select_expr} AS date_value,
      COUNT(*) AS row_count
    FROM {query_table}
    {indent_where}
    GROUP BY {select_expr}"""
        sql = f"""proc sql;
  %{conn_macro}
  create table work.{output_file} as
  select * from connection to oracle (
    {inner_sql.strip()}
  );
  disconnect from oracle;
quit;""".strip()
        return {"sql": sql, "output_file": f"{output_file}.sas"}


def _preview_select_expr(date_col, date_type, source):
    """Build the date expression for SELECT/GROUP BY (bucketing to day level)."""
    dtype = date_type.lower() if date_type else ""

    if source == "oracle":
        if dtype == "timestamp":
            return f"TRUNC({date_col})"
        return date_col
    elif source == "sas":
        if dtype in ("datetime", "timestamp"):
            return f"datepart({date_col})"
        return date_col
    elif source == "aws":
        if dtype in ("timestamp", "datetime"):
            return f"CAST({date_col} AS DATE)"
        return date_col
    return date_col


def _preview_date_literal(date_str, date_type, is_sas=False, source=""):
    """Format a YYYY-MM-DD date string as the correct SQL literal for the date type.

    For timestamp columns, uses TIMESTAMP literals so the raw column can be
    compared without wrapping in TRUNC (preserves index usage).
    """
    if not date_str:
        return "''"
    dtype = date_type.lower() if date_type else ""

    if dtype in ("num", "integer", "int", "number"):
        return date_str.replace("-", "")

    if dtype == "string_compact":
        return f"'{date_str.replace('-', '')}'"

    if dtype in ("string_dash", "string"):
        return f"'{date_str}'"

    if is_sas and dtype in ("datetime", "date", "timestamp"):
        # SAS date literal: '01JAN2024'd
        try:
            from datetime import datetime as _dt
            d = _dt.strptime(date_str, "%Y-%m-%d")
            if dtype in ("datetime", "timestamp"):
                return f"'{d.strftime('%d%b%Y').upper()}:00:00:00'dt"
            return f"'{d.strftime('%d%b%Y').upper()}'d"
        except ValueError:
            return f"'{date_str}'"

    # Oracle/Athena timestamp: use TIMESTAMP literal
    if dtype == "timestamp" and source == "oracle":
        return f"TIMESTAMP '{date_str} 00:00:00'"
    if dtype in ("timestamp", "datetime") and source == "aws":
        return f"TIMESTAMP '{date_str} 00:00:00'"

    # Oracle/Athena date: DATE literal
    if dtype in ("date",):
        return f"DATE '{date_str}'"

    return f"'{date_str}'"


# ---------------------------------------------------------------------------
# Column Mapping API
# ---------------------------------------------------------------------------

@app.get("/api/pairs/{pair_name}/columns")
async def api_get_pair_columns(pair_name: str):
    """Return left/right columns, current col_mappings, col_rules, and auto-match results."""
    try:
        from ..db import (
            get_table_pair, get_column_meta, get_pair_col_map_from_db,
            get_pair_col_rules, ensure_col_rules_column,
        )
        from ..compare import match_columns_from_dicts

        pair = get_table_pair(_db_path, pair_name)
        if not pair:
            return JSONResponse(status_code=404, content={"error": f"Pair '{pair_name}' not found"})

        ensure_col_rules_column(_db_path)

        # Get column metadata for both sides
        left_meta = get_column_meta(_db_path, pair["table_left"])
        right_meta = get_column_meta(_db_path, pair["table_right"])

        left_cols = {c["column_name"]: c.get("data_type", "") for c in left_meta}
        right_cols = {c["column_name"]: c.get("data_type", "") for c in right_meta}

        # Get existing mappings and rules
        col_mappings = get_pair_col_map_from_db(_db_path, pair_name)
        col_rules_data = get_pair_col_rules(_db_path, pair_name)

        # Determine unmapped columns
        mapped_left = set(col_mappings.keys())
        mapped_right = set(col_mappings.values())
        unmapped_left = {k: v for k, v in left_cols.items() if k not in mapped_left}
        unmapped_right = {k: v for k, v in right_cols.items() if k not in mapped_right}

        # Auto-match unmapped columns
        auto_result = match_columns_from_dicts(
            unmapped_left, unmapped_right,
            left_label=pair.get("source_left", "left"),
            right_label=pair.get("source_right", "right"),
        )

        return {
            "pair_name": pair_name,
            "table_left": pair["table_left"],
            "table_right": pair["table_right"],
            "source_left": pair.get("source_left", ""),
            "source_right": pair.get("source_right", ""),
            "left_columns": left_cols,
            "right_columns": right_cols,
            "col_mappings": col_mappings,
            "col_rules": col_rules_data,
            "auto_match": auto_result.get("matched", {}),
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/api/pairs/{pair_name}/columns/csv")
async def api_get_pair_columns_csv(pair_name: str, side: str = "left"):
    """Generate a column metadata CSV for one side of a pair.

    Columns: TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_TYPE_EXTENDED, MANDATORY
    """
    import csv as csv_mod

    try:
        from ..db import get_table_pair, get_column_meta

        pair = get_table_pair(_db_path, pair_name)
        if not pair:
            return JSONResponse(status_code=404, content={"error": f"Pair '{pair_name}' not found"})

        if side not in ("left", "right"):
            return JSONResponse(status_code=400, content={"error": "side must be 'left' or 'right'"})

        tbl_key = "table_left" if side == "left" else "table_right"
        table_name = pair[tbl_key].upper()
        col_meta = get_column_meta(_db_path, pair[tbl_key])

        buf = io.StringIO()
        writer = csv_mod.writer(buf)
        writer.writerow(["TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "DATA_TYPE_EXTENDED", "MANDATORY"])

        for col in sorted(col_meta, key=lambda c: c["column_name"]):
            dt = (col.get("data_type") or "").upper()
            writer.writerow([
                table_name,
                col["column_name"].upper(),
                dt,
                "",
                "",
            ])

        buf.seek(0)
        src_key = "source_left" if side == "left" else "source_right"
        source = pair.get(src_key, side)
        return StreamingResponse(
            iter([buf.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={pair_name}_{source}_columns.csv"},
        )
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/api/pairs/{pair_name}/columns/excel")
async def api_get_pair_columns_excel(pair_name: str):
    """Generate Excel with two sheets (left columns, right columns) for one pair."""
    try:
        from openpyxl import Workbook
        from ..db import get_table_pair, get_column_meta

        pair = get_table_pair(_db_path, pair_name)
        if not pair:
            return JSONResponse(status_code=404, content={"error": f"Pair '{pair_name}' not found"})

        wb = Workbook()
        for idx, side in enumerate(("left", "right")):
            tbl_key = "table_left" if side == "left" else "table_right"
            src_key = "source_left" if side == "left" else "source_right"
            table_name = pair[tbl_key].upper()
            source = (pair.get(src_key) or side).upper()
            col_meta = get_column_meta(_db_path, pair[tbl_key])

            if idx == 0:
                ws = wb.active
                ws.title = source
            else:
                ws = wb.create_sheet(title=source)

            ws.append(["TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "DATA_TYPE_EXTENDED", "MANDATORY"])
            for col in sorted(col_meta, key=lambda c: c["column_name"]):
                dt = (col.get("data_type") or "").upper()
                ws.append([table_name, col["column_name"].upper(), dt, "", ""])

        buf = io.BytesIO()
        wb.save(buf)
        buf.seek(0)
        return StreamingResponse(
            iter([buf.getvalue()]),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={pair_name}_columns.xlsx"},
        )
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.put("/api/pairs/{pair_name}/col-mappings")
async def api_save_col_mappings(pair_name: str, request: Request):
    """Save flat mappings + rules + sources."""
    body = await request.json()
    try:
        from ..db import (
            get_table_pair, update_pair_col_map,
            update_pair_col_rules, ensure_col_rules_column,
        )

        pair = get_table_pair(_db_path, pair_name)
        if not pair:
            return JSONResponse(status_code=404, content={"error": f"Pair '{pair_name}' not found"})

        ensure_col_rules_column(_db_path)

        mappings = body.get("mappings", {})
        rules_data = body.get("rules", {})

        update_pair_col_map(_db_path, pair_name, mappings)
        update_pair_col_rules(_db_path, pair_name, rules_data)

        return {"ok": True, "mapped": len(mappings)}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/api/pairs/{pair_name}/col-mappings/csv")
async def api_col_mappings_csv(pair_name: str):
    """Download CSV with columns: status, left_column, left_type, right_column, right_type."""
    import csv as csv_mod

    try:
        from ..db import get_table_pair, get_column_meta, get_pair_col_map_from_db

        pair = get_table_pair(_db_path, pair_name)
        if not pair:
            return JSONResponse(status_code=404, content={"error": f"Pair '{pair_name}' not found"})

        left_meta = get_column_meta(_db_path, pair["table_left"])
        right_meta = get_column_meta(_db_path, pair["table_right"])

        left_types = {c["column_name"]: c.get("data_type", "") for c in left_meta}
        right_types = {c["column_name"]: c.get("data_type", "") for c in right_meta}

        col_mappings = get_pair_col_map_from_db(_db_path, pair_name)
        source_left = pair.get("source_left", "left")
        source_right = pair.get("source_right", "right")

        buf = io.StringIO()
        writer = csv_mod.writer(buf)
        writer.writerow(["status", "left_column", "left_type", "right_column", "right_type"])

        # Mapped columns
        for left_col, right_col in sorted(col_mappings.items()):
            writer.writerow([
                "matched",
                left_col,
                left_types.get(left_col, ""),
                right_col,
                right_types.get(right_col, ""),
            ])

        # Left-only
        mapped_left = set(col_mappings.keys())
        for col in sorted(left_types.keys()):
            if col not in mapped_left:
                writer.writerow([
                    f"{source_left}-only",
                    col,
                    left_types.get(col, ""),
                    "",
                    "",
                ])

        # Right-only
        mapped_right = set(col_mappings.values())
        for col in sorted(right_types.keys()):
            if col not in mapped_right:
                writer.writerow([
                    f"{source_right}-only",
                    "",
                    "",
                    col,
                    right_types.get(col, ""),
                ])

        buf.seek(0)
        return StreamingResponse(
            iter([buf.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={pair_name}_col_mappings.csv"},
        )
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


# ---------------------------------------------------------------------------
# Testing mode
# ---------------------------------------------------------------------------

@app.get("/api/testing")
async def api_testing_status():
    """Return current testing mode state."""
    return {"testing": _testing_mode, "config_path": _config_path}


@app.post("/api/testing")
async def api_testing_toggle(request: Request):
    """Toggle testing mode on/off.

    When enabled: sets DTRACK_MOCK env var, swaps config to testing/config.json.
    When disabled: restores original config, unsets DTRACK_MOCK.
    """
    global _config_path, _original_config_path, _testing_mode

    body = await request.json()
    enable = body.get("enabled", False)

    testing_dir = Path(__file__).parent.parent.parent / "testing"
    mock_dir = testing_dir / "mock"
    test_config = testing_dir / "config.json"

    if enable:
        if not test_config.exists():
            return JSONResponse(status_code=404, content={
                "error": f"Testing config not found: {test_config}"
            })
        if not mock_dir.exists():
            return JSONResponse(status_code=404, content={
                "error": f"Mock data not found: {mock_dir}"
            })
        _original_config_path = _config_path
        _config_path = str(test_config.resolve())
        os.environ["DTRACK_MOCK"] = str(mock_dir.resolve())
        _testing_mode = True
        return {"ok": True, "testing": True,
                "config_path": _config_path,
                "mock_dir": str(mock_dir.resolve())}
    else:
        if _original_config_path:
            _config_path = _original_config_path
            _original_config_path = ""
        os.environ.pop("DTRACK_MOCK", None)
        _testing_mode = False
        return {"ok": True, "testing": False, "config_path": _config_path}


# ---------------------------------------------------------------------------
# Server entry point
# ---------------------------------------------------------------------------

def serve(db_path: str, config_path: str, port: int = 8080, host: str = "0.0.0.0"):
    """Start the dtrack web server."""
    global _db_path, _db_dir, _config_path, _original_config_path
    _db_path = os.path.abspath(db_path)
    _db_dir = os.path.dirname(_db_path)
    _config_path = os.path.abspath(config_path)
    _original_config_path = _config_path

    # Sync config pairs into DB on startup
    try:
        from ..config import load_unified_config
        config = load_unified_config(_config_path)
        n = _sync_config_pairs(config)
        print(f"  Synced {n} pairs from config → DB")
    except Exception as e:
        print(f"  Warning: could not sync pairs: {e}")

    import uvicorn
    print(f"dtrack web UI")
    print(f"  Database:  {_db_path}")
    print(f"  Config:    {_config_path}")
    print(f"  Base dir:  {_db_dir}")
    print(f"  URL:       http://localhost:{port}")
    uvicorn.run(app, host=host, port=port, log_level="warning")
