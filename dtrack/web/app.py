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
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
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
_config_path: str = ""


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


# ---------------------------------------------------------------------------
# API routes
# ---------------------------------------------------------------------------

@app.get("/api/status")
async def api_status():
    """Return pairs + metadata + date ranges."""
    from ..db import list_table_pairs, get_metadata
    from ..config import load_unified_config, get_all_tables_from_unified

    pairs = list_table_pairs(_db_path)
    config = load_unified_config(_config_path)

    result = []
    for pair in pairs:
        pname = pair["pair_name"]
        meta_left = get_metadata(_db_path, pair["table_left"])
        meta_right = get_metadata(_db_path, pair["table_right"])

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
            },
            "right": {
                "min_date": (meta_right or {}).get("min_date_loaded"),
                "max_date": (meta_right or {}).get("max_date_loaded"),
                "data_type": (meta_right or {}).get("data_type"),
                "row_count": (meta_right or {}).get("row_count_total"),
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
    outdir = body.get("outdir", "./csv/" if platform == "aws" else "./sas/")

    buf = io.StringIO()
    try:
        with redirect_stdout(buf), redirect_stderr(buf):
            if platform == "aws":
                from ..platforms.athena import extract_aws
                extract_aws(_config_path, outdir, types=[extract_type],
                            db_path=_db_path, from_date=from_date, to_date=to_date)
            else:
                from ..platforms.oracle import gen_sas
                gen_sas(_config_path, outdir, types=[extract_type],
                        db_path=_db_path, from_date=from_date, to_date=to_date)
        return {"ok": True, "output": buf.getvalue()}
    except Exception as e:
        return JSONResponse(status_code=500, content={
            "ok": False, "error": str(e), "output": buf.getvalue()
        })


@app.post("/api/load")
async def api_load(request: Request):
    """Run load-row or load-col."""
    body = await request.json()
    load_type = body.get("type", "row")  # "row" or "col"
    folder = body.get("folder", "./csv/")

    buf = io.StringIO()
    try:
        with redirect_stdout(buf), redirect_stderr(buf):
            from ..config import load_unified_config, get_all_tables_from_unified
            from ..platforms.base import qualified_name

            config = load_unified_config(_config_path)
            tables = get_all_tables_from_unified(config)

            if load_type == "row":
                from ..loader import load_row_counts
                from ..db import get_row_counts
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
    output_dir = os.path.join(os.getcwd(), "output")
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

        # Merge top-level metadata
        if "metadata" in body:
            config.setdefault("metadata", {}).update(body["metadata"])

        save_unified_config(config, _config_path)
        _sync_config_pairs(config)

        return {"ok": True}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


# ---------------------------------------------------------------------------
# Pair management (new UI)
# ---------------------------------------------------------------------------

@app.get("/api/pairs/list")
async def api_list_pairs():
    """List all pairs with their full configuration."""
    try:
        from ..config import load_unified_config
        config = load_unified_config(_config_path)

        pairs_list = []
        for pair_name, pair_cfg in config.get("pairs", {}).items():
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
        config = load_unified_config(_config_path)

        if pair_name not in config.get("pairs", {}):
            return JSONResponse(status_code=404, content={"error": f"Pair '{pair_name}' not found"})

        # Update the pair configuration
        config["pairs"][pair_name].update(body)

        save_unified_config(config, _config_path)
        return {"ok": True, "pair_name": pair_name}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/api/scan/csv")
async def api_scan_csv(dir: str = "./output/", type: str = "row"):
    """Scan directory for CSV files."""
    try:
        import glob
        from pathlib import Path

        dir_path = Path(dir)
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
            parts = filename.split('_')

            # Find matching table in config
            # This is a simplified approach - you might need more robust matching
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
        "left": {"source": "pcds", "table": "CUST_DAILY", "conn_macro": "pcds", "date_col": "RPT_DT"},
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

        # Ensure 'name' is set (required by config schema) — default to lowercase table
        _ensure_name(left)
        _ensure_name(right)

        # Build pair config
        pair_cfg = {
            "left": left,
            "right": right,
            "col_map": body.get("col_map", {}),
        }
        config.setdefault("pairs", {})[pair_name] = pair_cfg
        save_unified_config(config, _config_path)

        # Register in DB
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
async def api_delete_pair(pair_name: str):
    """Remove a pair from config."""
    try:
        from ..config import load_unified_config
        config = load_unified_config(_config_path)

        if pair_name not in config.get("pairs", {}):
            return JSONResponse(status_code=404, content={"error": f"Pair '{pair_name}' not found"})

        del config["pairs"][pair_name]

        # Write directly (skip validation — may have 0 pairs temporarily)
        with open(_config_path, 'w') as f:
            json.dump(config, f, indent=2)

        return {"ok": True, "deleted": pair_name}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


def _ensure_name(tbl_cfg):
    """Ensure tbl_cfg has 'name' key (required by config schema).

    Defaults to lowercase version of 'table' if not set.
    """
    if "name" not in tbl_cfg:
        tbl_cfg["name"] = tbl_cfg.get("table", "").lower()


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
        if not left.get("name") or not right.get("name"):
            continue

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
# Server entry point
# ---------------------------------------------------------------------------

def serve(db_path: str, config_path: str, port: int = 8080, host: str = "0.0.0.0"):
    """Start the dtrack web server."""
    global _db_path, _config_path
    _db_path = os.path.abspath(db_path)
    _config_path = os.path.abspath(config_path)

    import uvicorn
    print(f"dtrack web UI")
    print(f"  Database: {_db_path}")
    print(f"  Config:   {_config_path}")
    print(f"  URL:      http://localhost:{port}")
    uvicorn.run(app, host=host, port=port, log_level="warning")
