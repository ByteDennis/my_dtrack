from __future__ import annotations

import json
import logging
import os
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dtrack_left.backends.sources import source_from_config
from dtrack_left.backends.store import validate_run_id
from dtrack_left.checks.col_check import (
    ColCheckPipeline,
    LeftRuntime,
    publish_fixed_keys,
    wait_for_manifests,
)
from dtrack_left.checks.row_check import make_run_id
from dtrack_left.checks.row_check import run_row_check as left_row_check
from dtrack_left.cli.config import load_config
from dtrack_left.common.audit import SqlAudit, attach_sql_audit, prune_audit_history
from dtrack_left.common.dates import partition_where, quote_ident
from dtrack_left.common.duck import connect, use_temp
from dtrack_left.common.stats import compute_csv_statistics
from dtrack_right.backends.store import S3Store, VolumeStore
from dtrack_right.backends.targets import SparkTarget
from dtrack_right.checks.col_check import ColCheck
from dtrack_right.checks.row_check import run_row_check as right_row_check
from dtrack_right.report import build_report

from .run_logging import configure_run_logging, log_exception_once
from .warehouse import Warehouse

log = logging.getLogger("dtrack.local")
LOCAL = "_local"
NEVER_BLOCK = 10**9


# >>> every pulled window, Left and Right, kept in one local DuckDB file for ad-hoc queries <<< #
class Landing:
    def __init__(self, path: str | Path, *, sql_audit=None):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()
        self.sql_audit = sql_audit

    def _execute(self, connection, sql: str, operation: str):
        if self.sql_audit is not None:
            self.sql_audit.record(sql, backend="duckdb-landing", operation=operation)
        return connection.execute(sql)

    def land(self, side: str, pair: str, bucket: str, csv_path: str | Path) -> int:
        table = quote_ident(f"{side}__{pair}", "sql")
        source = (
            "read_csv('" + str(csv_path).replace("'", "''") + "', all_varchar=true, header=true)"
        )
        label = "'" + bucket.replace("'", "''") + "'"
        with self.lock, connect(str(self.path), ordered=False) as connection:
            self._execute(
                connection,
                f"CREATE TABLE IF NOT EXISTS {table} AS "
                f"SELECT *, {label} AS dtrack_bucket FROM {source} LIMIT 0",
                "landing_create",
            )
            self._execute(
                connection,
                f"DELETE FROM {table} WHERE dtrack_bucket = {label}",
                "landing_replace",
            )
            self._execute(
                connection,
                f"INSERT INTO {table} BY NAME SELECT *, {label} AS dtrack_bucket FROM {source}",
                "landing_insert",
            )
            return self._execute(
                connection,
                f"SELECT count(*) FROM {table} WHERE dtrack_bucket = {label}",
                "landing_count",
            ).fetchone()[0]

    def tables(self) -> dict[str, int]:
        if not self.path.exists():
            return {}
        with self.lock, connect(str(self.path), read_only=True) as connection:
            names = [
                row[0]
                for row in self._execute(connection, "SHOW TABLES", "landing_inspect").fetchall()
            ]
            return {
                name: self._execute(
                    connection,
                    f"SELECT count(*) FROM {quote_ident(name, 'sql')}",
                    "landing_inspect",
                ).fetchone()[0]
                for name in names
            }


# >>> Right counts in the warehouse; only source-sampled rows transfer for local statistics <<< #
class WarehouseTarget(SparkTarget):
    def __init__(
        self,
        warehouse: Warehouse,
        config: dict[str, Any],
        work_dir: str | Path,
        *,
        landing: Landing | None = None,
    ):
        super().__init__(None, config)
        self.warehouse = warehouse
        self.work_dir = Path(work_dir)
        self.landing = landing

    def _rows(self, sql: str, operation: str = "query") -> list[tuple]:
        return self.warehouse.rows(sql, operation=operation)

    def window_sql(self, manifest: dict[str, Any], window: dict[str, Any]) -> tuple[str, list[str]]:
        self.date_type()
        date_col = self.config["date_col"]
        predicate = partition_where(
            date_col, window["partitions"], dialect="spark", date_type=self.config.get("date_type")
        )
        sample = self._sample_predicate(manifest, window)
        where = self._where(predicate if sample is None else f"{predicate} AND ({sample})")
        right_columns = [manifest["col_map"][column] for column in manifest["columns"]]
        selected = list(dict.fromkeys([date_col, *manifest["right_key_cols"], *right_columns]))
        projection = ", ".join(quote_ident(column, "spark") for column in selected)
        return f"SELECT {projection} FROM {self.relation()} WHERE {where}", selected

    def window_statistics(
        self, manifest: dict[str, Any], window: dict[str, Any]
    ) -> dict[str, dict[str, Any]]:
        sql, selected = self.window_sql(manifest, window)
        output = self.work_dir / manifest["pair"] / window["bucket"] / "right.csv"
        self.warehouse.to_csv(sql, output, selected, operation="pull_sample")
        if self.landing:
            self.landing.land("right", manifest["pair"], window["bucket"], output)
        overrides = manifest.get("col_type_overrides") or {}
        column_types = manifest.get("column_types") or {}
        return compute_csv_statistics(
            output,
            [manifest["col_map"][column] for column in manifest["columns"]],
            strict=manifest.get("strict", False),
            col_types={manifest["col_map"][k]: v for k, v in overrides.items()},
            column_types={manifest["col_map"][k]: v for k, v in column_types.items()},
            side="right",
        )


# >>> Left pulls exactly as `dtrack col-check`, plus its query-filtered window in the landing DuckDB <<< #
class LandingLeftRuntime(LeftRuntime):
    def __init__(self, work_dir: str | Path, store, landing: Landing | None = None):
        super().__init__(work_dir, store)
        self.landing = landing

    def pull_window(self, manifest, window):
        sampled = super().pull_window(manifest, window)
        if self.landing:
            raw = self.work_dir / manifest["pair"] / window["bucket"] / "left.csv"
            self.landing.land("left", manifest["pair"], window["bucket"], raw)
        return sampled


# >>> one local run: the config, a directory store laid out exactly like an S3 run folder, and how to reach the Right side <<< #
@dataclass
class LocalRun:
    config: dict[str, Any]
    run_id: str
    folder: Path
    target_factory: Callable[[dict[str, Any]], Any]
    landing: Landing
    pairs: list[str] | None = None
    store: VolumeStore = field(init=False)

    def __post_init__(self):
        self.folder = Path(self.folder)
        self.store = VolumeStore(self.folder)

    @property
    def local(self) -> Path:
        return self.folder / LOCAL

    @property
    def error_log(self) -> Path:
        return self.local / "logs" / "errors.log"

    def status(self) -> dict[str, Any]:
        return self.store.get_json("_STATUS.json") if self.store.exists("_STATUS.json") else {}


# >>> open (or resume) a run under `out`: config file or dict, warehouse from profile + id, landing DuckDB in _local/ <<< #
def open_run(
    config: str | Path | dict[str, Any],
    *,
    run_id: str | None = None,
    out: str | Path = ".dtrack-local/runs",
    profile: str | None = None,
    warehouse_id: str | None = None,
    pairs: list[str] | None = None,
    target_factory: Callable[[dict[str, Any]], Any] | None = None,
) -> LocalRun:
    document = config if isinstance(config, dict) else load_config(config)
    run_id = validate_run_id(run_id or make_run_id(document))
    folder = Path(out) / run_id
    configure_run_logging(folder)
    use_temp(folder / LOCAL / "duckdb")
    run_store = VolumeStore(folder)
    landing = Landing(
        folder / LOCAL / "data.duckdb",
        sql_audit=SqlAudit(run_store, "local"),
    )
    if target_factory is None:
        from dtrack_left.backends.databricks import DEFAULT_PROFILE, workspace

        client = workspace(profile or os.getenv("DTRACK_DBX_PROFILE") or DEFAULT_PROFILE)
        warehouse = Warehouse(
            client,
            warehouse_id or os.getenv("DTRACK_DBX_WAREHOUSE_ID", ""),
            sql_audit=SqlAudit(run_store, "right"),
        )
        work = folder / LOCAL / "right"

        def target_factory(right: dict[str, Any]):
            return WarehouseTarget(warehouse, right, work, landing=landing)

    run = LocalRun(document, run_id, folder, target_factory, landing, pairs)
    log.info("local run %s at %s; errors -> %s", run_id, folder, run.error_log)
    return run


# >>> both sides reachable before any counting: Left SELECT ... WHERE 1=0, Right DESCRIBE QUERY <<< #
def preflight(run: LocalRun) -> dict[str, dict[str, Any]]:
    result = {}
    for name, pair in run.config["pairs"].items():
        if pair.get("skip") or (run.pairs and name not in run.pairs):
            continue
        source = attach_sql_audit(
            source_from_config(pair["left"]), SqlAudit(run.store, "left", pair=name)
        )
        target = attach_sql_audit(
            run.target_factory(pair["right"]), SqlAudit(run.store, "right", pair=name)
        )
        result[name] = {"left": source.preflight(), "right": target.preflight()}
        for side in ("left", "right"):
            health = result[name][side]
            if not health.get("ok"):
                log.error(
                    "preflight %s %s failed (%s): %s",
                    name,
                    side,
                    health.get("source", "unknown source"),
                    health.get("error", "unknown error"),
                )
        log.info(
            "preflight %s: left=%s right=%s",
            name,
            result[name]["left"].get("ok"),
            result[name]["right"].get("ok"),
        )
    SqlAudit(run.store, "local").refresh()
    return result


# >>> Stage 1: Left counts, Right counts, reconcile and gate; manifests for every pair that passed <<< #
def row_check(run: LocalRun, *, gate: str = "auto", workers: int = 4) -> dict[str, Any]:
    left_row_check(
        run.config, run.store, run_id=run.run_id, pair_names=run.pairs, pair_workers=workers
    )
    right_row_check(run.store, run.target_factory, gate=gate)
    verdicts = {
        key.removeprefix("row_check/").removesuffix(".json"): value
        for key, value in run.store.list_json("row_check/")
        if key.count(".") == 1 and not key.startswith("row_check/_")
    }
    return {
        pair: {
            "summary": document.get("summary"),
            "gate": document.get("gate_decision"),
        }
        for pair, document in verdicts.items()
    }


# >>> Stage 2: publish Left units while the Right side prefetches and compares them concurrently <<< #
def col_check(
    run: LocalRun, *, pull_workers: int = 2, stat_workers: int = 2, max_attempts: int = 3
) -> dict[str, Any]:
    try:
        return _col_check(
            run,
            pull_workers=pull_workers,
            stat_workers=stat_workers,
            max_attempts=max_attempts,
        )
    except Exception as exc:
        log_exception_once(log, "local col-check failed", exc)
        raise


def _col_check(
    run: LocalRun, *, pull_workers: int, stat_workers: int, max_attempts: int
) -> dict[str, Any]:
    manifests = wait_for_manifests(run.store, timeout=0, poll_interval=0, pair_names=run.pairs)
    left_work = run.local / "left"
    for manifest in manifests:
        publish_fixed_keys(run.store, manifest, left_work)
    runtime = LandingLeftRuntime(left_work, run.store, run.landing)
    producer = ColCheckPipeline(
        run.store,
        pull_window=runtime.pull_window,
        compute_stats=runtime.compute_stats,
        pull_workers=pull_workers,
        stat_workers=stat_workers,
        wip=NEVER_BLOCK,
    )
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="dtrack-left") as executor:
        producer_job = executor.submit(producer.run, manifests)

        def producer_health() -> bool:
            if not producer_job.done():
                return False
            producer_job.result()
            if not run.store.exists("manifest/_CLOSED"):
                raise RuntimeError("Left col-check finished without closing the manifest")
            return False

        results = ColCheck(
            run.store,
            run.target_factory,
            max_attempts=max_attempts,
            poll_interval=0.2,
            on_idle=producer_health,
        ).run()
        published = producer_job.result()
    verdicts: dict[str, int] = {}
    for row in results:
        verdicts[row["verdict"]] = verdicts.get(row["verdict"], 0) + 1
    return {
        "published_units": published,
        "compared": len(results),
        "verdicts": verdicts,
        "dead": len(run.store.list_keys("dead/")),
    }


# >>> HTML, XLSX, CSVs and summary.json into <run>/report/ <<< #
def report(run: LocalRun) -> dict[str, Any]:
    SqlAudit(run.store, "local").refresh()
    prune_audit_history(VolumeStore(run.folder.parent), keep=5)
    files = build_report(run.store, run.folder / "report")
    summary = json.loads(files["summary.json"].read_text())
    run.store.put_json(
        "_STATUS.json",
        {
            "state": "complete",
            "role": "local",
            "run_id": run.run_id,
            "results": summary["units_completed"],
        },
    )
    return {
        "summary": summary,
        "files": {k: str(v) for k, v in files.items()},
    }


# >>> copy the run folder to s3://.../runs/<run_id>/, laid out like a distributed run; pulled data only when asked <<< #
def backup(run: LocalRun, s3_root: str) -> dict[str, Any]:
    runs_store = S3Store(s3_root.rstrip("/"))
    target = runs_store.child(run.run_id)
    uploaded, skipped = [], 0
    for path in sorted(run.folder.rglob("*")):
        if not path.is_file() or path.name.endswith(".part"):
            continue
        relative = path.relative_to(run.folder).as_posix()
        if relative.startswith(f"{LOCAL}/"):
            skipped += 1
            continue
        content_type = "application/json" if path.suffix == ".json" else "application/octet-stream"
        target.put_bytes(relative, path.read_bytes(), content_type=content_type)
        uploaded.append(relative)
    log.info(
        "backup %s: %d file(s) -> %s (%d local file(s) kept back)",
        run.run_id,
        len(uploaded),
        target.root_uri,
        skipped,
    )
    prune_audit_history(runs_store, keep=5)
    return {"uri": target.root_uri, "files": len(uploaded), "skipped": skipped}


# >>> the whole backup plan in one call: preflight, Stage 1, Stage 2, report, backup <<< #
def run_all(
    run: LocalRun,
    *,
    gate: str = "auto",
    run_s3: str | None = None,
) -> dict[str, Any]:
    health = preflight(run)
    failed = [
        f"{name}.{side}: {row[side].get('error')}"
        for name, row in health.items()
        for side in ("left", "right")
        if not row[side].get("ok")
    ]
    if failed:
        raise RuntimeError("preflight failed: " + "; ".join(failed))
    stage_one = row_check(run, gate=gate)
    stage_two = col_check(run) if run.store.exists("row_check/_CLOSED") else None
    landing = run.landing.tables()
    built = report(run)
    saved = backup(run, run_s3) if run_s3 else None
    return {
        "run_id": run.run_id,
        "folder": str(run.folder),
        "row_check": stage_one,
        "col_check": stage_two,
        "status": built["summary"]["status"],
        "findings": built["summary"]["findings"],
        "backup": saved,
        "landing": landing,
        "error_log": str(run.error_log),
    }
