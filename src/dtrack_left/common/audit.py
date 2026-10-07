from __future__ import annotations

import hashlib
import logging
import re
import threading
import traceback
import uuid
from datetime import UTC, datetime
from typing import Any

log = logging.getLogger("dtrack.sql_audit")
_LOCKS: dict[str, threading.RLock] = {}
_LOCKS_GUARD = threading.Lock()
_CAPTURED: set[tuple[str, str, str, str, str, str]] = set()
_CAPTURED_GUARD = threading.Lock()
SKIPPED_OPERATIONS = {
    "attach",
    "session",
    "landing_create",
    "landing_replace",
    "landing_insert",
    "landing_count",
    "landing_inspect",
}
MUTATING_SQL = re.compile(
    r"^\s*(?:INSERT|UPDATE|DELETE|MERGE|CREATE|ALTER|DROP|TRUNCATE|GRANT|REVOKE)\b",
    re.IGNORECASE,
)
SQL_STRING = re.compile(r"'(?:''|[^'])*'")
SQL_NUMBER = re.compile(r"(?<![\w.])[-+]?\d+(?:\.\d+)?(?![\w.])")
SQL_IN_LIST = re.compile(r"\bIN\s*\(\s*\?(?:\s*,\s*\?)+\s*\)", re.IGNORECASE)


def _lock_for(store) -> threading.RLock:
    key = str(getattr(store, "root_uri", id(store)))
    with _LOCKS_GUARD:
        return _LOCKS.setdefault(key, threading.RLock())


def sql_shape(sql: str) -> str:
    normalized = SQL_STRING.sub("?", str(sql))
    normalized = SQL_NUMBER.sub("?", normalized)
    normalized = SQL_IN_LIST.sub("IN (?)", normalized)
    normalized = " ".join(normalized.split()).lower()
    return hashlib.sha256(normalized.encode()).hexdigest()[:16]


class SqlAudit:
    """Best-effort immutable SQL events plus a human-readable run-level audit.txt."""

    def __init__(self, store, side: str, pair: str | None = None):
        self.store = store
        self.side = side
        self.pair = pair
        self.lock = _lock_for(store)

    def _block(
        self,
        sql: str,
        *,
        event_id: str,
        timestamp: str,
        backend: str,
        operation: str,
        audit_key: str,
        shape: str,
    ) -> bytes:
        fields = [
            f"timestamp={timestamp}",
            f"event_id={event_id}",
            f"side={self.side}",
            f"backend={backend}",
            f"operation={operation}",
            f"audit_key={audit_key}",
            f"shape={shape}",
            "policy=first_per_shape",
        ]
        if self.pair:
            fields.append(f"pair={self.pair}")
        statement = str(sql).rstrip()
        return ("-- " + " ".join(fields) + "\n" + statement + "\n\n").encode("utf-8")

    def refresh(self) -> bool:
        try:
            with self.lock:
                keys = self.store.list_keys("audit/")
                blocks = []
                seen = set()
                for key in keys:
                    block = self.store.get_bytes(key)
                    header = block.split(b"\n", 1)[0].decode("utf-8", errors="replace")
                    match = re.search(r"\baudit_key=([^\s]+)", header)
                    audit_key = match.group(1) if match else key
                    if audit_key in seen:
                        continue
                    seen.add(audit_key)
                    _header, separator, statement = block.partition(b"\n")
                    sql = statement if separator else block
                    blocks.append(sql.rstrip() + b"\n\n")
                body = b"".join(blocks)
                self.store.put_bytes("audit.txt", body, content_type="text/plain; charset=utf-8")
            return True
        except Exception:
            log.debug("could not refresh audit.txt", exc_info=True)
            return False

    def record(self, sql: str, *, backend: str, operation: str = "query") -> str | None:
        if operation in SKIPPED_OPERATIONS:
            return None
        now = datetime.now(UTC)
        timestamp = now.isoformat(timespec="microseconds").replace("+00:00", "Z")
        event_id = f"{now.strftime('%Y%m%dT%H%M%S%fZ')}-{uuid.uuid4().hex}"
        shape = sql_shape(sql)
        root = str(getattr(self.store, "root_uri", id(self.store)))
        capture = (root, self.side, self.pair or "", backend, operation, shape)
        mutating = bool(MUTATING_SQL.match(str(sql)))
        if not mutating:
            with _CAPTURED_GUARD:
                if capture in _CAPTURED:
                    return None
                _CAPTURED.add(capture)
        audit_key = hashlib.sha256("|".join(capture).encode()).hexdigest()[:20]
        if mutating:
            audit_key = f"{audit_key}-{event_id}"
        key = f"audit/{event_id}.txt"
        block = self._block(
            sql,
            event_id=event_id,
            timestamp=timestamp,
            backend=backend,
            operation=operation,
            audit_key=audit_key,
            shape=shape,
        )
        try:
            with self.lock:
                self.store.put_bytes(
                    key,
                    block,
                    content_type="text/plain; charset=utf-8",
                )
            return key
        except Exception as exc:  # noqa: BLE001 - audit failure must never block the SQL
            if not mutating:
                with _CAPTURED_GUARD:
                    _CAPTURED.discard(capture)
            failure: dict[str, Any] = {
                "event_id": event_id,
                "timestamp": timestamp,
                "side": self.side,
                "pair": self.pair,
                "backend": backend,
                "operation": operation,
                "audit_key": audit_key,
                "shape": shape,
                "error": str(exc),
                "traceback": "".join(traceback.format_exception(exc)),
            }
            try:
                self.store.put_json(f"audit_errors/{event_id}.json", failure)
            except Exception:
                log.debug("could not persist SQL audit failure", exc_info=True)
            return None


def prune_audit_history(runs_store, *, keep: int = 5) -> list[str]:
    """Remove detailed SQL event files from older runs while retaining audit.txt."""
    if keep < 1:
        raise ValueError("audit history retention must keep at least one run")
    try:
        runs = []
        for run_id in runs_store.list_prefixes():
            run_store = runs_store.child(run_id)
            keys = run_store.list_keys("audit/")
            if keys:
                runs.append((max(keys), run_id, run_store, keys))
        removed = []
        for _latest, run_id, run_store, keys in sorted(runs, reverse=True)[keep:]:
            run_store.delete_keys(keys)
            removed.append(run_id)
        if removed:
            log.info("pruned detailed SQL audit events from old runs: %s", ", ".join(removed))
        return removed
    except Exception:
        log.debug("could not prune detailed SQL audit history", exc_info=True)
        return []


def attach_sql_audit(target, audit: SqlAudit):
    setter = getattr(target, "set_sql_audit", None)
    if setter:
        setter(audit)
    else:
        target.sql_audit = audit
    return target
