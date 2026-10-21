from __future__ import annotations

import hashlib
import logging
import re
import threading
import traceback
import uuid
from datetime import datetime
from typing import Any

from .protocol import (
    AUDIT_SUMMARY,
    audit_error_prefix,
    audit_prefix,
    get_run_status,
    timezone_from_name,
)
from .protocol import (
    timestamp as protocol_timestamp,
)

log = logging.getLogger("pair_recon.sql_audit")
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
AUDIT_SQL_MAX_CHARS = 8192
AUDIT_SQL_EDGE_CHARS = 3500
AUDIT_SELECT_ITEM_LIMIT = 20
AUDIT_SELECT_EDGE_ITEMS = 2


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


def _select_ranges(sql: str) -> list[tuple[int, int]]:
    ranges = []
    starts: dict[int, int] = {}
    depth = 0
    index = 0
    while index < len(sql):
        char = sql[index]
        if char in {"'", '"'}:
            quote = char
            index += 1
            while index < len(sql):
                if sql[index] == quote:
                    if index + 1 < len(sql) and sql[index + 1] == quote:
                        index += 2
                        continue
                    index += 1
                    break
                index += 1
            continue
        if sql.startswith("--", index):
            newline = sql.find("\n", index + 2)
            index = len(sql) if newline < 0 else newline + 1
            continue
        if sql.startswith("/*", index):
            closing = sql.find("*/", index + 2)
            index = len(sql) if closing < 0 else closing + 2
            continue
        if char == "(":
            depth += 1
            index += 1
            continue
        if char == ")":
            depth = max(0, depth - 1)
            index += 1
            continue
        if char.isalpha() or char == "_":
            end = index + 1
            while end < len(sql) and (sql[end].isalnum() or sql[end] in {"_", "$", "#"}):
                end += 1
            word = sql[index:end].upper()
            if word == "SELECT":
                starts[depth] = end
            elif word == "FROM" and depth in starts:
                ranges.append((starts.pop(depth), index))
            index = end
            continue
        index += 1
    return ranges


def _projection_items(projection: str) -> list[str]:
    items = []
    start = 0
    depth = 0
    index = 0
    while index < len(projection):
        char = projection[index]
        if char in {"'", '"'}:
            quote = char
            index += 1
            while index < len(projection):
                if projection[index] == quote:
                    if index + 1 < len(projection) and projection[index + 1] == quote:
                        index += 2
                        continue
                    index += 1
                    break
                index += 1
            continue
        if projection.startswith("--", index):
            newline = projection.find("\n", index + 2)
            index = len(projection) if newline < 0 else newline + 1
            continue
        if projection.startswith("/*", index):
            closing = projection.find("*/", index + 2)
            index = len(projection) if closing < 0 else closing + 2
            continue
        if char == "(":
            depth += 1
        elif char == ")":
            depth = max(0, depth - 1)
        elif char == "," and depth == 0:
            items.append(projection[start:index].strip())
            start = index + 1
        index += 1
    items.append(projection[start:].strip())
    return items


def _compact_select_lists(sql: str) -> str:
    edits = []
    for start, end in _select_ranges(sql):
        items = _projection_items(sql[start:end])
        if len(items) <= AUDIT_SELECT_ITEM_LIMIT:
            continue
        omitted = len(items) - (AUDIT_SELECT_EDGE_ITEMS * 2)
        noun = "attribute" if omitted == 1 else "attributes"
        shown = [
            *items[:AUDIT_SELECT_EDGE_ITEMS],
            f"/* ... {omitted} {noun} omitted from audit ... */",
            *items[-AUDIT_SELECT_EDGE_ITEMS:],
        ]
        edits.append((start, end, ", ".join(shown)))
    selected = []
    for edit in sorted(edits, key=lambda item: item[1] - item[0], reverse=True):
        start, end, _replacement = edit
        if any(
            start < chosen_end and end > chosen_start for chosen_start, chosen_end, _ in selected
        ):
            continue
        selected.append(edit)
    compact = sql
    for start, end, replacement in sorted(selected, reverse=True):
        compact = compact[:start] + replacement + compact[end:]
    return compact


def compact_sql(sql: str) -> tuple[str, dict[str, str | int]]:
    statement = str(sql).rstrip()
    compact = _compact_select_lists(statement)
    fields: dict[str, str | int] = {}
    if compact != statement:
        fields["audit_sql_truncated"] = "true"
    if len(compact) > AUDIT_SQL_MAX_CHARS:
        omitted = len(compact) - (AUDIT_SQL_EDGE_CHARS * 2)
        marker = f"\n/* ... {omitted} characters omitted from audit ... */\n"
        compact = compact[:AUDIT_SQL_EDGE_CHARS] + marker + compact[-AUDIT_SQL_EDGE_CHARS:]
        fields.update(
            {
                "executed_sql_chars": len(statement),
                "audit_sql_truncated": "true",
            }
        )
    return compact, fields


class SqlAudit:
    def __init__(self, store, side: str, pair: str | None = None):
        self.store = store
        self.side = side
        self.pair = pair
        self.lock = _lock_for(store)

    def _block(
        self,
        sql: str,
        *,
        timestamp: str,
        backend: str,
        operation: str,
    ) -> bytes:
        statement, compact_fields = compact_sql(sql)
        fields = [
            f"timestamp={timestamp}",
            f"side={self.side}",
            f"backend={backend}",
            f"operation={operation}",
        ]
        fields.extend(f"{key}={value}" for key, value in compact_fields.items())
        return ("-- " + " ".join(fields) + "\n" + statement + "\n\n").encode("utf-8")

    def _event_keys(self) -> list[str]:
        current = [
            key
            for key in self.store.list_keys("pairs/")
            if "/audit/" in key and key.endswith(".sql")
        ]
        current.extend(self.store.list_keys("_status/audit/"))
        return sorted(set(current))

    def refresh(self) -> bool:
        try:
            with self.lock:
                keys = self._event_keys()
                blocks = []
                seen = set()
                entries = []
                for key in keys:
                    block = self.store.get_bytes(key)
                    name = key.rsplit("/", 1)[-1]
                    header = block.split(b"\n", 1)[0].decode("utf-8", errors="replace")
                    match = re.search(r"\baudit_key=([^\s]+)", header)
                    audit_key = (
                        name.rsplit("--", 1)[1].removesuffix(".sql") if "--" in name else None
                    )
                    audit_key = audit_key or (match.group(1) if match else key)
                    timestamp = re.search(r"\btimestamp=([^\s]+)", header)
                    entries.append(
                        (timestamp.group(1) if timestamp else key, key, audit_key, block)
                    )
                for _order, _key, audit_key, block in sorted(entries):
                    if audit_key in seen:
                        continue
                    seen.add(audit_key)
                    _header, separator, statement = block.partition(b"\n")
                    sql = statement if separator else block
                    blocks.append(sql.rstrip() + b"\n\n")
                body = b"".join(blocks)
                self.store.put_bytes(AUDIT_SUMMARY, body, content_type="text/plain; charset=utf-8")
            return True
        except Exception:
            log.debug("could not refresh audit.sql", exc_info=True)
            return False

    def record(self, sql: str, *, backend: str, operation: str = "query") -> str | None:
        if operation in SKIPPED_OPERATIONS:
            return None
        zone = get_run_status(self.store).get("timezone")
        now = datetime.now(timezone_from_name(zone))
        timestamp = protocol_timestamp(self.store)
        event_id = f"{now.strftime('%Y%m%dT%H%M%S%z')}-{uuid.uuid4().hex}"
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
            key = f"{audit_prefix(self.pair)}{event_id}.sql"
        else:
            key = f"{audit_prefix(self.pair)}{audit_key}.sql"
        block = self._block(
            sql,
            timestamp=timestamp,
            backend=backend,
            operation=operation,
        )
        try:
            with self.lock:
                exists = getattr(self.store, "exists", None)
                if not mutating and exists is not None and exists(key):
                    return None
                self.store.put_bytes(
                    key,
                    block,
                    content_type="text/plain; charset=utf-8",
                )
            return key
        except Exception as exc:
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
                self.store.put_json(f"{audit_error_prefix(self.pair)}{event_id}.json", failure)
            except Exception:
                log.debug("could not persist SQL audit failure", exc_info=True)
            return None


def attach_sql_audit(target, audit: SqlAudit):
    setter = getattr(target, "set_sql_audit", None)
    if setter:
        setter(audit)
    else:
        target.sql_audit = audit
    return target
