from __future__ import annotations

import logging
import os
import tempfile
from pathlib import Path

log = logging.getLogger("dtrack.duck")
DEFAULT_SHARE = 4
DEFAULT_FRACTION = 0.6
_TEMP: str | None = None
_WARNED: set[str] = set()


# >>> spill and scratch databases under the run's own work directory (same disk and permissions as the pulled CSVs) <<< #
def use_temp(path: str | Path | None) -> None:
    global _TEMP
    _TEMP = None if path is None else str(path)


# >>> DTRACK_DUCKDB_TEMP, else the work directory registered by the run, else <system temp>/dtrack-duckdb <<< #
def temp_directory() -> str:
    return (
        os.getenv("DTRACK_DUCKDB_TEMP")
        or _TEMP
        or str(Path(tempfile.gettempdir()) / "dtrack-duckdb")
    )


# >>> physical RAM in bytes (Linux/macOS sysconf, Windows GlobalMemoryStatusEx); None when unknown <<< #
def physical_memory() -> int | None:
    try:
        return os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")
    except (ValueError, OSError, AttributeError):
        pass
    try:
        import ctypes

        class Status(ctypes.Structure):
            _fields_ = [
                ("dwLength", ctypes.c_ulong),
                ("dwMemoryLoad", ctypes.c_ulong),
                ("ullTotalPhys", ctypes.c_ulonglong),
                ("ullAvailPhys", ctypes.c_ulonglong),
                ("ullTotalPageFile", ctypes.c_ulonglong),
                ("ullAvailPageFile", ctypes.c_ulonglong),
                ("ullTotalVirtual", ctypes.c_ulonglong),
                ("ullAvailVirtual", ctypes.c_ulonglong),
                ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
            ]

        status = Status(dwLength=ctypes.sizeof(Status))
        if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
            return int(status.ullTotalPhys)
    except (AttributeError, OSError):
        pass
    return None


# >>> "16GB" / "512MB" / "2.5G" / plain bytes -> bytes <<< #
def parse_size(text: str) -> int:
    raw = text.strip().upper().replace("IB", "B")
    units = {
        "TB": 2**40,
        "GB": 2**30,
        "MB": 2**20,
        "KB": 2**10,
        "T": 2**40,
        "G": 2**30,
        "M": 2**20,
        "K": 2**10,
        "B": 1,
    }
    for suffix, factor in units.items():
        if raw.endswith(suffix):
            return int(float(raw[: -len(suffix)].strip()) * factor)
    return int(float(raw))


# >>> the process-wide DuckDB budget: DTRACK_DUCKDB_MEMORY, else 60% of physical RAM, split across DTRACK_DUCKDB_SHARE connections <<< #
def settings() -> dict[str, str]:
    share = max(1, int(os.getenv("DTRACK_DUCKDB_SHARE") or DEFAULT_SHARE))
    budget = os.getenv("DTRACK_DUCKDB_MEMORY")
    total = parse_size(budget) if budget else physical_memory()
    result: dict[str, str] = {}
    if total:
        per = max(256 * 2**20, int((total if budget else total * DEFAULT_FRACTION) / share))
        result["memory_limit"] = f"{per // 2**20}MiB"
    threads = os.getenv("DTRACK_DUCKDB_THREADS")
    cpus = os.cpu_count() or 1
    result["threads"] = str(max(1, int(threads) if threads else cpus // share))
    result["temp_directory"] = temp_directory()
    return result


# >>> create the spill directory, falling back to the system temp when the chosen one is not writable <<< #
def _ensure_temp(path: str) -> str:
    try:
        Path(path).mkdir(parents=True, exist_ok=True)
        return path
    except OSError as exc:
        fallback = str(Path(tempfile.gettempdir()) / "dtrack-duckdb")
        if path not in _WARNED:
            _WARNED.add(path)
            log.warning("duckdb temp %s unusable (%s); using %s", path, exc, fallback)
        Path(fallback).mkdir(parents=True, exist_ok=True)
        return fallback


# >>> every DuckDB connection in the Left pipeline: budgeted memory and threads, spilling to a temp directory <<< #
def connect(path: str | None = None, *, read_only: bool = False, ordered: bool = True):
    import duckdb

    connection = duckdb.connect(path, read_only=read_only) if path else duckdb.connect()
    applied = settings()
    applied["temp_directory"] = _ensure_temp(applied["temp_directory"])
    for key, value in applied.items():
        connection.execute(f"SET {key} = '{value}'")
    if not ordered:
        connection.execute("SET preserve_insertion_order = false")
    return connection
