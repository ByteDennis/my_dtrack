from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta


def canonical_date(value: object) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    for fmt in ("%Y-%m-%d", "%Y%m%d", "%m/%d/%Y", "%d%b%Y", "%d-%b-%Y"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()  # noqa: DTZ007
        except ValueError:
            continue
    raise ValueError(f"unsupported date value: {value!r}")


def bucket_date(value: str, vintage: str) -> str:
    day = date.fromisoformat(canonical_date(value))
    if vintage == "day":
        return day.isoformat()
    if vintage == "week":
        return (day - timedelta(days=day.weekday())).isoformat()
    if vintage == "month":
        return day.replace(day=1).isoformat()
    if vintage == "quarter":
        return day.replace(month=((day.month - 1) // 3) * 3 + 1, day=1).isoformat()
    if vintage == "year":
        return day.replace(month=1, day=1).isoformat()
    raise ValueError(f"unsupported vintage: {vintage}")


def apply_synonyms(
    counts: dict[str, int], synonyms: dict[str, str] | None = None
) -> dict[str, int]:
    synonyms = synonyms or {}
    result: dict[str, int] = defaultdict(int)
    for raw, count in counts.items():
        canonical = synonyms.get(str(raw), canonical_date(raw))
        result[canonical_date(canonical)] += int(count)
    return dict(result)


def contiguous_runs(partitions: list[str]) -> list[tuple[str, str]]:
    days = sorted({date.fromisoformat(canonical_date(item)) for item in partitions})
    if not days:
        return []
    runs: list[tuple[str, str]] = []
    start = previous = days[0]
    for current in days[1:]:
        if current != previous + timedelta(days=1):
            runs.append((start.isoformat(), previous.isoformat()))
            start = current
        previous = current
    runs.append((start.isoformat(), previous.isoformat()))
    return runs
