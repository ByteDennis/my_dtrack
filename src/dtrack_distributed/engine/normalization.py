from __future__ import annotations

import hashlib
import unicodedata
from decimal import Decimal, InvalidOperation

from .dates import canonical_date

HASH_BUCKET_SPACE = 2**32


def normalize_value(value: object, family: str = "string") -> str:
    if value is None:
        return "<<NULL>>"
    text = str(value).strip()
    if not text:
        return "<<NULL>>"
    if family == "date":
        return canonical_date(text)
    if family == "numeric":
        try:
            number = Decimal(text)
            normalized = format(number.normalize(), "f")
            return normalized.rstrip("0").rstrip(".") if "." in normalized else normalized
        except InvalidOperation:
            return text
    return unicodedata.normalize("NFC", text).lower()


def hash32(value: str) -> int:
    return int(hashlib.md5(value.encode("utf-8")).hexdigest()[:8], 16)


def selected_fraction(values: list[object], families: list[str], threshold: int) -> bool:
    composite = "|".join(normalize_value(value, family) for value, family in zip(values, families))
    return hash32(composite) < int(threshold)
