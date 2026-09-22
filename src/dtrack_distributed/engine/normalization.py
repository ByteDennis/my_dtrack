from __future__ import annotations

import hashlib
import re
import unicodedata

from .dates import canonical_date

HASH_BUCKET_SPACE = 2**32
PURE_DECIMAL = re.compile(r"^-?[0-9]+\.[0-9]+$")


# >>> trailing decimal zeros collapse ONLY on pure decimals; leading zeros ('007') and exponents stay <<< #
def collapse_decimal(text: str) -> str:
    if not PURE_DECIMAL.match(text):
        return text
    text = re.sub(r"(\.[0-9]*[1-9])0+$", r"\1", text)
    return re.sub(r"\.0+$", "", text)


def normalize_value(value: object, family: str = "string") -> str:
    if value is None:
        return "<<NULL>>"
    text = str(value).strip()
    if not text:
        return "<<NULL>>"
    if family == "date":
        return canonical_date(text)
    if family == "numeric":
        return collapse_decimal(text)
    return unicodedata.normalize("NFC", text).lower()


def hash32(value: str) -> int:
    return int(hashlib.md5(value.encode("utf-8")).hexdigest()[:8], 16)


def selected_fraction(values: list[object], families: list[str], threshold: int) -> bool:
    composite = "|".join(normalize_value(value, family) for value, family in zip(values, families))
    return hash32(composite) < int(threshold)
