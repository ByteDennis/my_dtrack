from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class AttributeCheck(ABC):
    name = ""

    @abstractmethod
    def compare(
        self, left: dict[str, Any], right: dict[str, Any]
    ) -> tuple[dict[str, Any], bool | None]: ...

    def result_match(self, result: dict[str, Any]) -> bool | None:
        match = result.get(f"{self.name}_match")
        if match is not None:
            return bool(match)
        difference = result.get(f"{self.name}_diff")
        return None if difference is None else difference == 0


ATTRIBUTE_CHECKS: dict[str, AttributeCheck] = {}


def register_attribute_check(cls: type[AttributeCheck]) -> type[AttributeCheck]:
    check = cls()
    if not check.name or check.name in ATTRIBUTE_CHECKS:
        raise ValueError(f"invalid or duplicate attribute check: {check.name!r}")
    ATTRIBUTE_CHECKS[check.name] = check
    return cls


def attribute_check_names() -> tuple[str, ...]:
    return tuple(ATTRIBUTE_CHECKS)


def get_attribute_check(name: str) -> AttributeCheck:
    try:
        return ATTRIBUTE_CHECKS[name]
    except KeyError:
        raise ValueError(f"unknown attribute check: {name}") from None


def _number(row: dict[str, Any], name: str) -> int:
    value = row.get(name)
    return 0 if value in (None, "") else int(value)


def _float_match(left: Any, right: Any, tolerance: float = 1e-6) -> bool | None:
    if left in (None, "") or right in (None, ""):
        return None
    try:
        left_number, right_number = float(left), float(right)
    except (TypeError, ValueError):
        return str(left).strip() == str(right).strip()
    return abs(left_number - right_number) <= max(
        1e-9, tolerance * max(abs(left_number), abs(right_number), 1.0)
    )


class DifferenceCheck(AttributeCheck):
    def compare(self, left, right):
        left_value, right_value = _number(left, self.name), _number(right, self.name)
        difference = right_value - left_value
        return {
            f"{self.name}_left": left_value,
            f"{self.name}_right": right_value,
            f"{self.name}_diff": difference,
        }, difference == 0


class FloatCheck(AttributeCheck):
    include_difference = False

    def compare(self, left, right):
        left_value, right_value = left.get(self.name), right.get(self.name)
        match = _float_match(left_value, right_value)
        result = {
            f"{self.name}_left": left_value,
            f"{self.name}_right": right_value,
            f"{self.name}_match": match,
        }
        if self.include_difference:
            try:
                result[f"{self.name}_diff"] = float(right_value) - float(left_value)
            except (TypeError, ValueError):
                result[f"{self.name}_diff"] = None
        return result, match


class ExtremeCheck(AttributeCheck):
    def compare(self, left, right):
        field = f"{self.name}_val"
        left_value = "" if left.get(field) is None else str(left[field]).strip()
        right_value = "" if right.get(field) is None else str(right[field]).strip()
        match = left_value == right_value
        if not match and "numeric" in (left.get("col_type"), right.get("col_type")):
            match = bool(left_value and right_value and _float_match(left_value, right_value, 0))
        return {
            f"{self.name}_left": left_value,
            f"{self.name}_right": right_value,
            f"{self.name}_match": match,
        }, match


@register_attribute_check
class NTotalCheck(DifferenceCheck):
    name = "n_total"


@register_attribute_check
class NMissingCheck(DifferenceCheck):
    name = "n_missing"


@register_attribute_check
class NUniqueCheck(DifferenceCheck):
    name = "n_unique"


@register_attribute_check
class MeanCheck(FloatCheck):
    name = "mean"
    include_difference = True


@register_attribute_check
class StdCheck(FloatCheck):
    name = "std"
    include_difference = True


@register_attribute_check
class MinCheck(ExtremeCheck):
    name = "min"


@register_attribute_check
class MaxCheck(ExtremeCheck):
    name = "max"


@register_attribute_check
class LengthAvgCheck(FloatCheck):
    name = "length_avg"


@register_attribute_check
class LengthMaxCheck(FloatCheck):
    name = "length_max"


@register_attribute_check
class HashSumCheck(FloatCheck):
    name = "hash_sum"
