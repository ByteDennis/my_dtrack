from __future__ import annotations

import hashlib
import math
import re
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any, ClassVar

from ..sql.dates import DateType, canonical_date, resolve_date_type

NULL = "<<NULL>>"
PURE_DECIMAL = re.compile(r"^-?[0-9]+\.[0-9]+$")


# >>> trailing decimal zeros collapse ONLY on pure decimals; leading zeros ('007') and exponents stay <<< #
def collapse_decimal(text: str) -> str:
    if not PURE_DECIMAL.match(text):
        return text
    text = re.sub(r"(\.[0-9]*[1-9])0+$", r"\1", text)
    return re.sub(r"\.0+$", "", text)


# >>> Spark SQL twin of collapse_decimal: pure decimals lose trailing zeros, nothing else changes <<< #
def collapse_decimal_sql(text: str) -> str:
    collapsed = (
        f"regexp_replace(regexp_replace({text}, '(\\\\.[0-9]*[1-9])0+$', '$1'), '\\\\.0+$', '')"
    )
    return f"CASE WHEN {text} rlike '^-?[0-9]+\\\\.[0-9]+$' THEN {collapsed} ELSE {text} END"


def spark_literal(text: str) -> str:
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _strip_zeros(text: str) -> str:
    return re.sub(r"^(-?)0+(?=[0-9])", r"\1", re.sub(r"^\+(?=[0-9.])", "", text))


def _strip_zeros_sql(text: str) -> str:
    unsigned = f"regexp_replace({text}, '^\\\\+(?=[0-9.])', '')"
    return f"regexp_replace({unsigned}, '^(-?)0+(?=[0-9])', '$1')"


def _date(text: str) -> str:
    try:
        return canonical_date(text)
    except ValueError:
        return ""


def _date_sql(text: str) -> str:
    def parse(value: str, pattern: str) -> str:
        return f"cast(try_to_timestamp({value}, '{pattern}') as date)"

    undashed = f"replace({text}, '-', '')"
    day = (
        f"CASE WHEN {text} rlike '^[0-9]{{8}}$' THEN {parse(text, 'yyyyMMdd')} "
        f"WHEN {text} rlike '^[0-9]{{1,2}}/[0-9]{{1,2}}/[0-9]{{4}}$' THEN {parse(text, 'M/d/yyyy')} "
        f"WHEN {text} rlike '^[0-9]{{1,2}}-?[A-Za-z]{{3}}-?[0-9]{{4}}$' "
        f"THEN {parse(undashed, 'dMMMyyyy')} "
        f"ELSE try_cast({text} as date) END"
    )
    return f"coalesce(date_format({day}, 'yyyy-MM-dd'), '')"


def _lpad(text: str, width: str) -> str:
    return text.rjust(int(width), "0")


def _lpad_sql(text: str, width: str) -> str:
    size = int(width)
    return f"CASE WHEN length({text}) >= {size} THEN {text} ELSE lpad({text}, {size}, '0') END"


def _strip_prefix_sql(text: str, prefix: str) -> str:
    n = len(prefix)
    return (
        f"CASE WHEN substring({text}, 1, {n}) = {spark_literal(prefix)} "
        f"THEN substring({text}, {n + 1}) ELSE {text} END"
    )


def _strip_suffix_sql(text: str, suffix: str) -> str:
    n = len(suffix)
    return (
        f"CASE WHEN length({text}) >= {n} AND substring({text}, -{n}) = {spark_literal(suffix)} "
        f"THEN substring({text}, 1, length({text}) - {n}) ELSE {text} END"
    )


# >>> one key transformation in two dialects: python(text[, arg]) and spark(sql_text[, arg]) must agree <<< #
@dataclass(frozen=True)
class Step:
    name: str
    python: Callable[..., str]
    spark: Callable[..., str]
    takes_arg: bool = False


STEPS: dict[str, Step] = {}


# >>> add a step usable in every family and in config step lists ("name" or "name:arg") <<< #
def register_step(
    name: str, python: Callable[..., str], spark: Callable[..., str], *, takes_arg: bool = False
) -> Step:
    STEPS[name] = Step(name, python, spark, takes_arg)
    return STEPS[name]


register_step(
    "trim",
    lambda t: re.sub(r"^\s+|\s+$", "", t, flags=re.ASCII),
    lambda t: f"regexp_replace({t}, '^\\\\s+|\\\\s+$', '')",
)
register_step("lower", str.lower, lambda t: f"lower({t})")
register_step("upper", str.upper, lambda t: f"upper({t})")
register_step("collapse_decimal", collapse_decimal, collapse_decimal_sql)
register_step("strip_leading_zeros", _strip_zeros, _strip_zeros_sql)
register_step(
    "digits",
    lambda t: re.sub(r"[^0-9]", "", t),
    lambda t: f"regexp_replace({t}, '[^0-9]', '')",
)
register_step(
    "alnum",
    lambda t: re.sub(r"[^0-9A-Za-z]", "", t),
    lambda t: f"regexp_replace({t}, '[^0-9A-Za-z]', '')",
)
register_step("date", _date, _date_sql)
register_step("lpad", _lpad, _lpad_sql, takes_arg=True)
register_step("strip_prefix", lambda t, p: t.removeprefix(p), _strip_prefix_sql, takes_arg=True)
register_step("strip_suffix", lambda t, s: t.removesuffix(s), _strip_suffix_sql, takes_arg=True)


# >>> "name" or "name:arg" checked against STEPS; returns (step, arg) pairs <<< #
def parse_steps(steps: Iterable[str]) -> tuple[tuple[Step, str | None], ...]:
    if isinstance(steps, str):
        raise TypeError("steps must be a list of step names, not one string")
    parsed = []
    for item in steps:
        name, sep, arg = str(item).partition(":")
        step = STEPS.get(name)
        if step is None:
            raise ValueError(f"unknown key step {name!r}; known: {', '.join(sorted(STEPS))}")
        if step.takes_arg != bool(sep):
            raise ValueError(f"key step {name!r} {'needs' if step.takes_arg else 'takes no'} :arg")
        parsed.append((step, arg if sep else None))
    return tuple(parsed)


def _run_python(parsed, text: str) -> str:
    for step, arg in parsed:
        text = step.python(text) if arg is None else step.python(text, arg)
    return text


def _run_spark(parsed, text: str) -> str:
    for step, arg in parsed:
        text = step.spark(text) if arg is None else step.spark(text, arg)
    return text


# >>> how one key is normalised before hashing; subclass and override any *_transform for asymmetric keys <<< #
class KeyFamily:
    name: ClassVar[str] = "custom"
    steps: ClassVar[tuple[str, ...]] = ("trim",)
    left_steps: ClassVar[tuple[str, ...] | None] = None
    right_steps: ClassVar[tuple[str, ...] | None] = None

    def __init__(self, steps=None, left_steps=None, right_steps=None, name: str | None = None):
        self.name = name or type(self).name
        self.steps = tuple(steps if steps is not None else type(self).steps)
        left = left_steps if left_steps is not None else type(self).left_steps
        right = right_steps if right_steps is not None else type(self).right_steps
        self.left_steps = tuple(left) if left is not None else None
        self.right_steps = tuple(right) if right is not None else None
        self._parsed = parse_steps(self.steps)
        self._left = parse_steps(self.left_steps) if self.left_steps is not None else None
        self._right = parse_steps(self.right_steps) if self.right_steps is not None else None

    def transform(self, text: str) -> str:
        return _run_python(self._parsed, text)

    def left_transform(self, text: str) -> str:
        return self.transform(text) if self._left is None else _run_python(self._left, text)

    def right_transform(self, text: str) -> str:
        return self.transform(text) if self._right is None else _run_python(self._right, text)

    def transform_sql(self, text: str) -> str:
        return _run_spark(self._parsed, text)

    def left_transform_sql(self, text: str) -> str:
        return self.transform_sql(text) if self._left is None else _run_spark(self._left, text)

    def right_transform_sql(self, text: str) -> str:
        return self.transform_sql(text) if self._right is None else _run_spark(self._right, text)

    def normalize(self, value: object, side: str = "left") -> str:
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return NULL
        text = (
            self.left_transform(str(value)) if side == "left" else self.right_transform(str(value))
        )
        return text or NULL

    def sql(self, expr: str, side: str = "right") -> str:
        text = f"cast({expr} as string)"
        body = self.left_transform_sql(text) if side == "left" else self.right_transform_sql(text)
        return f"coalesce(nullif({body}, ''), '{NULL}')"

    def spec(self) -> str | dict[str, Any]:
        registered = FAMILIES.get(self.name)
        if registered is not None and type(registered) is type(self) and self == registered:
            return self.name
        spec: dict[str, Any] = {"transform": list(self.steps)}
        if self.left_steps is not None:
            spec["left_transform"] = list(self.left_steps)
        if self.right_steps is not None:
            spec["right_transform"] = list(self.right_steps)
        return spec

    def __eq__(self, other: object) -> bool:
        return type(other) is type(self) and (self.steps, self.left_steps, self.right_steps) == (
            other.steps,
            other.left_steps,
            other.right_steps,
        )

    def __hash__(self) -> int:
        return hash((type(self), self.steps, self.left_steps, self.right_steps))

    def __repr__(self) -> str:
        return f"KeyFamily({self.name!r}, {self.spec()!r})"


class DateKeyFamily(KeyFamily):
    """A hash family backed by the two sides' physical DateType rules."""

    name = "date_type"

    def __init__(self, date_type=None, left_date_type=None, right_date_type=None):
        base = date_type or DateType("date_key")
        self.left_date_type = resolve_date_type(left_date_type or base)
        self.right_date_type = resolve_date_type(right_date_type or base)
        self.steps = ()
        self.left_steps = self.right_steps = None
        self._parsed = self._left = self._right = ()

    def normalize(self, value: object, side: str = "left") -> str:
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return NULL
        kind = self.left_date_type if side == "left" else self.right_date_type
        try:
            return kind.normalize(value)
        except (TypeError, ValueError, OverflowError):
            return NULL

    def sql_dialect(self, expr: str, dialect: str, side: str) -> str:
        kind = self.left_date_type if side == "left" else self.right_date_type
        body = kind.normalize_sql(expr, dialect)
        return f"coalesce(nullif({body}, ''), '{NULL}')"

    def sql(self, expr: str, side: str = "right") -> str:
        return self.sql_dialect(expr, "spark", side)

    def spec(self) -> dict[str, Any]:
        return {
            "family": self.name,
            "left_date_type": self.left_date_type.spec(portable=True),
            "right_date_type": self.right_date_type.spec(portable=True),
        }

    def __eq__(self, other: object) -> bool:
        return isinstance(other, DateKeyFamily) and self.spec() == other.spec()

    def __repr__(self) -> str:
        return f"DateKeyFamily({self.spec()!r})"


FAMILIES: dict[str, KeyFamily] = {}


# >>> register a family by name: register_family("x", ["trim"]) or @register_family("x") on a subclass <<< #
def register_family(name: str, steps=None, left_steps=None, right_steps=None):
    def add(cls: type[KeyFamily]) -> type[KeyFamily]:
        cls.name = name
        FAMILIES[name] = cls()
        return cls

    if steps is None and left_steps is None and right_steps is None:
        return add
    FAMILIES[name] = KeyFamily(steps or ("trim",), left_steps, right_steps, name=name)
    return FAMILIES[name]


register_family("string", ["trim", "lower"])
register_family("exact", ["trim"])
register_family("numeric", ["trim", "strip_leading_zeros", "collapse_decimal"])
register_family("date", ["trim", "date"])
register_family("digits", ["digits"])
FAMILIES["date_type"] = DateKeyFamily()


# >>> config value -> KeyFamily: "name" | ["step", ...] | {"family"?, "transform"?, "left_transform"?, "right_transform"?} <<< #
def key_family(spec: Any = "string") -> KeyFamily:
    if isinstance(spec, KeyFamily):
        return spec
    if spec is None or isinstance(spec, str):
        name = spec or "string"
        if name not in FAMILIES:
            raise ValueError(
                f"unsupported key family {name!r}; known: {', '.join(sorted(FAMILIES))}"
            )
        return FAMILIES[name]
    if isinstance(spec, list | tuple):
        return KeyFamily(spec)
    if isinstance(spec, Mapping):
        date_fields = {"date_type", "left_date_type", "right_date_type"}
        if spec.get("family") == "date_type" or set(spec) & date_fields:
            unknown = set(spec) - {"family", *date_fields}
            if unknown:
                raise ValueError(
                    f"unsupported date key family fields: {', '.join(sorted(unknown))}"
                )
            return DateKeyFamily(
                spec.get("date_type"),
                spec.get("left_date_type"),
                spec.get("right_date_type"),
            )
        unknown = set(spec) - {"family", "transform", "left_transform", "right_transform"}
        if unknown:
            raise ValueError(f"unsupported key family fields: {', '.join(sorted(unknown))}")
        base = key_family(spec.get("family") or "string")
        return KeyFamily(
            spec.get("transform", base.steps),
            spec.get("left_transform", base.left_steps),
            spec.get("right_transform", base.right_steps),
        )
    raise TypeError(f"key family must be a name, a step list or an object, got {spec!r}")


def key_families(specs: Iterable[Any] | None, count: int) -> list[KeyFamily]:
    specs = list(specs or [])
    return [key_family(specs[i] if i < len(specs) else "string") for i in range(count)]


HASH_BUCKET_SPACE = 2**32


def hash32(value: str) -> int:
    return int(hashlib.md5(value.encode("utf-8")).hexdigest()[:8], 16)


def composite_key(values, families, side: str) -> str:
    return "|".join(family.normalize(value, side) for value, family in zip(values, families))


def selected_fraction(
    values: list[object], families: list[object], threshold: int, side: str = "left"
) -> bool:
    return hash32(composite_key(values, key_families(families, len(values)), side)) < int(threshold)
