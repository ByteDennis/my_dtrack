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


def collapse_decimal(text: str) -> str:
    if not PURE_DECIMAL.match(text):
        return text
    text = re.sub(r"(\.[0-9]*[1-9])0+$", r"\1", text)
    return re.sub(r"\.0+$", "", text)


def collapse_decimal_sql(text: str) -> str:
    collapsed = (
        f"regexp_replace(regexp_replace({text}, '(\\\\.[0-9]*[1-9])0+$', '$1'), '\\\\.0+$', '')"
    )
    return f"CASE WHEN {text} rlike '^-?[0-9]+\\\\.[0-9]+$' THEN {collapsed} ELSE {text} END"


def spark_literal(text: str) -> str:
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


class SamplingSqlNotSupported(ValueError):
    pass


def dialect_name(value: str) -> str:
    return {
        "aws": "athena",
        "trino": "athena",
        "databricks": "spark",
        "hive": "hadoop",
    }.get(value.lower(), value.lower())


def sql_literal(value: object, dialect: str) -> str:
    if value is None:
        return "NULL"
    text = str(value)
    if dialect_name(dialect) in {"spark", "hadoop"}:
        return spark_literal(text)
    return "'" + text.replace("'", "''") + "'"


# >>> text form of a key expression; Oracle DATE/TIMESTAMP/NUMBER get explicit TO_CHAR formats <<< #
def cast_text_sql(expr: str, dialect: str, data_type: str | None = None) -> str:
    dialect = dialect_name(dialect)
    if dialect == "oracle":
        kind = str(data_type or "").upper()
        if "TIMESTAMP" in kind:
            rendered = f"TO_CHAR({expr}, 'YYYY-MM-DD HH24:MI:SS.FF6')"
            return f"REGEXP_REPLACE({rendered}, '\\.000000$', '')"
        if "DATE" in kind:
            return f"TO_CHAR({expr}, 'YYYY-MM-DD HH24:MI:SS')"
        if any(
            name in kind for name in ("NUMBER", "FLOAT", "DOUBLE", "INTEGER", "DECIMAL", "NUMERIC")
        ):
            return f"TO_CHAR({expr}, 'TM9', 'NLS_NUMERIC_CHARACTERS=''.,''')"
        return f"TO_CHAR({expr})"
    kind = "STRING" if dialect in {"spark", "hadoop"} else "VARCHAR"
    return f"CAST({expr} AS {kind})"


def _regex_replace(expr: str, pattern: str, replacement: str, dialect: str) -> str:
    dialect = dialect_name(dialect)
    args = f"{expr}, {sql_literal(pattern, dialect)}, {sql_literal(replacement, dialect)}"
    if dialect == "duckdb":
        return f"regexp_replace({args}, 'g')"
    name = "REGEXP_REPLACE" if dialect == "oracle" else "regexp_replace"
    return f"{name}({args})"


def _regex_matches(expr: str, pattern: str, dialect: str) -> str:
    dialect = dialect_name(dialect)
    literal = sql_literal(pattern, dialect)
    if dialect == "duckdb":
        return f"regexp_full_match({expr}, {literal})"
    if dialect == "oracle":
        return f"REGEXP_LIKE({expr}, {literal})"
    if dialect == "athena":
        return f"regexp_like({expr}, {literal})"
    return f"{expr} rlike {literal}"


def dialect_date_sql(expr: str, dialect: str) -> str:
    dialect = dialect_name(dialect)
    if dialect in {"spark", "hadoop"}:
        undashed = f"replace({expr}, '-', '')"
        day = (
            f"CASE WHEN {expr} rlike '^[0-9]{{8}}$' "
            f"THEN cast(try_to_timestamp({expr}, 'yyyyMMdd') as date) "
            f"WHEN {expr} rlike '^[0-9]{{1,2}}/[0-9]{{1,2}}/[0-9]{{4}}$' "
            f"THEN cast(try_to_timestamp({expr}, 'M/d/yyyy') as date) "
            f"WHEN {expr} rlike '^[0-9]{{1,2}}-?[A-Za-z]{{3}}-?[0-9]{{4}}$' "
            f"THEN cast(try_to_timestamp({undashed}, 'dMMMyyyy') as date) "
            f"ELSE try_cast({expr} as date) END"
        )
        return f"coalesce(date_format({day}, 'yyyy-MM-dd'), '')"
    if dialect == "duckdb":
        parsed = (
            f"coalesce(try_strptime({expr}, '%Y%m%d'), try_strptime({expr}, '%m/%d/%Y'), "
            f"try_strptime(replace({expr}, '-', ''), '%d%b%Y'), try_cast({expr} as timestamp))"
        )
        return f"coalesce(strftime({parsed}, '%Y-%m-%d'), '')"
    if dialect == "athena":
        parsed = (
            f"coalesce(try(date_parse({expr}, '%Y%m%d')), try(date_parse({expr}, '%m/%d/%Y')), "
            f"try(date_parse(replace({expr}, '-', ''), '%d%b%Y')), try(cast({expr} as timestamp)))"
        )
        return f"coalesce(date_format({parsed}, '%Y-%m-%d'), '')"
    if dialect == "oracle":
        parsed = (
            f"COALESCE(TO_DATE({expr} DEFAULT NULL ON CONVERSION ERROR, 'YYYYMMDD'), "
            f"TO_DATE({expr} DEFAULT NULL ON CONVERSION ERROR, 'MM/DD/YYYY'), "
            f"TO_DATE(REPLACE({expr}, '-', '') DEFAULT NULL ON CONVERSION ERROR, "
            f"'DDMONYYYY', 'NLS_DATE_LANGUAGE=English'), "
            f"TO_DATE({expr} DEFAULT NULL ON CONVERSION ERROR, 'YYYY-MM-DD'), "
            f"CAST(TO_TIMESTAMP({expr} DEFAULT NULL ON CONVERSION ERROR, "
            f"'YYYY-MM-DD HH24:MI:SS.FF') AS DATE), "
            f"CAST(TO_TIMESTAMP({expr} DEFAULT NULL ON CONVERSION ERROR, "
            f"'YYYY-MM-DD HH24:MI:SS') AS DATE), "
            f"CAST(TO_TIMESTAMP({expr} DEFAULT NULL ON CONVERSION ERROR, "
            f"'YYYY-MM-DD\"T\"HH24:MI:SS.FF') AS DATE), "
            f"CAST(TO_TIMESTAMP({expr} DEFAULT NULL ON CONVERSION ERROR, "
            f"'YYYY-MM-DD\"T\"HH24:MI:SS') AS DATE))"
        )
        return f"COALESCE(TO_CHAR({parsed}, 'YYYY-MM-DD'), '')"
    raise SamplingSqlNotSupported(f"sampling SQL does not support dialect {dialect!r}")


# >>> one built-in step rendered for a non-Spark dialect; Spark uses the registered step lambdas <<< #
def step_sql(expr: str, name: str, arg: str | None, dialect: str) -> str:
    dialect = dialect_name(dialect)
    if name == "trim":
        return _regex_replace(expr, r"^\s+|\s+$", "", dialect)
    if name == "lower":
        return f"lower({expr})"
    if name == "upper":
        return f"upper({expr})"
    if name == "collapse_decimal":
        replacement = r"\1" if dialect in {"duckdb", "oracle"} else "$1"
        collapsed = _regex_replace(expr, r"(\.[0-9]*[1-9])0+$", replacement, dialect)
        collapsed = _regex_replace(collapsed, r"\.0+$", "", dialect)
        decimal_pattern = r"^-?[0-9]+\.[0-9]+$"
        return (
            f"CASE WHEN {_regex_matches(expr, decimal_pattern, dialect)} "
            f"THEN {collapsed} ELSE {expr} END"
        )
    if name == "strip_leading_zeros":
        replacement = r"\1" if dialect in {"duckdb", "oracle"} else "$1"
        text = _regex_replace(expr, r"^\+", "", dialect)
        text = _regex_replace(text, r"^-0+([0-9])", "-" + replacement, dialect)
        return _regex_replace(text, r"^0+([0-9])", replacement, dialect)
    if name == "digits":
        return _regex_replace(expr, "[^0-9]", "", dialect)
    if name == "alnum":
        return _regex_replace(expr, "[^0-9A-Za-z]", "", dialect)
    if name == "date":
        return dialect_date_sql(expr, dialect)
    if name == "lpad":
        size = int(arg or "0")
        return f"CASE WHEN length({expr}) >= {size} THEN {expr} ELSE lpad({expr}, {size}, '0') END"
    if name == "strip_prefix":
        prefix = sql_literal(arg or "", dialect)
        size = len(arg or "")
        return (
            f"CASE WHEN substr({expr}, 1, {size}) = {prefix} "
            f"THEN substr({expr}, {size + 1}) ELSE {expr} END"
        )
    if name == "strip_suffix":
        suffix = sql_literal(arg or "", dialect)
        size = len(arg or "")
        return (
            f"CASE WHEN length({expr}) >= {size} AND substr({expr}, -{size}) = {suffix} "
            f"THEN substr({expr}, 1, length({expr}) - {size}) ELSE {expr} END"
        )
    raise SamplingSqlNotSupported(f"sampling SQL does not support key step {name!r} for {dialect}")


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


@dataclass(frozen=True)
class Step:
    name: str
    python: Callable[..., str]
    spark: Callable[..., str]
    takes_arg: bool = False


STEPS: dict[str, Step] = {}


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

    # >>> parsed steps for a non-Spark dialect; subclasses with Python/Spark overrides cannot be rendered <<< #
    def _portable_steps(self, side: str, dialect: str):
        methods = ("transform", "transform_sql", f"{side}_transform", f"{side}_transform_sql")
        overridden = [
            method
            for method in methods
            if getattr(type(self), method) is not getattr(KeyFamily, method)
        ]
        if overridden:
            raise SamplingSqlNotSupported(
                f"custom key family {self.name!r} needs {dialect} SQL for {', '.join(overridden)}"
            )
        steps = self.left_steps if side == "left" else self.right_steps
        return self._parsed if steps is None else self._left if side == "left" else self._right

    # >>> value normalization for statistics: NULL stays NULL, no sentinel <<< #
    def normalize_sql(self, expr: str, dialect: str, side: str = "left") -> str:
        dialect = dialect_name(dialect)
        text = cast_text_sql(expr, dialect)
        if dialect == "spark":
            return (
                self.left_transform_sql(text) if side == "left" else self.right_transform_sql(text)
            )
        for step, arg in self._portable_steps(side, dialect):
            text = step_sql(text, step.name, arg, dialect)
        return text

    # >>> key normalization for sampling: empty and NULL both become the NULL sentinel <<< #
    def sql_dialect(
        self, expr: str, dialect: str, side: str = "right", data_type: str | None = None
    ) -> str:
        dialect = dialect_name(dialect)
        if dialect == "spark":
            return self.sql(expr, side)
        text = cast_text_sql(expr, dialect, data_type)
        for step, arg in self._portable_steps(side, dialect):
            text = step_sql(text, step.name, arg, dialect)
        return f"CASE WHEN ({text}) IS NULL OR ({text}) = '' THEN '{NULL}' ELSE ({text}) END"

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


# >>> date normalizer with its own date type per side; subclasses set the class defaults <<< #
class DateKeyFamily(KeyFamily):
    name = "date_type"
    date_type: ClassVar[Any] = None
    left_date_type: ClassVar[Any] = None
    right_date_type: ClassVar[Any] = None

    def __init__(self, date_type=None, left_date_type=None, right_date_type=None):
        defaults = type(self)
        base = resolve_date_type(date_type or defaults.date_type or DateType("date_key"))
        self.left_date_type = resolve_date_type(left_date_type or defaults.left_date_type or base)
        self.right_date_type = resolve_date_type(
            right_date_type or defaults.right_date_type or base
        )
        self.steps = ()
        self.left_steps = self.right_steps = None
        self._parsed = self._left = self._right = ()

    def _kind(self, side: str) -> DateType:
        if side not in {"left", "right"}:
            raise ValueError("date normalization side must be left or right")
        return self.left_date_type if side == "left" else self.right_date_type

    def normalize(self, value: object, side: str = "left") -> str:
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return NULL
        try:
            return self._kind(side).normalize(value)
        except (TypeError, ValueError, OverflowError):
            return NULL

    def normalize_sql(self, expr: str, dialect: str, side: str = "left") -> str:
        return self._kind(side).normalize_sql(expr, dialect_name(dialect))

    def sql_dialect(
        self, expr: str, dialect: str, side: str = "right", data_type: str | None = None
    ) -> str:
        body = self.normalize_sql(expr, dialect, side)
        return f"coalesce(nullif({body}, ''), '{NULL}')"

    def sql(self, expr: str, side: str = "right") -> str:
        return self.sql_dialect(expr, "spark", side)

    def sides(self) -> tuple[Any, Any]:
        return (
            self.left_date_type.spec(portable=True),
            self.right_date_type.spec(portable=True),
        )

    def spec(self) -> str | dict[str, Any]:
        registered = FAMILIES.get(self.name)
        if registered is not None and type(registered) is type(self) and self == registered:
            return self.name
        left, right = self.sides()
        return {"type": "date", "left_date_type": left, "right_date_type": right}

    def __eq__(self, other: object) -> bool:
        return isinstance(other, DateKeyFamily) and self.sides() == other.sides()

    def __hash__(self) -> int:
        return hash((type(self), repr(self.sides())))

    def __repr__(self) -> str:
        return f"DateKeyFamily({self.spec()!r})"


FAMILIES: dict[str, KeyFamily] = {}
DATE_SPEC_FIELDS = {"date_type", "left_date_type", "right_date_type"}


# >>> registers a normalizer: a step list under a name, or a KeyFamily subclass via decorator <<< #
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


@register_family("sas_datetime_date")
class SasDatetimeDate(DateKeyFamily):
    left_date_type: ClassVar[Any] = {"type": "sas", "format": "datetime"}
    right_date_type: ClassVar[Any] = "date"


# >>> resolves a normalizer spec: a registered name, a step list, or an object keyed by family/type <<< #
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
        if "family" in spec and "type" in spec:
            raise ValueError("key family object takes family or type, not both")
        kind = spec.get("family", spec.get("type"))
        base = FAMILIES.get(kind) if isinstance(kind, str) else None
        if kind in {"date_type", "date"} or set(spec) & DATE_SPEC_FIELDS:
            unknown = set(spec) - {"family", "type", *DATE_SPEC_FIELDS}
            if unknown:
                raise ValueError(
                    f"unsupported date key family fields: {', '.join(sorted(unknown))}"
                )
            if kind not in {None, "date_type", "date"} and not isinstance(base, DateKeyFamily):
                raise ValueError(f"key family {kind!r} does not take date types")
            defaults = base if isinstance(base, DateKeyFamily) else None
            return DateKeyFamily(
                spec.get("date_type"),
                spec.get("left_date_type") or (defaults and defaults.left_date_type),
                spec.get("right_date_type") or (defaults and defaults.right_date_type),
            )
        unknown = set(spec) - {"family", "type", "transform", "left_transform", "right_transform"}
        if unknown:
            raise ValueError(f"unsupported key family fields: {', '.join(sorted(unknown))}")
        base = key_family(kind or "string")
        if isinstance(base, DateKeyFamily):
            if set(spec) - {"family", "type"}:
                raise ValueError(f"key family {kind!r} does not take transform steps")
            return base
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
