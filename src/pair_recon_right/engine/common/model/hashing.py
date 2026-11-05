from __future__ import annotations

import hashlib
import math
import re
from collections.abc import Iterable, Mapping
from decimal import Decimal, InvalidOperation
from typing import Any, ClassVar

from ..sql.dates import DateType, NativeDateType, canonical_date, resolve_date_type

NULL = "<<NULL>>"
PURE_DECIMAL = re.compile(r"^-?[0-9]+\.[0-9]+$")
ISO_DATETIME = re.compile(r"^([0-9]{4}-[0-9]{2}-[0-9]{2})[ Tt].*$")
NORMALIZATION_TRANSFORMS = ("trim", "lowercase", "decimal", "strip_datetime")
NORMALIZATION_SOURCES = ("all", "oracle", "hadoop", "csv", "databricks")
MISSING_VALUE = "@missing"
RUN_DATE_VALUE = "@run_date"


def collapse_decimal(text: str) -> str:
    if not PURE_DECIMAL.match(text):
        return text
    text = re.sub(r"(\.[0-9]*[1-9])0+$", r"\1", text)
    return re.sub(r"\.0+$", "", text)


def collapse_decimal_sql(text: str, dialect: str = "spark") -> str:
    replacement = r"\1" if dialect_name(dialect) in {"duckdb", "oracle"} else "$1"
    collapsed = _regex_replace(text, r"(\.[0-9]*[1-9])0+$", replacement, dialect)
    collapsed = _regex_replace(collapsed, r"\.0+$", "", dialect)
    matches = _regex_matches(text, r"^-?[0-9]+\.[0-9]+$", dialect)
    return f"CASE WHEN {matches} THEN {collapsed} ELSE {text} END"


# >>> canonical numeric text for hashes and row comparison; invalid text stays visible instead of becoming NULL <<< #
def canonical_numeric(value: object) -> str:
    text = str(value).strip()
    try:
        number = Decimal(text)
    except InvalidOperation:
        return text
    if not number.is_finite():
        return text
    if not number:
        return "0"
    rendered = format(number, "f")
    return rendered.rstrip("0").rstrip(".") if "." in rendered else rendered


def canonical_numeric_sql(expr: str, dialect: str) -> str:
    dialect = dialect_name(dialect)
    text = f"trim({expr})"
    mantissa = r"(?:[0-9]{1,20}(?:[.][0-9]{0,18})?|[.][0-9]{1,18})"
    # A negative exponent adds fractional places. Reject values that would
    # round at scale 18, while allowing extra trailing fractional zeros.
    negative_exponents = []
    for exponent in range(1, 19):
        scale = 18 - exponent
        fractional = rf"(?:[.][0-9]{{0,{scale}}}0*)?"
        leading_dot = rf"|[.][0-9]{{1,{scale}}}0*" if scale else r"|[.]0+"
        negative_exponents.append(
            rf"(?:[0-9]{{1,20}}{fractional}{leading_dot})[eE]-0*{exponent}"
        )
    scientific_negative = "|".join(negative_exponents)
    safe = _regex_matches(
        text,
        rf"^[+-]?(?:{mantissa}(?:[eE](?:[+-]?0+|[+]?0*(?:[1-9]|1[0-8])))?|{scientific_negative})$",
        dialect,
    )
    if dialect in {"spark", "duckdb", "athena"}:
        cast = "VARCHAR" if dialect in {"duckdb", "athena"} else "STRING"
        parsed = f"try_cast({text} AS DECIMAL(38,18))"
    elif dialect == "hadoop":
        cast = "STRING"
        parsed = f"CAST({text} AS DECIMAL(38,18))"
    else:
        raise SamplingSqlNotSupported(f"canonical numeric SQL does not support dialect {dialect!r}")
    rendered = collapse_decimal_sql(f"CAST({parsed} AS {cast})", dialect)
    return (
        f"CASE WHEN {expr} IS NULL THEN NULL WHEN NOT ({safe}) OR {parsed} IS NULL THEN {text} "
        f"ELSE {rendered} END"
    )


def strip_datetime(text: str) -> str:
    match = ISO_DATETIME.match(text.strip())
    return match.group(1) if match else text


def strip_datetime_sql(text: str, dialect: str = "spark") -> str:
    pattern = r"^([0-9]{4}-[0-9]{2}-[0-9]{2})[ Tt].*$"
    replacement = r"\1" if dialect_name(dialect) in {"duckdb", "oracle"} else "$1"
    stripped = _regex_replace(f"trim({text})", pattern, replacement, dialect)
    matches = _regex_matches(f"trim({text})", pattern, dialect)
    return f"CASE WHEN {matches} THEN {stripped} ELSE {text} END"


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


def default_normalization_rules() -> list[dict[str, str]]:
    return [{"rule_name": name, "source": "all"} for name in NORMALIZATION_TRANSFORMS]


def is_transform_rule(rule: Mapping[str, str]) -> bool:
    return "match_value" not in rule


def canonical_normalization_rules(raw: object) -> list[dict[str, str]]:
    if raw is None:
        return default_normalization_rules()
    if not isinstance(raw, list):
        raise TypeError("normalization_rules must be a list of objects")
    result = []
    identities = set()
    for item in raw:
        if not isinstance(item, Mapping):
            raise TypeError("normalization_rules must be a list of objects")
        name = str(item.get("rule_name") or "").strip().casefold()
        source = str(item.get("source") or "").strip().casefold()
        if not name:
            raise ValueError("normalization rule_name must not be empty")
        if source not in NORMALIZATION_SOURCES:
            raise ValueError(
                f"normalization rule {name!r} source must be one of "
                f"{', '.join(NORMALIZATION_SOURCES)}"
            )
        normalized = {"rule_name": name, "source": source}
        match_value = str(item.get("match_value") or "")
        if not match_value:
            if name not in NORMALIZATION_TRANSFORMS:
                raise ValueError(f"unsupported normalization transform: {name}")
            if item.get("canonical_value") not in (None, ""):
                raise ValueError(f"normalization transform {name!r} cannot define canonical_value")
        else:
            canonical_value = str(item.get("canonical_value") or "")
            if not canonical_value:
                raise ValueError(f"normalization synonym {name!r} needs canonical_value")
            normalized.update(
                match_value=match_value,
                canonical_value=canonical_value,
            )
        identity = (source, name)
        if identity in identities:
            raise ValueError(f"normalization rule {source}.{name} must not repeat")
        identities.add(identity)
        result.append(normalized)
    return result


def active_normalization_rules(
    rules: object,
    source: str,
) -> tuple[dict[str, str], ...]:
    source = str(source or "").strip().casefold()
    if source not in NORMALIZATION_SOURCES[1:]:
        raise ValueError(f"unsupported normalization source: {source or '<empty>'}")
    return tuple(
        rule for rule in canonical_normalization_rules(rules) if rule["source"] in {"all", source}
    )


def normalize_system_value(
    value: object,
    rules: Iterable[Mapping[str, str]],
    *,
    run_date: str | None = None,
) -> str | None:
    text = str(value)
    for rule in rules:
        if is_transform_rule(rule):
            if rule["rule_name"] == "trim":
                text = text.strip()
            elif rule["rule_name"] == "lowercase":
                text = text.lower()
            elif rule["rule_name"] == "decimal":
                text = collapse_decimal(text)
            elif rule["rule_name"] == "strip_datetime":
                text = strip_datetime(text)
        elif text == rule["match_value"]:
            replacement = rule["canonical_value"]
            if replacement == MISSING_VALUE:
                return None
            if replacement == RUN_DATE_VALUE:
                if not run_date:
                    raise ValueError("@run_date normalization needs normalization_date")
                replacement = run_date
            text = replacement
    return text


def normalize_system_sql(
    expr: str,
    dialect: str,
    rules: Iterable[Mapping[str, str]],
    *,
    run_date: str | None = None,
) -> str:
    value = expr
    for rule in rules:
        if is_transform_rule(rule):
            if rule["rule_name"] == "trim":
                value = f"trim({value})"
            elif rule["rule_name"] == "lowercase":
                value = f"lower({value})"
            elif rule["rule_name"] == "decimal":
                value = collapse_decimal_sql(value, dialect)
            elif rule["rule_name"] == "strip_datetime":
                value = strip_datetime_sql(value, dialect)
            continue
        replacement = rule["canonical_value"]
        if replacement == MISSING_VALUE:
            target = "NULL"
        else:
            if replacement == RUN_DATE_VALUE:
                if not run_date:
                    raise ValueError("@run_date normalization needs normalization_date")
                replacement = run_date
            target = sql_literal(replacement, dialect)
        value = (
            f"CASE WHEN {value} = {sql_literal(rule['match_value'], dialect)} "
            f"THEN {target} ELSE {value} END"
        )
    return value


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
    if dialect in {"spark", "hadoop"}:
        kind = str(data_type or "").lower()
        if kind.startswith(("double", "float", "real")):
            cast = "try_cast" if dialect == "spark" else "CAST"
            return collapse_decimal_sql(
                f"CAST({cast}({expr} AS DECIMAL(38,15)) AS STRING)", dialect
            )
        if kind.startswith(("decimal", "numeric")):
            return collapse_decimal_sql(f"CAST({expr} AS STRING)", dialect)
        return f"CAST({expr} AS STRING)"
    return f"CAST({expr} AS VARCHAR)"


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


def _strip_zeros(text: str) -> str:
    return re.sub(r"^(-?)0+(?=[0-9])", r"\1", re.sub(r"^\+(?=[0-9.])", "", text))


def _strip_zeros_sql(text: str, dialect: str) -> str:
    replacement = r"\1" if dialect_name(dialect) in {"duckdb", "oracle"} else "$1"
    unsigned = _regex_replace(text, r"^\+", "", dialect)
    signed = _regex_replace(unsigned, r"^-0+([0-9])", "-" + replacement, dialect)
    return _regex_replace(signed, r"^0+([0-9])", replacement, dialect)


def _date(text: str) -> str:
    try:
        return canonical_date(text)
    except ValueError:
        return ""


def _month(text: str) -> str:
    if re.fullmatch(r"[0-9]{6}", text):
        return text if re.fullmatch(r"[0-9]{4}(0[1-9]|1[0-2])", text) else ""
    day = _date(text)
    return day[:7].replace("-", "") if day else ""


class KeyFamily:
    name: ClassVar[str] = "custom"

    def normalize_value(self, text: str) -> str:
        return text

    def normalize_left(self, text: str) -> str:
        return self.normalize_value(text)

    def normalize_right(self, text: str) -> str:
        return self.normalize_value(text)

    def normalize_value_sql(self, expr: str, dialect: str) -> str:
        if type(self).normalize_value is not KeyFamily.normalize_value:
            raise SamplingSqlNotSupported(
                f"custom key family {self.name!r} needs {dialect} SQL for normalize_value"
            )
        del dialect
        return expr

    def normalize_left_sql(self, expr: str, dialect: str) -> str:
        if type(self).normalize_left is not KeyFamily.normalize_left:
            raise SamplingSqlNotSupported(
                f"custom key family {self.name!r} needs {dialect} SQL for normalize_left"
            )
        return self.normalize_value_sql(expr, dialect)

    def normalize_right_sql(self, expr: str, dialect: str) -> str:
        if type(self).normalize_right is not KeyFamily.normalize_right:
            raise SamplingSqlNotSupported(
                f"custom key family {self.name!r} needs {dialect} SQL for normalize_right"
            )
        return self.normalize_value_sql(expr, dialect)

    @staticmethod
    def _side(side: str) -> str:
        if side not in {"left", "right"}:
            raise ValueError("normalization side must be left or right")
        return side

    def normalize(self, value: object, side: str = "left") -> str:
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return NULL
        method = self.normalize_left if self._side(side) == "left" else self.normalize_right
        text = method(str(value))
        return text or NULL

    def sql(self, expr: str, side: str = "right") -> str:
        return self.sql_dialect(expr, "spark", side)

    # >>> value normalization for statistics: NULL stays NULL, no sentinel <<< #
    def normalize_sql(self, expr: str, dialect: str, side: str = "left") -> str:
        dialect = dialect_name(dialect)
        text = cast_text_sql(expr, dialect)
        method = self.normalize_left_sql if self._side(side) == "left" else self.normalize_right_sql
        return method(text, dialect)

    # >>> key normalization for sampling: empty and NULL both become the NULL sentinel <<< #
    def sql_dialect(
        self, expr: str, dialect: str, side: str = "right", data_type: str | None = None
    ) -> str:
        dialect = dialect_name(dialect)
        text = cast_text_sql(expr, dialect, data_type)
        method = self.normalize_left_sql if self._side(side) == "left" else self.normalize_right_sql
        text = method(text, dialect)
        return f"CASE WHEN ({text}) IS NULL OR ({text}) = '' THEN '{NULL}' ELSE ({text}) END"

    def spec(self) -> str | dict[str, Any]:
        registered = FAMILIES.get(self.name)
        if registered is not None and type(registered) is type(self) and self == registered:
            return self.name
        raise ValueError(f"unregistered key family class {type(self).__name__!r}")

    def __eq__(self, other: object) -> bool:
        return type(other) is type(self)

    def __hash__(self) -> int:
        return hash(type(self))

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

    # >>> a physical DATE/TIMESTAMP key skips text parsing (Oracle TO_CHAR would follow session NLS) <<< #
    def sql_dialect(
        self, expr: str, dialect: str, side: str = "right", data_type: str | None = None
    ) -> str:
        kind = str(data_type or "").upper()
        if "DATE" in kind or "TIMESTAMP" in kind:
            body = NativeDateType().normalize_sql(expr, dialect_name(dialect))
        else:
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


# >>> registers an inheritable normalizer class under its own name <<< #
def register_family(cls: type[KeyFamily]) -> type[KeyFamily]:
    if not isinstance(cls, type) or not issubclass(cls, KeyFamily):
        raise TypeError("register_family requires a KeyFamily class")
    if not cls.name or cls.name == "custom":
        raise ValueError("registered KeyFamily class must declare a name")
    FAMILIES[cls.name] = cls()
    return cls


class TrimmedFamily(KeyFamily):
    def normalize_value(self, text: str) -> str:
        return re.sub(r"^\s+|\s+$", "", text, flags=re.ASCII)

    def normalize_value_sql(self, expr: str, dialect: str) -> str:
        return _regex_replace(expr, r"^\s+|\s+$", "", dialect)


@register_family
class StringFamily(TrimmedFamily):
    name = "string"

    def normalize_value(self, text: str) -> str:
        return super().normalize_value(text).lower()

    def normalize_value_sql(self, expr: str, dialect: str) -> str:
        return f"lower({super().normalize_value_sql(expr, dialect)})"


@register_family
class ExactFamily(TrimmedFamily):
    name = "exact"


@register_family
class NumericFamily(TrimmedFamily):
    name = "numeric"

    def normalize_value(self, text: str) -> str:
        return collapse_decimal(_strip_zeros(super().normalize_value(text)))

    def normalize_value_sql(self, expr: str, dialect: str) -> str:
        text = super().normalize_value_sql(expr, dialect)
        return collapse_decimal_sql(_strip_zeros_sql(text, dialect), dialect)


@register_family
class DateFamily(TrimmedFamily):
    name = "date"

    def normalize_value(self, text: str) -> str:
        return _date(super().normalize_value(text))

    def normalize_value_sql(self, expr: str, dialect: str) -> str:
        return dialect_date_sql(super().normalize_value_sql(expr, dialect), dialect)


@register_family
class MonthFamily(TrimmedFamily):
    name = "month"

    def normalize_value(self, text: str) -> str:
        return _month(super().normalize_value(text))

    def normalize_value_sql(self, expr: str, dialect: str) -> str:
        text = super().normalize_value_sql(expr, dialect)
        day = dialect_date_sql(text, dialect)
        compact = _regex_matches(text, r"^[0-9]{4}(0[1-9]|1[0-2])$", dialect)
        return f"CASE WHEN {compact} THEN {text} ELSE replace(substr({day}, 1, 7), '-', '') END"


@register_family
class DigitsFamily(KeyFamily):
    name = "digits"

    def normalize_value(self, text: str) -> str:
        return re.sub(r"[^0-9]", "", text)

    def normalize_value_sql(self, expr: str, dialect: str) -> str:
        return _regex_replace(expr, "[^0-9]", "", dialect)


@register_family
class TypedDateFamily(DateKeyFamily):
    name = "date_type"


@register_family
class SasDatetimeDate(DateKeyFamily):
    name = "sas_datetime_date"
    left_date_type: ClassVar[Any] = {"type": "sas", "format": "datetime"}
    right_date_type: ClassVar[Any] = "date"


@register_family
class DateString(DateKeyFamily):
    name = "date_string"
    date_type: ClassVar[Any] = DateType("date_string", python_formats=("%Y%m%d", "%Y-%m-%d"))


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
        raise TypeError("key family must be a registered name or a date type object")
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
        unknown = set(spec) - {"family", "type"}
        if unknown:
            raise ValueError(f"unsupported key family fields: {', '.join(sorted(unknown))}")
        base = key_family(kind or "string")
        return base
    raise TypeError(f"key family must be a registered name or a date type object, got {spec!r}")


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
