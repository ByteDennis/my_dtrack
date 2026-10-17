from __future__ import annotations

import math
import re
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

from .date_platforms import date_platform


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
    try:
        return datetime.fromisoformat(text).date().isoformat()
    except ValueError:
        raise ValueError(f"unsupported date value: {value!r}") from None


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


def quote_ident(name: str, dialect: str) -> str:
    if dialect in {"spark", "hadoop"}:
        return ".".join("`" + part.replace("`", "``") + "`" for part in name.split("."))
    if dialect == "oracle":
        return name
    if dialect in {"athena", "databricks"}:
        return ".".join(f'"{part.replace(chr(34), chr(34) * 2)}"' for part in name.split("."))
    return f'"{name.replace(chr(34), chr(34) * 2)}"'


SqlTemplate = str | Mapping[str, str] | None
DATE_TYPES: dict[str, DateType] = {}
BUILTIN_DATE_TYPES: set[str] = set()


def _template(value: SqlTemplate, field: str) -> str | dict[str, str] | None:
    if value is None or isinstance(value, str):
        return value
    if not isinstance(value, Mapping) or not all(
        isinstance(key, str) and isinstance(item, str) for key, item in value.items()
    ):
        raise TypeError(f"date type {field} must be SQL text or a dialect-to-SQL object")
    return {key.lower(): item for key, item in value.items()}


def _pick_template(value: SqlTemplate, dialect: str, field: str) -> str | None:
    if value is None or isinstance(value, str):
        return value
    template = value.get(dialect.lower()) or value.get("*")
    if template is None:
        raise ValueError(f"date type {field} does not support dialect {dialect!r}")
    return template


def _render(template: str, **values: object) -> str:
    try:
        return template.format_map(values)
    except KeyError as exc:
        raise ValueError(f"unknown date type SQL placeholder {exc.args[0]!r}") from None


def _probe_values(values: list[object]) -> list[object]:
    found = [
        value
        for value in values
        if value is not None
        and not (isinstance(value, float) and math.isnan(value))
        and str(value).strip()
    ]
    if not found:
        raise ValueError("date_type probe found no non-null values; configure an explicit format")
    return found


def _string_format(value: object) -> str:
    text = str(value).strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return "dash"
    if re.fullmatch(r"\d{8}", text):
        return "compact"
    raise ValueError(f"unsupported string date value: {value!r}")


def _sas_format(value: object) -> str:
    text = str(value).strip()
    if re.fullmatch(r"\d{1,2}[A-Za-z]{3}\d{4}:\d{2}:\d{2}:\d{2}(?:\.\d+)?", text):
        return "datetime"
    if re.fullmatch(r"\d{1,2}-?[A-Za-z]{3}-?\d{4}", text):
        return "date9"
    if re.fullmatch(r"[+-]?\d+(?:\.\d+)?", text):
        return "days"
    raise ValueError(f"unsupported SAS date value: {value!r}")


@dataclass(frozen=True)
class DateType:
    """Serializable SQL rules for one physical date representation."""

    name: str = "custom"
    predicate_sql: SqlTemplate = None
    day_sql: SqlTemplate = None
    literal_sql: SqlTemplate = "'{iso}'"
    range: str = "value"
    python_formats: tuple[str, ...] = ()
    normalizer_sql: SqlTemplate = None

    def __post_init__(self):
        object.__setattr__(self, "name", self.name.lower())
        object.__setattr__(self, "predicate_sql", _template(self.predicate_sql, "predicate_sql"))
        object.__setattr__(self, "day_sql", _template(self.day_sql, "day_sql"))
        object.__setattr__(self, "literal_sql", _template(self.literal_sql, "literal_sql"))
        object.__setattr__(self, "python_formats", tuple(self.python_formats))
        object.__setattr__(self, "normalizer_sql", _template(self.normalizer_sql, "normalizer_sql"))
        if self.range not in {"value", "day"}:
            raise ValueError("date type range must be 'value' or 'day'")

    @property
    def requires_probe(self) -> bool:
        return False

    def detect(self, values: list[object]) -> DateType:
        return self

    def renamed(self, name: str) -> DateType:
        return DateType(
            name,
            self.predicate_sql,
            self.day_sql,
            self.literal_sql,
            self.range,
            self.python_formats,
            self.normalizer_sql,
        )

    def predicate(self, column: str, dialect: str) -> str:
        template = _pick_template(self.predicate_sql, dialect, "predicate_sql")
        return column if template is None else _render(template, column=column)

    def day(self, column: str, dialect: str) -> str:
        template = _pick_template(self.day_sql, dialect, "day_sql")
        return (
            self.predicate(column, dialect)
            if template is None
            else _render(template, column=column)
        )

    def literal(self, value: str, dialect: str) -> str:
        day = canonical_date(value)
        sas_days = (date.fromisoformat(day) - date(1960, 1, 1)).days
        template = _pick_template(self.literal_sql, dialect, "literal_sql")
        if template is None:
            template = "'{iso}'"
        return _render(
            template,
            iso=day,
            value=day,
            compact=day.replace("-", ""),
            sas_days=sas_days,
        )

    def normalize(self, value: object) -> str:
        text = str(value).strip()
        for fmt in self.python_formats:
            try:
                return datetime.strptime(text, fmt).date().isoformat()  # noqa: DTZ007
            except ValueError:
                continue
        return canonical_date(value)

    def normalize_sql(self, column: str, dialect: str) -> str:
        template = _pick_template(self.normalizer_sql, dialect, "normalizer_sql")
        if template is not None:
            return _render(template, column=column)
        platform = date_platform(dialect)
        return platform.format_day(platform.generic_day(column))

    @property
    def uses_day_range(self) -> bool:
        return self.range == "day"

    def spec(self, *, portable: bool = False) -> str | dict[str, Any]:
        registered = DATE_TYPES.get(self.name)
        if registered == self and (not portable or self.name in BUILTIN_DATE_TYPES):
            return self.name
        result: dict[str, Any] = {"range": self.range}
        if self.predicate_sql is not None:
            result["predicate_sql"] = self.predicate_sql
        if self.day_sql is not None:
            result["day_sql"] = self.day_sql
        if self.literal_sql is not None:
            result["literal_sql"] = self.literal_sql
        if self.python_formats:
            result["python_formats"] = list(self.python_formats)
        if self.normalizer_sql is not None:
            result["normalizer_sql"] = self.normalizer_sql
        return result


class NativeDateType(DateType):
    def __init__(self, name: str = "date"):
        super().__init__(name, literal_sql="DATE '{iso}'", range="day")

    def day(self, column: str, dialect: str) -> str:
        return date_platform(dialect).native_day(column)

    def normalize_sql(self, column: str, dialect: str) -> str:
        platform = date_platform(dialect)
        return platform.format_day(platform.native_day(column))

    def renamed(self, name: str) -> DateType:
        return NativeDateType(name)


class StringDateType(DateType):
    FORMATS = frozenset({"auto", "dash", "compact"})

    def __init__(self, name: str = "string", format: str = "auto"):
        if format not in self.FORMATS:
            raise ValueError("string date format must be auto, dash or compact")
        object.__setattr__(self, "format", format)
        super().__init__(name)

    @property
    def requires_probe(self) -> bool:
        return self.format == "auto"

    def detect(self, values: list[object]) -> DateType:
        if not self.requires_probe:
            return self
        found = {_string_format(value) for value in _probe_values(values)}
        if len(found) != 1:
            raise ValueError(f"mixed string date formats: {', '.join(sorted(found))}")
        return StringDateType(self.name, found.pop())

    def _concrete(self) -> str:
        if self.requires_probe:
            raise ValueError("string date_type requires a non-null value probe before querying")
        return self.format

    def literal(self, value: str, dialect: str) -> str:
        del dialect
        day = canonical_date(value)
        return f"'{day if self._concrete() == 'dash' else day.replace('-', '')}'"

    def normalize(self, value: object) -> str:
        format = self._concrete()
        pattern = "%Y-%m-%d" if format == "dash" else "%Y%m%d"
        return datetime.strptime(str(value).strip(), pattern).date().isoformat()  # noqa: DTZ007

    def normalize_sql(self, column: str, dialect: str) -> str:
        platform = date_platform(dialect)
        return platform.format_day(platform.parse_text(column, self._concrete()))

    def spec(self, *, portable: bool = False) -> str | dict[str, Any]:
        if DATE_TYPES.get(self.name) is self and (not portable or self.name in BUILTIN_DATE_TYPES):
            return self.name
        return {"type": "string", "format": self._concrete()}

    def renamed(self, name: str) -> DateType:
        return StringDateType(name, self.format)


class NumericDateType(DateType):
    def __init__(self, name: str = "number"):
        super().__init__(name, literal_sql="{compact}")

    def normalize_sql(self, column: str, dialect: str) -> str:
        platform = date_platform(dialect)
        return platform.format_day(platform.parse_text(column, "compact"))

    def renamed(self, name: str) -> DateType:
        return NumericDateType(name)


class SasDateType(DateType):
    """SAS family resolved by a value probe to one concrete physical format."""

    FORMATS = frozenset({"auto", "date9", "datetime", "days"})
    ORIGIN = date(1960, 1, 1)

    def __init__(self, name: str = "sas", format: str = "auto"):
        if format not in self.FORMATS:
            raise ValueError("SAS date format must be auto, date9, datetime or days")
        object.__setattr__(self, "format", format)
        super().__init__(name, range="day")

    @property
    def requires_probe(self) -> bool:
        return self.format == "auto"

    def detect(self, values: list[object]) -> DateType:
        if not self.requires_probe:
            return self
        found = {_sas_format(value) for value in _probe_values(values)}
        if len(found) != 1:
            raise ValueError(f"mixed SAS date formats: {', '.join(sorted(found))}")
        return SasDateType(self.name, found.pop())

    def _concrete(self) -> str:
        if self.requires_probe:
            raise ValueError("sas date_type requires a non-null value probe before querying")
        return self.format

    def predicate(self, column: str, dialect: str) -> str:
        format = self._concrete()
        platform = date_platform(dialect)
        if format == "days":
            return platform.epoch_value(column, "days")
        return platform.parse_text(column, format)

    def day(self, column: str, dialect: str) -> str:
        format = self._concrete()
        platform = date_platform(dialect)
        if format == "days":
            return platform.epoch_day(column, self.ORIGIN.isoformat(), "days")
        return platform.parse_text(column, format)

    def literal(self, value: str, dialect: str) -> str:
        format = self._concrete()
        day = date.fromisoformat(canonical_date(value))
        if format == "days":
            return str((day - self.ORIGIN).days)
        return f"DATE '{day.isoformat()}'"

    def normalize(self, value: object) -> str:
        format = self._concrete()
        text = str(value).strip()
        if format == "days":
            return (self.ORIGIN + timedelta(days=int(float(text)))).isoformat()
        formats = (
            ("%d%b%Y", "%d-%b-%Y")
            if format == "date9"
            else ("%d%b%Y:%H:%M:%S", "%d%b%Y:%H:%M:%S.%f")
        )
        for pattern in formats:
            try:
                return datetime.strptime(text, pattern).date().isoformat()  # noqa: DTZ007
            except ValueError:
                continue
        raise ValueError(f"unsupported SAS {format} value: {value!r}")

    def normalize_sql(self, column: str, dialect: str) -> str:
        platform = date_platform(dialect)
        return platform.format_day(self.day(column, dialect))

    def spec(self, *, portable: bool = False) -> str | dict[str, Any]:
        if DATE_TYPES.get(self.name) is self and (not portable or self.name in BUILTIN_DATE_TYPES):
            return self.name
        return {"type": "sas", "format": self._concrete()}

    def renamed(self, name: str) -> DateType:
        return SasDateType(name, self.format)


# >>> name a reusable physical date representation; custom names serialize inline for remote jobs <<< #
def register_date_type(
    name: str | DateType,
    spec: DateType | Mapping[str, Any] | None = None,
    *,
    predicate_sql: SqlTemplate = None,
    day_sql: SqlTemplate = None,
    literal_sql: SqlTemplate = "'{iso}'",
    range: str = "value",
    python_formats: tuple[str, ...] | list[str] = (),
    normalizer_sql: SqlTemplate = None,
) -> DateType:
    if isinstance(name, DateType):
        if spec is not None or any(
            value is not None for value in (predicate_sql, day_sql, normalizer_sql)
        ):
            raise TypeError("a DateType instance must be registered without SQL overrides")
        if literal_sql != "'{iso}'" or range != "value" or python_formats:
            raise TypeError("a DateType instance must be registered without SQL overrides")
        resolved = name
        DATE_TYPES[resolved.name] = resolved
        return resolved
    if spec is not None and any(
        value is not None for value in (predicate_sql, day_sql, normalizer_sql)
    ):
        raise TypeError("register_date_type accepts either spec or SQL keyword arguments")
    if spec is not None and (literal_sql != "'{iso}'" or range != "value" or python_formats):
        raise TypeError("register_date_type accepts either spec or SQL keyword arguments")
    if isinstance(spec, DateType):
        resolved = spec.renamed(name)
    elif isinstance(spec, Mapping):
        resolved = resolve_date_type(spec).renamed(name)
    elif spec is None:
        resolved = DateType(
            name,
            predicate_sql,
            day_sql,
            literal_sql,
            range,
            tuple(python_formats),
            normalizer_sql,
        )
    else:
        raise TypeError("date type registration must be a DateType or an object")
    DATE_TYPES[resolved.name] = resolved
    return resolved


def _register_builtin(kind: DateType) -> None:
    DATE_TYPES[kind.name] = kind
    BUILTIN_DATE_TYPES.add(kind.name)


_register_builtin(NativeDateType())
_register_builtin(StringDateType())
_register_builtin(NumericDateType())
_register_builtin(SasDateType())


# >>> config value -> DateType: built-in/registered name or a portable SQL-rule object <<< #
def resolve_date_type(spec: Any = None) -> DateType:
    if isinstance(spec, DateType):
        return spec
    if spec is None or isinstance(spec, str):
        name = str(spec or "string").lower()
        try:
            return DATE_TYPES[name]
        except KeyError:
            supported = ", ".join(sorted(DATE_TYPES))
            raise ValueError(f"unknown date type {name!r}; supported types: {supported}") from None
    if isinstance(spec, Mapping):
        unknown = set(spec) - {
            "type",
            "format",
            "predicate_sql",
            "day_sql",
            "literal_sql",
            "range",
            "python_formats",
            "normalizer_sql",
        }
        if unknown:
            raise ValueError(f"unsupported date type fields: {', '.join(sorted(unknown))}")
        base = resolve_date_type(spec.get("type") or "string")
        format = spec.get("format")
        if format is not None:
            if isinstance(base, StringDateType):
                base = StringDateType(base.name, str(format).lower())
            elif isinstance(base, SasDateType):
                base = SasDateType(base.name, str(format).lower())
            else:
                raise ValueError(f"date type {base.name!r} does not accept a format")
        if set(spec) <= {"type", "format"}:
            return base
        return DateType(
            "custom",
            spec.get("predicate_sql", base.predicate_sql),
            spec.get("day_sql", base.day_sql),
            spec.get("literal_sql", base.literal_sql),
            spec.get("range", base.range),
            tuple(spec.get("python_formats", base.python_formats)),
            spec.get("normalizer_sql", base.normalizer_sql),
        )
    raise TypeError(f"date type must be a name or an object, got {spec!r}")


def _predicate_expr(date_col: str, dialect: str, date_type: Any) -> str:
    column = quote_ident(date_col, dialect)
    return resolve_date_type(date_type).predicate(column, dialect)


# >>> the partition key of a row: the column itself, or its calendar day when it can carry a time <<< #
def day_expr(date_col: str, dialect: str, date_type: Any) -> str:
    column = quote_ident(date_col, dialect)
    return resolve_date_type(date_type).day(column, dialect)


# >>> the ISO day after `value` <<< #
def next_day(value: str) -> str:
    return (date.fromisoformat(value) + timedelta(days=1)).isoformat()


def date_literal(value: str, dialect: str, date_type: Any) -> str:
    return resolve_date_type(date_type).literal(value, dialect)


def partition_where(
    date_col: str,
    partitions: list[str],
    *,
    dialect: str,
    date_type: Any = None,
) -> str:
    resolved = resolve_date_type(date_type)
    column = _predicate_expr(date_col, dialect, date_type)
    clauses = []
    for start, end in contiguous_runs(partitions):
        lo = date_literal(start, dialect, date_type)
        if resolved.uses_day_range:
            after = date_literal(next_day(end), dialect, date_type)
            clauses.append(f"{column} >= {lo} AND {column} < {after}")
            continue
        hi = date_literal(end, dialect, date_type)
        clauses.append(f"{column} = {lo}" if start == end else f"{column} BETWEEN {lo} AND {hi}")
    if not clauses:
        return "1=0"
    return "(" + " OR ".join(f"({item})" for item in clauses) + ")"


def bounds_where(
    date_col: str,
    *,
    dialect: str,
    date_type: Any,
    from_date: str | None,
    to_date: str | None,
) -> str | None:
    resolved = resolve_date_type(date_type)
    column = _predicate_expr(date_col, dialect, date_type)
    clauses = []
    if from_date:
        clauses.append(f"{column} >= {date_literal(from_date, dialect, date_type)}")
    if to_date and resolved.uses_day_range:
        clauses.append(f"{column} < {date_literal(next_day(to_date), dialect, date_type)}")
    elif to_date:
        clauses.append(f"{column} <= {date_literal(to_date, dialect, date_type)}")
    return " AND ".join(clauses) or None
