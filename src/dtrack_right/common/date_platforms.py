from __future__ import annotations


class DateSqlPlatform:
    """SQL primitives needed by DateType implementations."""

    name = "platform"

    def text(self, column: str) -> str:
        raise NotImplementedError

    def format_day(self, day: str) -> str:
        raise NotImplementedError

    def native_day(self, column: str) -> str:
        raise NotImplementedError

    def parse_text(self, column: str, format: str) -> str:
        raise NotImplementedError

    def generic_day(self, column: str) -> str:
        raise NotImplementedError

    def epoch_value(self, column: str, unit: str) -> str:
        raise NotImplementedError

    def epoch_day(self, column: str, origin: str, unit: str) -> str:
        raise NotImplementedError


class SparkDateSqlPlatform(DateSqlPlatform):
    name = "spark"

    def text(self, column: str) -> str:
        return f"TRIM(CAST({column} AS STRING))"

    def format_day(self, day: str) -> str:
        return f"coalesce(date_format({day}, 'yyyy-MM-dd'), '')"

    def native_day(self, column: str) -> str:
        return f"to_date({column})"

    def _parse(self, text: str, pattern: str) -> str:
        return f"CAST(try_to_timestamp({text}, '{pattern}') AS DATE)"

    def parse_text(self, column: str, format: str) -> str:
        pattern = {
            "dash": "yyyy-MM-dd",
            "compact": "yyyyMMdd",
            "date9": "dMMMyyyy",
            "datetime": "dMMMyyyy:HH:mm:ss",
        }[format]
        text = f"replace({self.text(column)}, '-', '')" if format == "date9" else self.text(column)
        return self._parse(text, pattern)

    def generic_day(self, column: str) -> str:
        text = self.text(column)
        undashed = f"replace({text}, '-', '')"
        return (
            f"CASE WHEN {text} rlike '^[0-9]{{8}}$' THEN {self._parse(text, 'yyyyMMdd')} "
            f"WHEN {text} rlike '^[0-9]{{1,2}}/[0-9]{{1,2}}/[0-9]{{4}}$' "
            f"THEN {self._parse(text, 'M/d/yyyy')} "
            f"WHEN {text} rlike '^[0-9]{{1,2}}-?[A-Za-z]{{3}}-?[0-9]{{4}}$' "
            f"THEN {self._parse(undashed, 'dMMMyyyy')} ELSE try_cast({text} as date) END"
        )

    def epoch_value(self, column: str, unit: str) -> str:
        value = f"CAST({column} AS DOUBLE)"
        return f"CAST({value} AS BIGINT)" if unit == "days" else f"CAST(floor({value}) AS BIGINT)"

    def epoch_day(self, column: str, origin: str, unit: str) -> str:
        value = f"CAST({column} AS DOUBLE)"
        offset = (
            f"CAST({value} AS INT)"
            if unit == "days"
            else f"CAST(floor({value} / 86400) AS INT)"
        )
        return f"date_add(DATE '{origin}', {offset})"


DATE_SQL_PLATFORMS: dict[str, DateSqlPlatform] = {}


def register_date_platform(platform: DateSqlPlatform, *, aliases: tuple[str, ...] = ()) -> None:
    for name in (platform.name, *aliases):
        DATE_SQL_PLATFORMS[name.lower()] = platform


def date_platform(dialect: str) -> DateSqlPlatform:
    try:
        return DATE_SQL_PLATFORMS[dialect.lower()]
    except KeyError:
        raise ValueError(f"date SQL does not support dialect {dialect!r}") from None


register_date_platform(SparkDateSqlPlatform(), aliases=("databricks",))
