from __future__ import annotations


class DateSqlPlatform:
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


class OracleDateSqlPlatform(DateSqlPlatform):
    name = "oracle"

    def text(self, column: str) -> str:
        return f"TRIM(TO_CHAR({column}))"

    def format_day(self, day: str) -> str:
        return f"COALESCE(TO_CHAR({day}, 'YYYY-MM-DD'), '')"

    def native_day(self, column: str) -> str:
        return f"TRUNC({column})"

    def parse_text(self, column: str, format: str) -> str:
        text = self.text(column)
        pattern = {
            "dash": "YYYY-MM-DD",
            "compact": "YYYYMMDD",
            "month": "YYYYMM",
            "date9": "DDMONYYYY",
            "datetime": "DDMONYYYY:HH24:MI:SS",
        }[format]
        language = ", 'NLS_DATE_LANGUAGE=English'" if format in {"date9", "datetime"} else ""
        if format == "datetime":
            parsed = f"TO_TIMESTAMP({text} DEFAULT NULL ON CONVERSION ERROR, '{pattern}'{language})"
            return f"TRUNC(CAST({parsed} AS DATE))"
        parsed_text = f"REPLACE({text}, '-', '')" if format == "date9" else text
        return f"TO_DATE({parsed_text} DEFAULT NULL ON CONVERSION ERROR, '{pattern}'{language})"

    def generic_day(self, column: str) -> str:
        text = self.text(column)
        return (
            f"COALESCE(TO_DATE({text} DEFAULT NULL ON CONVERSION ERROR, 'YYYYMMDD'), "
            f"TO_DATE({text} DEFAULT NULL ON CONVERSION ERROR, 'MM/DD/YYYY'), "
            f"TO_DATE(REPLACE({text}, '-', '') DEFAULT NULL ON CONVERSION ERROR, "
            f"'DDMONYYYY', 'NLS_DATE_LANGUAGE=English'), "
            f"TO_DATE({text} DEFAULT NULL ON CONVERSION ERROR, 'YYYY-MM-DD'), "
            f"CAST(TO_TIMESTAMP({text} DEFAULT NULL ON CONVERSION ERROR, "
            f"'YYYY-MM-DD HH24:MI:SS.FF') AS DATE), "
            f"CAST(TO_TIMESTAMP({text} DEFAULT NULL ON CONVERSION ERROR, "
            f"'YYYY-MM-DD HH24:MI:SS') AS DATE))"
        )

    def epoch_value(self, column: str, unit: str) -> str:
        value = f"TO_NUMBER({column})"
        return f"TRUNC({value})" if unit == "days" else f"FLOOR({value})"

    def epoch_day(self, column: str, origin: str, unit: str) -> str:
        value = f"TO_NUMBER({column})"
        offset = f"TRUNC({value})" if unit == "days" else f"FLOOR({value} / 86400)"
        return f"DATE '{origin}' + {offset}"


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
            "month": "yyyyMM",
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
            f"CAST({value} AS INT)" if unit == "days" else f"CAST(floor({value} / 86400) AS INT)"
        )
        return f"date_add(DATE '{origin}', {offset})"


class HadoopDateSqlPlatform(SparkDateSqlPlatform):
    name = "hadoop"

    def _parse(self, text: str, pattern: str) -> str:
        return f"to_date(from_unixtime(unix_timestamp({text}, '{pattern}')))"

    def generic_day(self, column: str) -> str:
        text = self.text(column)
        undashed = f"replace({text}, '-', '')"
        return (
            f"CASE WHEN {text} rlike '^[0-9]{{8}}$' THEN {self._parse(text, 'yyyyMMdd')} "
            f"WHEN {text} rlike '^[0-9]{{1,2}}/[0-9]{{1,2}}/[0-9]{{4}}$' "
            f"THEN {self._parse(text, 'M/d/yyyy')} "
            f"WHEN {text} rlike '^[0-9]{{1,2}}-?[A-Za-z]{{3}}-?[0-9]{{4}}$' "
            f"THEN {self._parse(undashed, 'dMMMyyyy')} ELSE CAST({text} AS DATE) END"
        )


class AthenaDateSqlPlatform(DateSqlPlatform):
    name = "athena"

    def text(self, column: str) -> str:
        return f"TRIM(CAST({column} AS VARCHAR))"

    def format_day(self, day: str) -> str:
        return f"coalesce(date_format({day}, '%Y-%m-%d'), '')"

    def native_day(self, column: str) -> str:
        return f"CAST({column} AS DATE)"

    def parse_text(self, column: str, format: str) -> str:
        pattern = {
            "dash": "%Y-%m-%d",
            "compact": "%Y%m%d",
            "month": "%Y%m",
            "date9": "%d%b%Y",
            "datetime": "%d%b%Y:%H:%i:%s",
        }[format]
        return f"CAST(date_parse({self.text(column)}, '{pattern}') AS DATE)"

    def generic_day(self, column: str) -> str:
        text = self.text(column)
        return (
            f"coalesce(try(date_parse({text}, '%Y%m%d')), try(date_parse({text}, '%m/%d/%Y')), "
            f"try(date_parse(replace({text}, '-', ''), '%d%b%Y')), "
            f"try(cast({text} as timestamp)))"
        )

    def epoch_value(self, column: str, unit: str) -> str:
        value = f"CAST({column} AS DOUBLE)"
        fn = "truncate" if unit == "days" else "floor"
        return f"CAST({fn}({value}) AS BIGINT)"

    def epoch_day(self, column: str, origin: str, unit: str) -> str:
        value = f"CAST({column} AS DOUBLE)"
        offset = f"truncate({value})" if unit == "days" else f"floor({value} / 86400)"
        return f"date_add('day', CAST({offset} AS BIGINT), DATE '{origin}')"


class DuckDbDateSqlPlatform(DateSqlPlatform):
    name = "duckdb"

    def text(self, column: str) -> str:
        return f"TRIM(CAST({column} AS VARCHAR))"

    def format_day(self, day: str) -> str:
        return f"coalesce(strftime({day}, '%Y-%m-%d'), '')"

    def native_day(self, column: str) -> str:
        return f"try_cast({column} AS DATE)"

    def parse_text(self, column: str, format: str) -> str:
        pattern = {
            "dash": "%Y-%m-%d",
            "compact": "%Y%m%d",
            "month": "%Y%m",
            "date9": "%d%b%Y",
            "datetime": "%d%b%Y:%H:%M:%S",
        }[format]
        return f"try_strptime({self.text(column)}, '{pattern}')::DATE"

    def generic_day(self, column: str) -> str:
        text = self.text(column)
        return (
            f"coalesce(try_strptime({text}, '%Y%m%d'), try_strptime({text}, '%m/%d/%Y'), "
            f"try_strptime(replace({text}, '-', ''), '%d%b%Y'), try_cast({text} as timestamp))"
        )

    def epoch_value(self, column: str, unit: str) -> str:
        value = f"CAST({column} AS DOUBLE)"
        fn = "trunc" if unit == "days" else "floor"
        return f"CAST({fn}({value}) AS BIGINT)"

    def epoch_day(self, column: str, origin: str, unit: str) -> str:
        value = f"CAST({column} AS DOUBLE)"
        offset = f"trunc({value})" if unit == "days" else f"floor({value} / 86400)"
        return f"DATE '{origin}' + CAST({offset} AS INTEGER)"


DATE_SQL_PLATFORMS: dict[str, DateSqlPlatform] = {}


def register_date_platform(platform: DateSqlPlatform, *, aliases: tuple[str, ...] = ()) -> None:
    for name in (platform.name, *aliases):
        DATE_SQL_PLATFORMS[name.lower()] = platform


def date_platform(dialect: str) -> DateSqlPlatform:
    try:
        return DATE_SQL_PLATFORMS[dialect.lower()]
    except KeyError:
        raise ValueError(f"date SQL does not support dialect {dialect!r}") from None


register_date_platform(OracleDateSqlPlatform())
register_date_platform(SparkDateSqlPlatform(), aliases=("databricks",))
register_date_platform(HadoopDateSqlPlatform())
register_date_platform(AthenaDateSqlPlatform())
register_date_platform(DuckDbDateSqlPlatform())
