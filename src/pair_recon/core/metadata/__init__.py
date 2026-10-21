import re

_TABLE = re.compile(r"^[A-Za-z_][\w$]*(?:\.[A-Za-z_][\w$]*){2}$")


def validate_table_name(table: str) -> str:
    value = str(table or "").strip()
    if not _TABLE.fullmatch(value):
        raise ValueError(f"invalid table name: {table}")
    return value


def companion_table(pair_table: str, name: str) -> str:
    return f"{validate_table_name(pair_table).rsplit('.', 1)[0]}.{name}"
