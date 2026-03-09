"""Date parsing and vintage bucketing utilities"""

from datetime import datetime, timedelta
import re


def parse_date(value: str) -> str:
    """
    Parse various date formats and return YYYY-MM-DD string.

    Supported formats:
    - YYYY-MM-DD (ISO)
    - YYYY-MM-DD HH:MM:SS (ISO datetime)
    - DDMONYYYY:HH:MM:SS (SAS datetime)
    - YYYYMM (converts to first of month)
    - MM/DD/YYYY (US date format)

    Args:
        value: Date string to parse

    Returns:
        Date in YYYY-MM-DD format

    Raises:
        ValueError: If date format is not recognized
    """
    value = value.strip()

    # Try multiple formats
    formats = [
        ("%Y-%m-%d", False),                  # ISO date
        ("%Y-%m-%d %H:%M:%S", False),         # ISO datetime
        ("%d%b%Y:%H:%M:%S", True),            # SAS datetime: 03MAR2022:00:00:00
        ("%m/%d/%Y", False),                  # US date
    ]

    for fmt, upper_case in formats:
        try:
            val = value.upper() if upper_case else value
            dt = datetime.strptime(val, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    # Try YYYYMM format (6 digits) - keep as-is, don't convert to date
    if re.match(r'^\d{6}$', value):
        return value  # Return YYYYMM as-is (e.g., "202401")

    raise ValueError(f"Unable to parse date: {value}")


def bucket_date(dt_str: str, vintage: str) -> str:
    """
    Bucket a date according to the specified vintage granularity.

    Args:
        dt_str: Date string in YYYY-MM-DD format or YYYYMM format
        vintage: One of 'day', 'week', 'month', 'quarter', 'year'

    Returns:
        Bucketed date in YYYY-MM-DD or YYYYMM format

    Raises:
        ValueError: If vintage is not valid
    """
    # Check if YYYYMM format (6 digits)
    if re.match(r'^\d{6}$', dt_str):
        # YYYYMM format - already at month granularity, return as-is
        # (bucketing not meaningful for YYYYMM since it's already monthly)
        return dt_str

    dt = datetime.strptime(dt_str, "%Y-%m-%d")

    if vintage == "day":
        return dt_str

    elif vintage == "week":
        # ISO week starts on Monday (weekday 0)
        # Calculate days to subtract to get to Monday
        days_since_monday = dt.weekday()
        monday = dt - timedelta(days=days_since_monday)
        return monday.strftime("%Y-%m-%d")

    elif vintage == "month":
        # First of month
        return dt.replace(day=1).strftime("%Y-%m-%d")

    elif vintage == "quarter":
        # First of quarter
        quarter_month = ((dt.month - 1) // 3) * 3 + 1
        return dt.replace(month=quarter_month, day=1).strftime("%Y-%m-%d")

    elif vintage == "year":
        # First of year
        return dt.replace(month=1, day=1).strftime("%Y-%m-%d")

    else:
        raise ValueError(f"Invalid vintage: {vintage}. Must be one of: day, week, month, quarter, year")
