"""Date parsing, format detection, bidirectional conversion, and vintage bucketing utilities"""

from datetime import datetime, timedelta
import re


# ============================================================================
# FORMAT REGISTRY: (strftime_pattern, needs_upper, label)
# ============================================================================
_DATE_FORMATS = [
    # ISO / Athena
    ("%Y-%m-%dT%H:%M:%S.%f", False, "YYYY-MM-DDTHH:MM:SS.ffffff"),
    ("%Y-%m-%dT%H:%M:%S", False, "YYYY-MM-DDTHH:MM:SS"),
    ("%Y-%m-%d %H:%M:%S.%f", False, "YYYY-MM-DD HH:MM:SS.ffffff"),
    ("%Y-%m-%d %H:%M:%S", False, "YYYY-MM-DD HH:MM:SS"),
    ("%Y-%m-%d", False, "YYYY-MM-DD"),
    # SAS datetime variants
    ("%d%b%Y:%H:%M:%S.%f", True, "DDMONYYYY:HH:MM:SS.ffffff"),
    ("%d%b%Y:%H:%M:%S", True, "DDMONYYYY:HH:MM:SS"),
    ("%d%b%Y", True, "DDMONYYYY"),
    ("%d%b%y", True, "DDMONYY"),
    # Oracle
    ("%d-%b-%Y %H:%M:%S", True, "DD-MON-YYYY HH24:MI:SS"),
    ("%d-%b-%Y", True, "DD-MON-YYYY"),
    ("%d-%b-%y", True, "DD-MON-YY"),
    # Slash variants
    ("%Y/%m/%d %H:%M:%S", False, "YYYY/MM/DD HH:MM:SS"),
    ("%Y/%m/%d", False, "YYYY/MM/DD"),
    ("%m/%d/%Y", False, "MM/DD/YYYY"),
]


def detect_format(value: str) -> str:
    """Detect the date format label of a string.

    Returns a human-readable label like 'YYYY-MM-DD' or 'DDMONYYYY:HH:MM:SS'.
    For numeric-only formats returns 'YYYYMMDD' or 'YYYYMM'.

    Raises:
        ValueError: If format is not recognized
    """
    value = value.strip().rstrip("Z")
    for fmt, upper, label in _DATE_FORMATS:
        try:
            val = value.upper() if upper else value
            datetime.strptime(val, fmt)
            return label
        except ValueError:
            continue
    if re.match(r'^\d{8}$', value):
        return "YYYYMMDD"
    if re.match(r'^\d{6}$', value):
        return "YYYYMM"
    if re.match(r'^\d{5,}$', value):
        num = int(value)
        return "SAS_DATETIME" if num >= 100_000_000 else "SAS_DATE"
    raise ValueError(f"Unable to detect date format: {value}")


def parse_date(value: str) -> str:
    """
    Parse various date formats and return YYYY-MM-DD string.

    Supported formats:
    - YYYY-MM-DD (ISO)
    - YYYY-MM-DD HH:MM:SS (ISO datetime, with optional fractional seconds)
    - YYYY-MM-DDTHH:MM:SS (ISO 8601 with T separator, optional .fff and Z)
    - DDMONYYYY:HH:MM:SS (SAS datetime, with optional fractional seconds)
    - DDMONYYYY (SAS date without time)
    - DDMONYY (SAS short year)
    - DD-MON-YYYY (Oracle default NLS_DATE_FORMAT)
    - DD-MON-YYYY HH24:MI:SS (Oracle datetime)
    - DD-MON-YY (Oracle short year)
    - YYYY/MM/DD (slash variant)
    - YYYYMMDD (8 digits)
    - YYYYMM (6 digits, returned as-is)
    - MM/DD/YYYY (US date format)

    Args:
        value: Date string to parse

    Returns:
        Date in YYYY-MM-DD format (or YYYYMM for 6-digit input)

    Raises:
        ValueError: If date format is not recognized
    """
    value = value.strip().rstrip("Z")

    # Normalize colon-separated milliseconds to dot: "HH:MM:SS:fff" → "HH:MM:SS.fff"
    # ISO: "2025-11-03 00:00:00:000"  SAS: "10MAR2025:00:00:00:000"
    _colon_ms = re.match(
        r'^(.+\d{2}:\d{2}:\d{2}):(\d{3,6})$', value)
    if _colon_ms:
        value = f"{_colon_ms.group(1)}.{_colon_ms.group(2)}"

    for fmt, upper, _label in _DATE_FORMATS:
        try:
            val = value.upper() if upper else value
            dt = datetime.strptime(val, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    # Try YYYYMMDD format (8 digits) - convert to YYYY-MM-DD
    if re.match(r'^\d{8}$', value):
        dt = datetime.strptime(value, "%Y%m%d")
        return dt.strftime("%Y-%m-%d")

    # Try YYYYMM format (6 digits) - keep as-is, don't convert to date
    if re.match(r'^\d{6}$', value):
        return value  # Return YYYYMM as-is (e.g., "202401")

    # SAS numeric date/datetime: integer days or seconds since 1960-01-01
    if re.match(r'^\d{5,}$', value):
        sas_epoch = datetime(1960, 1, 1)
        num = int(value)
        # SAS datetime (seconds) produces huge numbers (>= ~1.6B for year 2010+)
        # SAS date (days) produces smaller numbers (~18000 for year 2010)
        if num >= 100_000_000:
            # SAS datetime: seconds since 1960-01-01
            dt = sas_epoch + timedelta(seconds=num)
        else:
            # SAS date: days since 1960-01-01
            dt = sas_epoch + timedelta(days=num)
        return dt.strftime("%Y-%m-%d")

    raise ValueError(f"Unable to parse date: {value}")


# ============================================================================
# DateConverter: bidirectional format conversion
# ============================================================================

class DateConverter:
    """Bidirectional date format converter.

    Detects the original format on first value seen, then provides
    to_canonical() and to_original() for converting between the original
    format and YYYY-MM-DD.

    Usage:
        dc = DateConverter()
        dc.learn("03MAR2022:00:00:00")  # auto-detect SAS format
        dc.to_canonical("15JAN2024:00:00:00")  # -> "2024-01-15"
        dc.to_original("2024-01-15")            # -> "15JAN2024"

        # Or detect from a column of values:
        dc = DateConverter.from_values(["20220303", "20220304"])
    """

    # Canonical -> original format templates
    _ORIGINAL_FORMATS = {
        "YYYYMMDD":                     "%Y%m%d",
        "YYYYMM":                       "%Y%m",
        "YYYY-MM-DD":                   "%Y-%m-%d",
        "YYYY-MM-DD HH:MM:SS":         "%Y-%m-%d %H:%M:%S",
        "YYYY-MM-DD HH:MM:SS.ffffff":  "%Y-%m-%d %H:%M:%S.%f",
        "YYYY-MM-DDTHH:MM:SS":         "%Y-%m-%dT%H:%M:%S",
        "YYYY-MM-DDTHH:MM:SS.ffffff":  "%Y-%m-%dT%H:%M:%S.%f",
        "DDMONYYYY":                    "%d%b%Y",
        "DDMONYYYY:HH:MM:SS":          "%d%b%Y:%H:%M:%S",
        "DDMONYYYY:HH:MM:SS.ffffff":   "%d%b%Y:%H:%M:%S.%f",
        "DDMONYY":                      "%d%b%y",
        "DD-MON-YYYY":                  "%d-%b-%Y",
        "DD-MON-YYYY HH24:MI:SS":      "%d-%b-%Y %H:%M:%S",
        "DD-MON-YY":                    "%d-%b-%y",
        "YYYY/MM/DD":                   "%Y/%m/%d",
        "YYYY/MM/DD HH:MM:SS":         "%Y/%m/%d %H:%M:%S",
        "MM/DD/YYYY":                   "%m/%d/%Y",
    }

    # Formats where output should be uppercased (SAS/Oracle month abbreviations)
    _UPPER_FORMATS = {
        "DDMONYYYY", "DDMONYYYY:HH:MM:SS", "DDMONYYYY:HH:MM:SS.ffffff",
        "DDMONYY", "DD-MON-YYYY", "DD-MON-YYYY HH24:MI:SS", "DD-MON-YY",
    }

    def __init__(self, format_label: str = None):
        """Initialize with optional known format label."""
        self.format_label = format_label

    def learn(self, value: str) -> str:
        """Detect format from a sample value. Returns the format label."""
        if not self.format_label:
            self.format_label = detect_format(value)
        return self.format_label

    @classmethod
    def from_values(cls, values, sample_size: int = 5):
        """Create a DateConverter by detecting format from a list of values."""
        dc = cls()
        for v in values[:sample_size]:
            v = str(v).strip()
            if v:
                dc.learn(v)
                break
        return dc

    def to_canonical(self, value: str) -> str:
        """Convert from original format to YYYY-MM-DD."""
        return parse_date(value)

    def to_original(self, canonical: str) -> str:
        """Convert from YYYY-MM-DD back to the detected original format.

        Args:
            canonical: Date in YYYY-MM-DD format

        Returns:
            Date string in the original format
        """
        if not self.format_label:
            return canonical

        if self.format_label == "YYYYMM":
            # YYYY-MM-DD -> YYYYMM
            dt = datetime.strptime(canonical, "%Y-%m-%d")
            return dt.strftime("%Y%m")

        fmt = self._ORIGINAL_FORMATS.get(self.format_label)
        if not fmt:
            return canonical

        dt = datetime.strptime(canonical, "%Y-%m-%d")
        result = dt.strftime(fmt)
        if self.format_label in self._UPPER_FORMATS:
            result = result.upper()
        return result


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


def vintage_bucket_spans(from_date: str, to_date: str, vintage: str):
    """Compute (bucket_key, bucket_min, bucket_max) for each vintage bucket
    that overlaps the [from_date, to_date] range, without enumerating days.

    Args:
        from_date: range start, YYYY-MM-DD
        to_date:   range end (inclusive), YYYY-MM-DD
        vintage:   day | week | month | quarter | year

    Returns:
        List of (bucket_key, bmin, bmax) tuples, all in YYYY-MM-DD format.
        bmin is max(natural bucket start, from_date); bmax is min(natural
        bucket end, to_date) — partial buckets at the edges are clipped.
    """
    d_start = datetime.strptime(from_date, "%Y-%m-%d")
    d_end = datetime.strptime(to_date, "%Y-%m-%d")
    if d_end < d_start:
        return []

    def _natural_end(d):
        if vintage == "day":
            return d
        if vintage == "week":
            return d + timedelta(days=6 - d.weekday())  # Sunday
        if vintage == "month":
            if d.month == 12:
                next_month = d.replace(year=d.year + 1, month=1, day=1)
            else:
                next_month = d.replace(month=d.month + 1, day=1)
            return next_month - timedelta(days=1)
        if vintage == "quarter":
            q_first_month = ((d.month - 1) // 3) * 3 + 1
            next_q_first_month = q_first_month + 3
            if next_q_first_month > 12:
                first = d.replace(year=d.year + 1, month=1, day=1)
            else:
                first = d.replace(month=next_q_first_month, day=1)
            return first - timedelta(days=1)
        if vintage == "year":
            return d.replace(year=d.year + 1, month=1, day=1) - timedelta(days=1)
        raise ValueError(f"Invalid vintage: {vintage}")

    spans = []
    cur = d_start
    while cur <= d_end:
        bucket_key = bucket_date(cur.strftime("%Y-%m-%d"), vintage)
        natural_end = _natural_end(cur)
        bmax = min(natural_end, d_end)
        spans.append((bucket_key, cur.strftime("%Y-%m-%d"), bmax.strftime("%Y-%m-%d")))
        cur = bmax + timedelta(days=1)
    return spans


def format_vintage_label(dt_str: str, vintage: str) -> str:
    """Format a bucketed date as a human-readable label for HTML reports.

    Examples:
        format_vintage_label("2025-03-03", "week")    -> "Week of Mar 3"
        format_vintage_label("2025-01-01", "quarter") -> "Q1 2025"
        format_vintage_label("2025-03-01", "month")   -> "Mar 2025"
        format_vintage_label("2025-01-01", "year")    -> "2025"
        format_vintage_label("2025-03-15", "day")     -> "Mar 15, 2025"

    Args:
        dt_str: Date string in YYYY-MM-DD format
        vintage: One of 'day', 'week', 'month', 'quarter', 'year'

    Returns:
        Human-readable label string
    """
    if re.match(r'^\d{6}$', dt_str):
        # YYYYMM format
        y, m = int(dt_str[:4]), int(dt_str[4:])
        dt = datetime(y, m, 1)
        return dt.strftime("%b %Y")

    dt = datetime.strptime(dt_str, "%Y-%m-%d")

    if vintage == "day":
        return f"{dt.strftime('%b')} {dt.day}, {dt.year}"
    elif vintage == "week":
        iso_year, iso_week, _ = dt.isocalendar()
        return f"{iso_year}-W{iso_week:02d}"
    elif vintage == "month":
        return dt.strftime("%b %Y")
    elif vintage == "quarter":
        q = (dt.month - 1) // 3 + 1
        return f"Q{q} {dt.year}"
    elif vintage == "year":
        return str(dt.year)
    else:
        return dt_str
