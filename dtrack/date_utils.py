
from datetime import datetime, timedelta
import re

_DATE_FORMATS = [
    ("%Y-%m-%dT%H:%M:%S.%f", False, "YYYY-MM-DDTHH:MM:SS.ffffff"),
    ("%Y-%m-%dT%H:%M:%S", False, "YYYY-MM-DDTHH:MM:SS"),
    ("%Y-%m-%d %H:%M:%S.%f", False, "YYYY-MM-DD HH:MM:SS.ffffff"),
    ("%Y-%m-%d %H:%M:%S", False, "YYYY-MM-DD HH:MM:SS"),
    ("%Y-%m-%d", False, "YYYY-MM-DD"),
    ("%d%b%Y:%H:%M:%S.%f", True, "DDMONYYYY:HH:MM:SS.ffffff"),
    ("%d%b%Y:%H:%M:%S", True, "DDMONYYYY:HH:MM:SS"),
    ("%d%b%Y", True, "DDMONYYYY"),
    ("%d%b%y", True, "DDMONYY"),
    ("%d-%b-%Y %H:%M:%S.%f", True, "DD-MON-YYYY HH24:MI:SS.ffffff"),
    ("%d-%b-%Y %H:%M:%S", True, "DD-MON-YYYY HH24:MI:SS"),
    ("%d-%b-%Y", True, "DD-MON-YYYY"),
    ("%d-%b-%y %H:%M:%S.%f", True, "DD-MON-YY HH24:MI:SS.ffffff"),
    ("%d-%b-%y %H:%M:%S", True, "DD-MON-YY HH24:MI:SS"),
    ("%d-%b-%y", True, "DD-MON-YY"),
    ("%Y/%m/%d %H:%M:%S", False, "YYYY/MM/DD HH:MM:SS"),
    ("%Y/%m/%d", False, "YYYY/MM/DD"),
    ("%m/%d/%Y", False, "MM/DD/YYYY"),
]

# >>> Detect the date format label of a string <<< #
def detect_format(value: str) -> str:
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

# >>> Parse various date formats and return YYYY-MM-DD string <<< #
def parse_date(value: str) -> str:
    value = value.strip().rstrip("Z")

    _colon_ms = re.match(
        r'^(.+\d{2}:\d{2}:\d{2}):(\d{3,6})$', value)
    if _colon_ms:
        value = f"{_colon_ms.group(1)}.{_colon_ms.group(2)}"

    _oracle_ts = re.match(
        r'^(.+ \d{1,2})\.(\d{2})\.(\d{2})(\.\d+)?(.*)$', value)
    if _oracle_ts:
        head, mm, ss, frac, tail = _oracle_ts.groups()
        value = f"{head}:{mm}:{ss}{frac or ''}{tail or ''}"

    for fmt, upper, _label in _DATE_FORMATS:
        try:
            val = value.upper() if upper else value
            dt = datetime.strptime(val, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    if re.match(r'^\d{8}$', value):
        dt = datetime.strptime(value, "%Y%m%d")
        return dt.strftime("%Y-%m-%d")

    if re.match(r'^\d{6}$', value):
        return value

    if re.match(r'^\d{5,}$', value):
        sas_epoch = datetime(1960, 1, 1)
        num = int(value)
        if num >= 100_000_000:
            dt = sas_epoch + timedelta(seconds=num)
        else:
            dt = sas_epoch + timedelta(days=num)
        return dt.strftime("%Y-%m-%d")

    raise ValueError(f"Unable to parse date: {value}")

# >>> Bidirectional date format converter <<< #
class DateConverter:

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

    _UPPER_FORMATS = {
        "DDMONYYYY", "DDMONYYYY:HH:MM:SS", "DDMONYYYY:HH:MM:SS.ffffff",
        "DDMONYY", "DD-MON-YYYY", "DD-MON-YYYY HH24:MI:SS", "DD-MON-YY",
    }

    # >>> Initialize with optional known format label <<< #
    def __init__(self, format_label: str = None):
        self.format_label = format_label

    # >>> Detect format from a sample value. Returns the format label <<< #
    def learn(self, value: str) -> str:
        if not self.format_label:
            self.format_label = detect_format(value)
        return self.format_label

    # >>> Create a DateConverter by detecting format from a list of values <<< #
    @classmethod
    def from_values(cls, values, sample_size: int = 5):
        dc = cls()
        for v in values[:sample_size]:
            v = str(v).strip()
            if v:
                dc.learn(v)
                break
        return dc

    # >>> Convert from original format to YYYY-MM-DD <<< #
    def to_canonical(self, value: str) -> str:
        return parse_date(value)

    # >>> Convert from YYYY-MM-DD back to the detected original format <<< #
    def to_original(self, canonical: str) -> str:
        if not self.format_label:
            return canonical

        if self.format_label == "YYYYMM":
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

# >>> Bucket a date according to the specified vintage granularity <<< #
def bucket_date(dt_str: str, vintage: str) -> str:
    if re.match(r'^\d{6}$', dt_str):
        return dt_str

    dt = datetime.strptime(dt_str, "%Y-%m-%d")

    if vintage == "day":
        return dt_str

    elif vintage == "week":
        days_since_monday = dt.weekday()
        monday = dt - timedelta(days=days_since_monday)
        return monday.strftime("%Y-%m-%d")

    elif vintage == "month":
        return dt.replace(day=1).strftime("%Y-%m-%d")

    elif vintage == "quarter":
        quarter_month = ((dt.month - 1) // 3) * 3 + 1
        return dt.replace(month=quarter_month, day=1).strftime("%Y-%m-%d")

    elif vintage == "year":
        return dt.replace(month=1, day=1).strftime("%Y-%m-%d")

    else:
        raise ValueError(f"Invalid vintage: {vintage}. Must be one of: day, week, month, quarter, year")

# >>> Compute (bucket_key, bucket_min, bucket_max) for each vintage bucket <<< #
def vintage_bucket_spans(from_date: str, to_date: str, vintage: str):
    d_start = datetime.strptime(from_date, "%Y-%m-%d")
    d_end = datetime.strptime(to_date, "%Y-%m-%d")
    if d_end < d_start:
        return []

    def _natural_end(d):
        if vintage == "day":
            return d
        if vintage == "week":
            return d + timedelta(days=6 - d.weekday())
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

# >>> Format a bucketed date as a human-readable label for HTML reports <<< #
def format_vintage_label(dt_str: str, vintage: str) -> str:
    if re.match(r'^\d{6}$', dt_str):
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
