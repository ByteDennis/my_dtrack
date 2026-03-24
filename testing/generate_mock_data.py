#!/usr/bin/env python3
"""Generate mock CSV data for dtrack testing.

4 pair scenarios matching real production patterns.
Output goes to testing/mock/ with flat naming: {source}_{name}_row.csv
matching qualified_name() from dtrack/platforms/base.py.

Each pair has:
  - History:      2024-10-01 to 2024-12-31 (92 days)
  - Incremental:  2025-01-01 to 2025-01-07 (7 days)  [separate _incr files for later]

Run:  python testing/generate_mock_data.py
"""

import csv
import random
from datetime import date, datetime, timedelta
from pathlib import Path

random.seed(42)

HERE = Path(__file__).parent
MOCK = HERE / "mock"

HIST_START = date(2024, 10, 1)
HIST_END   = date(2024, 12, 31)
INCR_START = date(2025, 1, 1)
INCR_END   = date(2025, 1, 7)


def daterange(start, end):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def write_csv(path, rows, header=("date_value", "row_count")):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    print(f"  {path.relative_to(HERE)}  ({len(rows)} rows)")


def base_counts(start, end, base=300, noise=50, weekend_dip=True):
    rows = []
    for d in daterange(start, end):
        n = base + random.randint(-noise, noise)
        if weekend_dip and d.weekday() >= 5:
            n = int(n * 0.3)
        rows.append((d, max(1, n)))
    return rows


# -- Format helpers ----------------------------------------------------------

def fmt_timestamp(d):
    """Oracle TIMESTAMP with 00:00:00."""
    return f"{d.isoformat()} 00:00:00"

def fmt_iso(d):
    return d.isoformat()

def fmt_compact(d):
    """YYYYMMDD string."""
    return d.strftime("%Y%m%d")

def fmt_int(d):
    """Integer YYYYMMDD."""
    return int(d.strftime("%Y%m%d"))

def fmt_sas_datetime(d):
    """SAS datetime = seconds since 1960-01-01 00:00:00."""
    epoch = datetime(1960, 1, 1)
    dt = datetime(d.year, d.month, d.day)
    return str(int((dt - epoch).total_seconds()))


# ============================================================================
# PAIR 1: oracle_ts_vs_aws_str
#   Left:  pcds / ts_cust_daily   → TIMESTAMP '2024-10-01 00:00:00'
#   Right: aws  / str_cust_daily  → VARCHAR  '2024-10-01'
#   Diffs: 3 days with count mismatches
# ============================================================================
def gen_pair1():
    print("\n[1] oracle_ts_vs_aws_str")
    left = base_counts(HIST_START, HIST_END, base=350, noise=40)

    mismatch_days = {date(2024, 11, 15), date(2024, 12, 1), date(2024, 12, 20)}
    right = []
    for d, n in left:
        if d in mismatch_days:
            right.append((d, n + random.choice([-5, -3, 2, 7])))
        else:
            right.append((d, n))

    write_csv(MOCK / "pcds_ts_cust_daily_row.csv",
              [(fmt_timestamp(d), n) for d, n in left])
    write_csv(MOCK / "aws_str_cust_daily_row.csv",
              [(fmt_iso(d), n) for d, n in right])

    # Incremental (for later)
    left_inc = base_counts(INCR_START, INCR_END, base=350, noise=40)
    write_csv(MOCK / "pcds_ts_cust_daily_row_incr.csv",
              [(fmt_timestamp(d), n) for d, n in left_inc])
    write_csv(MOCK / "aws_str_cust_daily_row_incr.csv",
              [(fmt_iso(d), n) for d, n in left_inc])


# ============================================================================
# PAIR 2: oracle_date_vs_aws_cte
#   Left:  pcds / acct_master  → DATE '2024-10-01'
#   Right: aws  / cust_log     → TIMESTAMP (post ROW_NUMBER dedup)
#   Diffs: right missing Dec 25, dedup reduces counts ~2-8%
# ============================================================================
def gen_pair2():
    print("\n[2] oracle_date_vs_aws_cte")
    left = base_counts(HIST_START, HIST_END, base=280, noise=35)

    right = []
    for d, n in left:
        if d == date(2024, 12, 25):
            continue  # holiday gap
        dedup_loss = random.randint(0, int(n * 0.08))
        right.append((d, n - dedup_loss))

    write_csv(MOCK / "pcds_acct_master_row.csv",
              [(fmt_iso(d), n) for d, n in left])
    write_csv(MOCK / "aws_cust_log_row.csv",
              [(fmt_timestamp(d), n) for d, n in right])

    # Incremental
    left_inc = base_counts(INCR_START, INCR_END, base=280, noise=35)
    right_inc = [(d, n - random.randint(0, int(n * 0.08))) for d, n in left_inc]
    write_csv(MOCK / "pcds_acct_master_row_incr.csv",
              [(fmt_iso(d), n) for d, n in left_inc])
    write_csv(MOCK / "aws_cust_log_row_incr.csv",
              [(fmt_timestamp(d), n) for d, n in right_inc])


# ============================================================================
# PAIR 3: sas_dt_vs_aws_compact
#   Left:  sas / acct_snap       → SAS DATETIME (seconds since 1960)
#   Right: aws / acct_snap_aws   → VARCHAR '20241001' (YYYYMMDD)
#   Diffs: right has 2 extra early days (Sep 29-30)
# ============================================================================
def gen_pair3():
    print("\n[3] sas_dt_vs_aws_compact")
    left = base_counts(HIST_START, HIST_END, base=180, noise=25)

    right = [(date(2024, 9, 29), 170), (date(2024, 9, 30), 185)] + list(left)
    right.sort()

    write_csv(MOCK / "sas_acct_snap_row.csv",
              [(fmt_sas_datetime(d), n) for d, n in left])
    write_csv(MOCK / "aws_acct_snap_aws_row.csv",
              [(fmt_compact(d), n) for d, n in right])

    # Incremental — zero-row day on right
    left_inc = base_counts(INCR_START, INCR_END, base=180, noise=25)
    right_inc = [(d, 0 if d == date(2025, 1, 4) else n) for d, n in left_inc]
    write_csv(MOCK / "sas_acct_snap_row_incr.csv",
              [(fmt_sas_datetime(d), n) for d, n in left_inc])
    write_csv(MOCK / "aws_acct_snap_aws_row_incr.csv",
              [(fmt_compact(d), n) for d, n in right_inc])


# ============================================================================
# PAIR 4: oracle_date_vs_aws_int
#   Left:  oracle / pos_daily     → DATE '2024-10-01'
#   Right: aws    / pos_snapshot  → INTEGER 20241001 (with MAX subquery CTE)
#   Diffs: ~5% of days have count differences
# ============================================================================
def gen_pair4():
    print("\n[4] oracle_date_vs_aws_int")
    left = base_counts(HIST_START, HIST_END, base=420, noise=55)

    right = []
    for d, n in left:
        if random.random() < 0.05:
            right.append((d, n + random.choice([-10, -7, 8, 12])))
        else:
            right.append((d, n))

    write_csv(MOCK / "oracle_pos_daily_row.csv",
              [(fmt_iso(d), n) for d, n in left])
    write_csv(MOCK / "aws_pos_snapshot_row.csv",
              [(fmt_int(d), n) for d, n in right])

    # Incremental
    left_inc = base_counts(INCR_START, INCR_END, base=420, noise=55)
    write_csv(MOCK / "oracle_pos_daily_row_incr.csv",
              [(fmt_iso(d), n) for d, n in left_inc])
    write_csv(MOCK / "aws_pos_snapshot_row_incr.csv",
              [(fmt_int(d), n) for d, n in left_inc])


# ============================================================================
# PAIR 5: oracle_month_vs_aws_month
#   Left:  oracle / month_summary  → INTEGER YYYYMM (e.g. 202410)
#   Right: aws    / month_summary  → INTEGER YYYYMM (e.g. 202410)
#   Diffs: 1 month has count mismatch, right missing Oct
# ============================================================================
def gen_pair5():
    print("\n[5] oracle_month_vs_aws_month")
    # Monthly data: Oct–Dec 2024 = 3 months
    months = [date(2024, 10, 1), date(2024, 11, 1), date(2024, 12, 1)]
    left = [(d, random.randint(8000, 15000)) for d in months]

    # Right: missing Oct, Nov has a mismatch
    right = []
    for d, n in left:
        if d.month == 10:
            continue  # missing month
        elif d.month == 11:
            right.append((d, n + 230))  # count mismatch
        else:
            right.append((d, n))

    fmt_yyyymm = lambda d: int(d.strftime("%Y%m"))

    write_csv(MOCK / "oracle_month_summary_row.csv",
              [(fmt_yyyymm(d), n) for d, n in left])
    write_csv(MOCK / "aws_month_summary_row.csv",
              [(fmt_yyyymm(d), n) for d, n in right])

    # Incremental: Jan 2025
    inc = [(date(2025, 1, 1), random.randint(9000, 14000))]
    write_csv(MOCK / "oracle_month_summary_row_incr.csv",
              [(fmt_yyyymm(d), n) for d, n in inc])
    write_csv(MOCK / "aws_month_summary_row_incr.csv",
              [(fmt_yyyymm(d), n) for d, n in inc])


# ============================================================================

if __name__ == "__main__":
    print("Generating mock data for dtrack testing...")
    gen_pair1()
    gen_pair2()
    gen_pair3()
    gen_pair4()
    gen_pair5()
    print(f"\nDone! Files in {MOCK.relative_to(HERE)}/")
