from __future__ import annotations

import csv
import json
import random
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent
LEFT = ROOT / "data" / "left"
RIGHT = ROOT / "data" / "right"
STATUSES = ["NEW", "PAID", "SHIPPED", "CANCELLED"]
REGIONS = ["NE", "SE", "MW", "W"]
TIERS = ["Gold", "Silver", "Bronze"]


# >>> every calendar day in [start, end] <<< #
def days(start: date, end: date) -> list[date]:
    return [start + timedelta(days=offset) for offset in range((end - start).days + 1)]


# >>> write rows with the given header, creating parent directories <<< #
def write(path: Path, header: list[str], rows: list[list[object]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerows(rows)
    return path


# >>> orders: 59 daily partitions with injected row-count and column findings <<< #
def build_orders(rng: random.Random) -> dict[str, object]:
    left, right = [], []
    order_id = 100000
    for day in days(date(2026, 1, 1), date(2026, 2, 28)):
        iso = day.isoformat()
        for _ in range(30):
            order_id += 1
            amount = round(rng.uniform(5, 500), 2)
            status = rng.choice(STATUSES)
            region = rng.choice(REGIONS)
            left_row = [iso, order_id, f"{amount:.2f}", status, region]
            right_row = [iso, order_id, f"{amount:.2f}", status, region]
            if iso == "2026-02-05":
                right_row[2] = f"{amount + 0.01:.2f}"
            if iso == "2026-01-03":
                right_row[3] = status.lower()
            if iso != "2026-01-31":
                left.append(left_row)
            if iso != "2026-02-10":
                right.append(right_row)
    dropped = [row for row in right if row[0] == "2026-01-15"][:3]
    right = [row for row in right if row not in dropped]
    write(LEFT / "ORDERS.csv", ["BUSINESS_DATE", "ORDER_ID", "AMOUNT", "STATUS", "REGION"], left)
    write(RIGHT / "orders.csv", ["business_date", "order_id", "amount", "status", "region"], right)
    return {
        "left_rows": len(left),
        "right_rows": len(right),
        "injected": {
            "2026-01-15": "right side drops 3 rows -> count_diff",
            "2026-01-31": "left side lacks the day -> right_only",
            "2026-02-10": "right side lacks the day -> left_only",
            "2026-02-05": "right AMOUNT +0.01 on every row -> AMOUNT diff in window 2026-02-01",
            "2026-01-03": "right STATUS lower-cased -> STATUS diff in window 2026-01-01",
        },
    }


# >>> customers: one month, identical populations, TIER differs only by case <<< #
def build_customers(rng: random.Random) -> dict[str, object]:
    left, right = [], []
    cust_id = 5000
    for day in days(date(2026, 1, 1), date(2026, 1, 31)):
        iso = day.isoformat()
        for _ in range(20):
            cust_id += 1
            tier = rng.choice(TIERS)
            balance = round(rng.uniform(0, 10000), 2)
            left.append([iso, cust_id, tier, f"{balance:.2f}"])
            right.append([iso, cust_id, tier.upper(), f"{balance:.2f}"])
    write(LEFT / "CUSTOMERS.csv", ["SNAPSHOT_DT", "CUST_ID", "TIER", "BALANCE"], left)
    write(RIGHT / "customers.csv", ["snapshot_dt", "cust_id", "tier", "balance"], right)
    return {
        "left_rows": len(left),
        "right_rows": len(right),
        "injected": {"TIER": "right upper-cases the value; strict=true must still match"},
    }


def main() -> None:
    rng = random.Random(20260921)
    summary = {"orders": build_orders(rng), "customers": build_customers(rng)}
    (ROOT / "data" / "SYNTHETIC.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary))


if __name__ == "__main__":
    main()
