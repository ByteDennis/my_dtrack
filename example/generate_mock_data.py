#!/usr/bin/env python3
"""
Generate mock row count data with > 100 overlapping partitions for vintage sampling.

For txn_monthly (monthly vintage YYYYMM):
  - Generate enough months so overlap > 100 after excluding mismatches/source-only

For cust_daily (daily vintage DATE):
  - Generate enough days so overlap > 100 after excluding mismatches/source-only
"""
import random
from datetime import date, timedelta
from pathlib import Path

random.seed(42)  # Reproducible results

def generate_monthly_data(start_ym, n_months, base_count=50000, variance=5000):
    """Generate monthly row counts in YYYYMM format."""
    data = []
    year, month = divmod(start_ym, 100)

    for i in range(n_months):
        ym = year * 100 + month
        count = base_count + random.randint(-variance, variance)
        data.append((ym, count))

        # Increment month
        month += 1
        if month > 12:
            month = 1
            year += 1

    return data

def generate_daily_data(start_date, n_days, base_count=1500, variance=100):
    """Generate daily row counts in YYYY-MM-DD format."""
    data = []
    current = start_date

    for i in range(n_days):
        count = base_count + random.randint(-variance, variance)
        data.append((current.strftime('%Y-%m-%d'), count))
        current += timedelta(days=1)

    return data

def create_test_scenario(oracle_data, aws_data, n_oracle_only=15, n_aws_only=12, n_mismatch=20):
    """
    Create test scenario with:
    - oracle-only partitions
    - aws-only partitions
    - mismatched partitions (different row counts)
    - matched partitions (same row counts)

    Ensures > 100 matching partitions remain after exclusions.
    """
    # Start with identical data
    oracle_out = oracle_data.copy()
    aws_out = aws_data.copy()

    # Remove random partitions to create source-only scenarios
    oracle_only_indices = random.sample(range(len(oracle_out)), n_oracle_only)
    for idx in sorted(oracle_only_indices, reverse=True):
        del aws_out[idx]

    aws_only_indices = random.sample(range(len(aws_out)), n_aws_only)
    for idx in sorted(aws_only_indices, reverse=True):
        del oracle_out[idx]

    # Create mismatches by altering row counts
    mismatch_indices = random.sample(range(min(len(oracle_out), len(aws_out))), n_mismatch)
    for idx in mismatch_indices:
        date_val, count = aws_out[idx]
        # Change AWS count by ±10-30%
        delta = int(count * random.uniform(0.1, 0.3))
        aws_out[idx] = (date_val, count + random.choice([-delta, delta]))

    return oracle_out, aws_out

def write_csv(filepath, data, header="date_value,row_count"):
    """Write data to CSV file."""
    with open(filepath, 'w') as f:
        f.write(header + '\n')
        for date_val, count in data:
            f.write(f'{date_val},{count}\n')

def generate_column_statistics(oracle_daily_data, aws_daily_data, oracle_monthly_data, aws_monthly_data):
    """
    Generate mock column statistics CSV files matching real extraction format.
    Format: column_name,dt,col_type,n_total,n_missing,n_unique,mean,std,min_val,max_val,top_10

    Respects vintage settings:
    - week: generates weekly aggregated stats (Monday dates)
    - sample: generates sampled subset of dates (simulates N_SAMPLE behavior)

    Args:
        oracle_daily_data: List of (date, row_count) tuples for Oracle daily data
        aws_daily_data: List of (date, row_count) tuples for AWS daily data
        oracle_monthly_data: List of (date, row_count) tuples for Oracle monthly data
        aws_monthly_data: List of (date, row_count) tuples for AWS monthly data
    """
    import json
    outdir = Path(__file__).parent

    print("📊 Generating column statistics (vintage-aware)...")
    print()

    # Convert row count data to dictionaries for easy lookup
    oracle_daily_dict = dict(oracle_daily_data)
    aws_daily_dict = dict(aws_daily_data)
    oracle_monthly_dict = dict(oracle_monthly_data)
    aws_monthly_dict = dict(aws_monthly_data)

    # N_SAMPLE=100 from .env (for vintage=sample)
    # Generate realistic sample sizes based on vintage mode

    # cust_daily: vintage=week → weekly buckets from overlap period
    # Assuming ~1.5 years of overlap → ~78 weeks
    from datetime import timedelta
    start_week = date(2023, 1, 2)  # Monday
    sample_weeks = []
    for i in range(78):  # ~1.5 years of weeks
        week_date = start_week + timedelta(weeks=i)
        date_str = week_date.strftime('%Y-%m-%d')
        # Only include weeks that exist in BOTH oracle and aws row data
        if date_str in oracle_daily_dict and date_str in aws_daily_dict:
            sample_weeks.append(date_str)

    # txn_monthly: vintage=sample with N_SAMPLE=100
    # Simulate: 123 matching months, sample 100 of them (without replacement)
    # Use months that exist in BOTH oracle and aws row data
    common_months = sorted(set(oracle_monthly_dict.keys()) & set(aws_monthly_dict.keys()))
    random.seed(42)  # Reproducible
    sample_months = sorted(random.sample(common_months, min(100, len(common_months))))  # Sample up to 100 months

    # Oracle CUST_DAILY column stats (weekly vintage - Mondays)
    # Most dates will have matching statistics (~85%), a few will have differences (~15%)
    oracle_cust_path = outdir.parent / 'oracle_mock' / 'CUST_DAILY_col.csv'
    mismatch_weeks = random.sample(range(len(sample_weeks)), min(12, len(sample_weeks) // 7))  # ~15% mismatch

    with open(oracle_cust_path, 'w') as f:
        f.write("column_name,dt,col_type,n_total,n_missing,n_unique,mean,std,min_val,max_val,top_10\n")
        for i, dt in enumerate(sample_weeks):
            # Use actual row count from row count data
            n_total = oracle_daily_dict.get(dt, 1500)
            # RPT_DT (date column) - categorical
            f.write(f"RPT_DT,{dt},categorical,{n_total},0,1,,,,{dt},{{}}\n")
            # CUSTOMER_ID - numeric
            f.write(f"CUSTOMER_ID,{dt},numeric,{n_total},0,{n_total-100},25000.5,15000.2,1000,50000,\n")
            # AMT - numeric
            f.write(f"AMT,{dt},numeric,{n_total},3,{n_total-50},2500.75,1800.5,10.50,9999.99,\n")
            # REGION - categorical with top_10
            top10_region = json.dumps([{"value": "EAST", "count": 600}, {"value": "WEST", "count": 500}, {"value": "NORTH", "count": 250}, {"value": "SOUTH", "count": 150}])
            f.write(f"REGION,{dt},categorical,{n_total},0,5,,,EAST,WEST,\"{top10_region}\"\n")
            # STATUS - categorical
            top10_status = json.dumps([{"value": "A", "count": 1200}, {"value": "I", "count": 200}, {"value": "P", "count": 100}])
            f.write(f"STATUS,{dt},categorical,{n_total},1,3,,,A,Z,\"{top10_status}\"\n")
    print(f"  ✓ Oracle cust_daily: {len(sample_weeks) * 5} stat rows (vintage=week, {len(sample_weeks)} weeks from overlap) → {oracle_cust_path}")

    # AWS cust_daily column stats (weekly vintage - MOSTLY MATCHING)
    aws_cust_path = outdir.parent / 'athena_mock' / 'analytics_db' / 'cust_daily' / 'col.csv'
    with open(aws_cust_path, 'w') as f:
        f.write("column_name,dt,col_type,n_total,n_missing,n_unique,mean,std,min_val,max_val,top_10\n")
        for i, dt in enumerate(sample_weeks):
            # Use actual row count from row count data
            n_total = aws_daily_dict.get(dt, 1500)

            f.write(f"rpt_dt,{dt},categorical,{n_total},0,1,,,,{dt},{{}}\n")
            # Most numeric stats match
            f.write(f"customer_id,{dt},numeric,{n_total},0,{n_total-100},25000.5,15000.2,1000,50000,\n")
            f.write(f"amount,{dt},numeric,{n_total},3,{n_total-50},2500.75,1800.5,10.50,9999.99,\n")
            # Most categorical stats match
            top10_region = json.dumps([{"value": "EAST", "count": 600}, {"value": "WEST", "count": 500}, {"value": "NORTH", "count": 250}, {"value": "SOUTH", "count": 150}])
            f.write(f"region,{dt},categorical,{n_total},0,5,,,EAST,WEST,\"{top10_region}\"\n")
            top10_status = json.dumps([{"value": "A", "count": 1200}, {"value": "I", "count": 200}, {"value": "P", "count": 100}])
            f.write(f"status,{dt},categorical,{n_total},1,3,,,A,Z,\"{top10_status}\"\n")
    print(f"  ✓ AWS cust_daily:    {len(sample_weeks) * 5} stat rows (vintage=week, {len(sample_weeks)} weeks, ~85% match) → {aws_cust_path}")
    print()

    # Oracle TXN_MONTHLY column stats (sampled months - vintage=sample)
    # Most dates will have matching statistics (~90%), a few will have differences (~10%)
    oracle_txn_path = outdir.parent / 'oracle_mock' / 'TXN_MONTHLY_col.csv'
    mismatch_months = random.sample(range(len(sample_months)), min(10, len(sample_months) // 10))  # ~10% mismatch

    with open(oracle_txn_path, 'w') as f:
        f.write("column_name,dt,col_type,n_total,n_missing,n_unique,mean,std,min_val,max_val,top_10\n")
        for i, dt in enumerate(sample_months):
            # Use actual row count from row count data
            n_total = oracle_monthly_dict.get(dt, 50000)
            # MONTH_DT - numeric (YYYYMM integer)
            f.write(f"MONTH_DT,{dt},numeric,{n_total},0,1,{dt},0,{dt},{dt},\n")
            # ACCT_ID - numeric
            f.write(f"ACCT_ID,{dt},numeric,{n_total},0,8500,55000.2,25000.5,10000,99999,\n")
            # TXN_TYPE - categorical
            top10_type = json.dumps([{"value": "CREDIT", "count": 20000}, {"value": "DEBIT", "count": 18000}, {"value": "TRANSFER", "count": 10000}, {"value": "WITHDRAWAL", "count": 2000}])
            f.write(f"TXN_TYPE,{dt},categorical,{n_total},0,15,,,CREDIT,WITHDRAWAL,\"{top10_type}\"\n")
            # TXN_AMT - numeric
            f.write(f"TXN_AMT,{dt},numeric,{n_total},5,{n_total-100},5500.50,3200.25,0.01,999999.99,\n")
            # CHANNEL - categorical
            top10_channel = json.dumps([{"value": "WEB", "count": 25000}, {"value": "MOBILE", "count": 15000}, {"value": "ATM", "count": 8000}, {"value": "BRANCH", "count": 2000}])
            f.write(f"CHANNEL,{dt},categorical,{n_total},2,6,,,ATM,WEB,\"{top10_channel}\"\n")
    print(f"  ✓ Oracle txn_monthly: {len(sample_months) * 5} stat rows (vintage=sample, N_SAMPLE={len(sample_months)} from ~150 months) → {oracle_txn_path}")

    # AWS txn_monthly column stats (sampled months - MOSTLY MATCHING)
    aws_txn_path = outdir.parent / 'athena_mock' / 'analytics_db' / 'txn_monthly' / 'col.csv'
    with open(aws_txn_path, 'w') as f:
        f.write("column_name,dt,col_type,n_total,n_missing,n_unique,mean,std,min_val,max_val,top_10\n")
        for i, dt in enumerate(sample_months):
            # Use actual row count from row count data
            n_total = aws_monthly_dict.get(dt, 50000)

            f.write(f"month_dt,{dt},categorical,{n_total},0,1,,,,{dt},{{}}\n")
            # Most numeric stats match
            f.write(f"account_id,{dt},numeric,{n_total},0,8500,55000.2,25000.5,10000,99999,\n")
            # Most categorical stats match
            top10_type = json.dumps([{"value": "CREDIT", "count": 20000}, {"value": "DEBIT", "count": 18000}, {"value": "TRANSFER", "count": 10000}, {"value": "WITHDRAWAL", "count": 2000}])
            f.write(f"txn_type,{dt},categorical,{n_total},0,15,,,CREDIT,WITHDRAWAL,\"{top10_type}\"\n")
            f.write(f"transaction_amount,{dt},numeric,{n_total},5,{n_total-100},5500.50,3200.25,0.01,999999.99,\n")
            top10_channel = json.dumps([{"value": "WEB", "count": 25000}, {"value": "MOBILE", "count": 15000}, {"value": "ATM", "count": 8000}, {"value": "BRANCH", "count": 2000}])
            f.write(f"channel,{dt},categorical,{n_total},2,6,,,ATM,WEB,\"{top10_channel}\"\n")
    print(f"  ✓ AWS txn_monthly:    {len(sample_months) * 5} stat rows (vintage=sample, {len(sample_months)} sampled months, ~90% match) → {aws_txn_path}")
    print()


def generate_column_metadata():
    """
    Generate column metadata CSVs with realistic migration scenarios:
    - Matched columns (case-insensitive)
    - Renamed columns (need manual mapping)
    - Oracle-only columns (not migrating)
    - AWS-only columns (new columns)
    """
    outdir = Path(__file__).parent

    print("📋 Generating column metadata...")
    print()

    # ===== cust_daily columns =====
    print("📋 cust_daily columns:")

    # Oracle CUST_DAILY - includes legacy columns
    oracle_cust_cols = [
        ("RPT_DT", "DATE"),
        ("CUSTOMER_ID", "NUMBER"),
        ("AMT", "NUMBER"),  # Will be renamed to 'amount' in AWS
        ("REGION", "VARCHAR2"),
        ("STATUS", "VARCHAR2"),
        # Oracle-only (legacy, not migrating)
        ("LEGACY_FLAG", "VARCHAR2"),
        ("OLD_SYSTEM_ID", "NUMBER"),
        ("BATCH_NO", "NUMBER"),
    ]

    oracle_cust_path = outdir.parent / 'oracle_mock' / 'CUST_DAILY_columns.csv'
    with open(oracle_cust_path, 'w') as f:
        f.write("COLUMN_NAME,DATA_TYPE\n")
        for col_name, col_type in oracle_cust_cols:
            f.write(f"{col_name},{col_type}\n")
    print(f"  ✓ Oracle: {len(oracle_cust_cols)} columns → {oracle_cust_path}")

    # AWS cust_daily - includes new columns
    aws_cust_cols = [
        ("rpt_dt", "date"),
        ("customer_id", "bigint"),
        ("amount", "double"),  # Renamed from AMT
        ("region", "varchar"),
        ("status", "varchar"),
        # AWS-only (new columns)
        ("created_at", "timestamp"),
        ("updated_at", "timestamp"),
        ("data_source", "varchar"),
    ]

    aws_cust_path = outdir.parent / 'athena_mock' / 'analytics_db' / 'cust_daily' / 'columns.csv'
    with open(aws_cust_path, 'w') as f:
        f.write("column_name,data_type\n")
        for col_name, col_type in aws_cust_cols:
            f.write(f"{col_name},{col_type}\n")
    print(f"  ✓ AWS:    {len(aws_cust_cols)} columns → {aws_cust_path}")
    print(f"  → Matched: 4, Renamed: 1 (AMT→amount), Oracle-only: 3, AWS-only: 3")
    print()

    # ===== txn_monthly columns =====
    print("📋 txn_monthly columns:")

    # Oracle TXN_MONTHLY - includes legacy columns
    oracle_txn_cols = [
        ("MONTH_DT", "NUMBER"),  # YYYYMM integer
        ("ACCT_ID", "NUMBER"),  # Will be renamed to 'account_id' in AWS
        ("TXN_TYPE", "VARCHAR2"),
        ("TXN_AMT", "NUMBER"),  # Will be renamed to 'transaction_amount' in AWS
        ("CHANNEL", "VARCHAR2"),
        # Oracle-only (legacy, not migrating)
        ("LEGACY_BATCH_ID", "NUMBER"),
        ("OLD_PROCESSING_FLAG", "VARCHAR2"),
        ("SRC_SYSTEM_CODE", "VARCHAR2"),
    ]

    oracle_txn_path = outdir.parent / 'oracle_mock' / 'TXN_MONTHLY_columns.csv'
    with open(oracle_txn_path, 'w') as f:
        f.write("COLUMN_NAME,DATA_TYPE\n")
        for col_name, col_type in oracle_txn_cols:
            f.write(f"{col_name},{col_type}\n")
    print(f"  ✓ Oracle: {len(oracle_txn_cols)} columns → {oracle_txn_path}")

    # AWS txn_monthly - includes new columns
    aws_txn_cols = [
        ("month_dt", "varchar"),  # YYYYMM string
        ("account_id", "bigint"),  # Renamed from ACCT_ID
        ("txn_type", "varchar"),
        ("transaction_amount", "double"),  # Renamed from TXN_AMT
        ("channel", "varchar"),
        # AWS-only (new columns)
        ("processed_timestamp", "timestamp"),
        ("data_quality_score", "double"),
        ("migration_batch", "varchar"),
    ]

    aws_txn_path = outdir.parent / 'athena_mock' / 'analytics_db' / 'txn_monthly' / 'columns.csv'
    with open(aws_txn_path, 'w') as f:
        f.write("column_name,data_type\n")
        for col_name, col_type in aws_txn_cols:
            f.write(f"{col_name},{col_type}\n")
    print(f"  ✓ AWS:    {len(aws_txn_cols)} columns → {aws_txn_path}")
    print(f"  → Matched: 3, Renamed: 2 (ACCT_ID→account_id, TXN_AMT→transaction_amount), Oracle-only: 3, AWS-only: 3")
    print()

if __name__ == '__main__':
    outdir = Path(__file__).parent

    print("Generating mock data for vintage sampling tests...")
    print()

    # ===== Monthly data (txn_monthly) =====
    # Need > 100 matching partitions, so generate 150 months with some exclusions
    print("📅 Monthly data (txn_monthly):")
    print("  - Generating 150 months (202001-202412 + extra)")

    monthly_base = generate_monthly_data(start_ym=202001, n_months=150)
    oracle_monthly, aws_monthly = create_test_scenario(
        monthly_base, monthly_base,
        n_oracle_only=15,  # Oracle-only months
        n_aws_only=12,     # AWS-only months
        n_mismatch=20      # Mismatched months
    )

    # Write Oracle mock data
    oracle_mock = outdir.parent / 'oracle_mock' / 'TXN_MONTHLY_row.csv'
    write_csv(oracle_mock, oracle_monthly)
    print(f"  ✓ Oracle: {len(oracle_monthly)} months → {oracle_mock}")

    # Write AWS mock data
    aws_mock = outdir.parent / 'athena_mock' / 'analytics_db' / 'txn_monthly' / 'row.csv'
    write_csv(aws_mock, aws_monthly)
    print(f"  ✓ AWS:    {len(aws_monthly)} months → {aws_mock}")

    # Calculate expected overlap
    oracle_dates = {d for d, _ in oracle_monthly}
    aws_dates = {d for d, _ in aws_monthly}
    overlap = len(oracle_dates & aws_dates)
    print(f"  → Expected overlap: ~{overlap} months (should be > 100)")
    print()

    # ===== Daily data (cust_daily) - WEEKLY VINTAGE =====
    # Need > 100 matching weeks, but we're storing daily dates that get bucketed to weeks
    # Generate ~730 days (2 years) to get ~104 weeks, with some exclusions
    print("📅 Daily data (cust_daily) - weekly vintage:")
    print("  - Generating 730 days (2 years) → ~104 weeks when bucketed")

    daily_base = generate_daily_data(start_date=date(2023, 1, 2), n_days=730)  # Start on Monday
    oracle_daily, aws_daily = create_test_scenario(
        daily_base, daily_base,
        n_oracle_only=80,   # Oracle-only days (~11 weeks worth)
        n_aws_only=70,      # AWS-only days (~10 weeks worth)
        n_mismatch=50       # Mismatched days
    )

    # Write Oracle mock data (PCDS uses uppercase table name)
    oracle_daily_mock = outdir.parent / 'oracle_mock' / 'CUST_DAILY_row.csv'
    write_csv(oracle_daily_mock, oracle_daily)
    print(f"  ✓ Oracle: {len(oracle_daily)} days → {oracle_daily_mock}")

    # Write AWS mock data
    aws_daily_mock = outdir.parent / 'athena_mock' / 'analytics_db' / 'cust_daily' / 'row.csv'
    write_csv(aws_daily_mock, aws_daily)
    print(f"  ✓ AWS:    {len(aws_daily)} days → {aws_daily_mock}")

    # Calculate expected overlap
    oracle_dates = {d for d, _ in oracle_daily}
    aws_dates = {d for d, _ in aws_daily}
    overlap = len(oracle_dates & aws_dates)
    print(f"  → Expected overlap: ~{overlap} days (should be > 100)")
    print()

    # Generate column metadata
    generate_column_metadata()

    # Generate column statistics (AFTER row count data is generated)
    generate_column_statistics(oracle_daily, aws_daily, oracle_monthly, aws_monthly)

    print("✅ Mock data generation complete!")
    print()
    print("Next steps:")
    print("  1. Extract mock data:")
    print("     dtrack gen-sas extract_config_test.json --outdir ./sas --type row")
    print("     dtrack gen-aws extract_config_test.json --outdir ./csv/ --type row")
    print("  2. Load extracted data:")
    print("     dtrack load-row project.db ./sas/ --config extract_config_test.json --mode replace")
    print("     dtrack load-row project.db ./csv/ --config extract_config_test.json --mode replace")
    print("  3. Compare rows:")
    print("     dtrack compare-row project.db --config pairs_config.json -y")
    print("  4. Generate column extraction with sampling:")
    print("     dtrack gen-sas extract_config_test.json --outdir ./sas --type col --db project.db --vintage sample")
    print("     dtrack gen-aws extract_config_test.json --outdir ./csv/ --type col --db project.db --vintage sample")
