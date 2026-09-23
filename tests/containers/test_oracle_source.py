import csv
import time

import pandas as pd
import pytest
from support import synthetic_data

from dtrack_left.sources import OracleSource
from dtrack_left.statistics import compute_csv_statistics

pytestmark = pytest.mark.containers
IMAGE = "gvenzl/oracle-free:slim-faststart"


@pytest.fixture(scope="module")
def oracle():
    testcontainers = pytest.importorskip("testcontainers.core.container")
    container = (
        testcontainers.DockerContainer(IMAGE)
        .with_env("ORACLE_PASSWORD", "dtrack")
        .with_env("APP_USER", "dtrack")
        .with_env("APP_USER_PASSWORD", "dtrack")
        .with_exposed_ports(1521)
    )
    container.start()
    from testcontainers.core.waiting_utils import wait_for_logs

    wait_for_logs(container, "DATABASE IS READY TO USE!", timeout=600)
    dsn = f"{container.get_container_host_ip()}:{container.get_exposed_port(1521)}/FREEPDB1"
    yield {"user": "dtrack", "password": "dtrack", "dsn": dsn}
    container.stop()


@pytest.fixture(scope="module")
def orders_table(oracle, tmp_path_factory):
    import oracledb

    root = tmp_path_factory.mktemp("synthetic")
    synthetic_data(root)
    rows = list(csv.DictReader((root / "left/ORDERS.csv").open()))
    for _ in range(30):
        try:
            connection = oracledb.connect(**oracle)
            break
        except oracledb.Error:
            time.sleep(2)
    with connection:
        cursor = connection.cursor()
        cursor.execute(
            "CREATE TABLE ORDERS (BUSINESS_DATE DATE, ORDER_ID NUMBER(12), AMOUNT NUMBER(10,2), STATUS VARCHAR2(20), REGION VARCHAR2(4))"
        )
        cursor.executemany(
            "INSERT INTO ORDERS VALUES (TO_DATE(:1, 'YYYY-MM-DD'), :2, :3, :4, :5)",
            [
                (r["BUSINESS_DATE"], int(r["ORDER_ID"]), r["AMOUNT"], r["STATUS"], r["REGION"])
                for r in rows
            ],
        )
        connection.commit()
    return root


def _source(oracle):
    return OracleSource(
        {
            "source": "oracle",
            "table": "ORDERS",
            "date_col": "BUSINESS_DATE",
            "date_type": "date",
            **oracle,
        }
    )


# >>> the left-side SQL counts a real Oracle table exactly, day by day <<< #
def test_oracle_partition_counts_match_the_csv_truth(oracle, orders_table):
    source = _source(oracle)
    assert source.preflight()["ok"]
    counts = source.partition_counts("2026-01-01", "2026-02-28")
    truth = pd.read_csv(orders_table / "left/ORDERS.csv", dtype=object)[
        "BUSINESS_DATE"
    ].value_counts()
    assert counts == {k: int(v) for k, v in truth.items()}
    assert len(counts) == 58 and "2026-01-31" not in counts


# >>> a window pull with holes returns exactly the admitted days, and its statistics equal the CSV's <<< #
def test_oracle_window_pull_honours_holes_and_statistics_match_csv(oracle, orders_table, tmp_path):
    source = _source(oracle)
    partitions = [f"2026-01-{d:02d}" for d in range(1, 31) if d not in (15, 31)]
    pulled = source.pull_window(
        partitions, ["AMOUNT", "STATUS", "REGION"], tmp_path / "ora.csv", key_columns=["ORDER_ID"]
    )
    frame = pd.read_csv(pulled, dtype=object)
    assert len(frame) == 29 * 30 and "2026-01-15" not in set(frame["BUSINESS_DATE"].str[:10])
    csv_frame = pd.read_csv(orders_table / "left/ORDERS.csv", dtype=object)
    csv_frame[csv_frame["BUSINESS_DATE"].isin(partitions)].to_csv(tmp_path / "csv.csv", index=False)
    from_oracle = compute_csv_statistics(pulled, ["ORDER_ID", "AMOUNT", "STATUS", "REGION"])
    from_csv = compute_csv_statistics(
        tmp_path / "csv.csv", ["ORDER_ID", "AMOUNT", "STATUS", "REGION"]
    )
    for column in ("ORDER_ID", "AMOUNT", "STATUS", "REGION"):
        for field in ("n_total", "n_missing", "n_unique", "hash_sum", "min_val", "max_val"):
            assert str(from_oracle[column][field]) == str(from_csv[column][field]), (
                column,
                field,
                from_oracle[column][field],
                from_csv[column][field],
            )
