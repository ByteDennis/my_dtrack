import pandas as pd
import pytest
from support import load_oracle_tables, oracle_container, synthetic_data

from dtrack_left.backends.sources import OracleSource
from dtrack_left.common.stats import compute_csv_statistics

pytestmark = [pytest.mark.oracle, pytest.mark.docker]


@pytest.fixture(scope="module")
def oracle():
    pytest.importorskip("testcontainers")
    yield from oracle_container()


@pytest.fixture(scope="module")
def left_dir(oracle, tmp_path_factory):
    root = tmp_path_factory.mktemp("synthetic")
    synthetic_data(root)
    load_oracle_tables(oracle, root / "left", tables=("ORDERS",))
    return root / "left"


def _source(oracle):
    config = {"source": "oracle", "table": "ORDERS", "date_col": "BUSINESS_DATE"}
    return OracleSource({**config, "date_type": "date", **oracle})


# >>> the left-side SQL counts a real Oracle table exactly, day by day <<< #
def test_oracle_partition_counts_match_the_csv_truth(oracle, left_dir):
    source = _source(oracle)
    assert source.preflight()["ok"]
    counts = source.partition_counts("2026-01-01", "2026-02-28")
    truth = pd.read_csv(left_dir / "ORDERS.csv", dtype=object)["BUSINESS_DATE"].value_counts()
    assert counts == {k: int(v) for k, v in truth.items()}
    assert len(counts) == 58 and "2026-01-31" not in counts


# >>> a window pull with holes returns exactly the admitted days, and its statistics equal the CSV's <<< #
def test_oracle_window_pull_honours_holes_and_statistics_match_csv(oracle, left_dir, tmp_path):
    partitions = [f"2026-01-{d:02d}" for d in range(1, 31) if d not in (15, 31)]
    columns = ["ORDER_ID", "AMOUNT", "STATUS", "REGION"]
    pulled = _source(oracle).pull_window(
        partitions, columns[1:], tmp_path / "ora.csv", key_columns=["ORDER_ID"]
    )
    frame = pd.read_csv(pulled, dtype=object)
    assert len(frame) == 29 * 30 and "2026-01-15" not in set(frame["BUSINESS_DATE"].str[:10])
    csv_frame = pd.read_csv(left_dir / "ORDERS.csv", dtype=object)
    csv_frame[csv_frame["BUSINESS_DATE"].isin(partitions)].to_csv(tmp_path / "csv.csv", index=False)
    from_oracle = compute_csv_statistics(pulled, columns)
    from_csv = compute_csv_statistics(tmp_path / "csv.csv", columns)
    for column in columns:
        for field in ("n_total", "n_missing", "n_unique", "hash_sum", "min_val", "max_val"):
            assert str(from_oracle[column][field]) == str(from_csv[column][field]), (column, field)


# >>> DATE values carrying a time are counted and pulled by calendar day, the last bounded day included <<< #
def test_oracle_dates_with_times_count_and_pull_by_calendar_day(oracle, tmp_path):
    source = OracleSource(
        {"source": "oracle", "table": "TIMED", "date_col": "APP_DT", "date_type": "date", **oracle}
    )
    connection = source.connect()
    cursor = connection.cursor()
    cursor.execute(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE TIMED'; EXCEPTION WHEN OTHERS THEN NULL; END;"
    )
    cursor.execute("CREATE TABLE TIMED (ID NUMBER, APP_DT DATE)")
    rows = [
        (1, "2022-09-05 00:00:00"),
        (2, "2022-09-06 00:00:00"),
        (3, "2022-09-06 08:15:00"),
        (4, "2022-09-06 23:59:59"),
        (5, "2022-09-07 12:00:00"),
    ]
    cursor.executemany("INSERT INTO TIMED VALUES (:1, TO_DATE(:2, 'YYYY-MM-DD HH24:MI:SS'))", rows)
    connection.commit()
    connection.close()
    assert source.partition_counts("2022-09-05", "2022-09-06") == {"2022-09-05": 1, "2022-09-06": 3}
    pulled = source.pull_window(["2022-09-06"], ["ID"], tmp_path / "w.csv")
    assert sorted(pd.read_csv(pulled)["ID"]) == [2, 3, 4]
