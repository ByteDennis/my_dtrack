import json

import boto3
import duckdb
from support import pairs_config

from dtrack_local import pipeline
from dtrack_right.backends.targets import CsvAnchorTarget


# >>> the full local plan on the e2e synthetic files: CSV anchor as the Right side, report and backup to S3 <<< #
def test_run_all_matches_the_distributed_protocol_and_backs_up(synthetic, tmp_path, s3_bucket):
    def anchor(config):
        return CsvAnchorTarget(config, synthetic.root / "right", tmp_path / "rw")

    def open_local():
        return pipeline.open_run(
            pairs_config(synthetic.left), run_id="r1", out=tmp_path / "runs", target_factory=anchor
        )

    outcome = pipeline.run_all(
        open_local(),
        report_s3=f"s3://{s3_bucket}/report",
        backup_s3=f"s3://{s3_bucket}/runs",
    )
    assert outcome["status"] == "DONE" and outcome["findings"] == {
        "row_count_diff": 1,
        "partition_missing": 2,
        "col_stat_diff": 2,
    }
    assert outcome["row_check"]["orders"]["summary"] == {
        "match": 56,
        "count_diff": 1,
        "left_only": 1,
        "right_only": 1,
    }
    assert outcome["col_check"]["compared"] == 11 and outcome["col_check"]["dead"] == 0
    assert set(outcome["landing"]) == {"left__orders", "left__customers"}
    keys = {row["Key"] for row in boto3.client("s3").list_objects_v2(Bucket=s3_bucket)["Contents"]}
    assert "runs/r1/row_check/orders.json" in keys and "runs/r1/report/index.html" in keys
    assert "report/latest/pointer.json" in keys
    assert not any(key.startswith("runs/r1/_local/") for key in keys)

    again = pipeline.run_all(open_local())
    assert again["findings"] == outcome["findings"] and again["landing"] == outcome["landing"]
    assert open_local().status()["state"] == "complete"


class FakeWarehouse:
    def __init__(self, rows):
        self.sql = []
        self.rows_out = rows

    def to_csv(self, sql, output, columns):
        from dtrack_left.backends.sources import write_rows

        self.sql.append(sql)
        write_rows(output, columns, self.rows_out)
        return len(self.rows_out)


MANIFEST = {
    "pair": "orders",
    "columns": ["AMOUNT", "STATUS"],
    "col_map": {"AMOUNT": "amount", "STATUS": "status"},
    "right_key_cols": ["order_id"],
    "key_families": ["numeric"],
    "sample": {"mode": "all"},
    "strict": False,
    "col_type_overrides": {},
}


def test_warehouse_target_pulls_text_columns_and_computes_statistics_locally(tmp_path):
    rows = [("2026-01-01", "1", "10.50", "OPEN"), ("2026-01-02", "2", "3", None)]
    warehouse = FakeWarehouse(rows)
    landing = pipeline.Landing(tmp_path / "data.duckdb")
    config = {"catalog": "c", "schema": "s", "table": "orders", "date_col": "business_date"}
    target = pipeline.WarehouseTarget(warehouse, config, tmp_path / "w", landing=landing)
    window = {"bucket": "2026-01-01", "partitions": ["2026-01-01", "2026-01-02"]}
    stats = target.window_statistics(MANIFEST, window)
    assert set(stats) == {"amount", "status"} and stats["amount"]["n_total"] == 2
    assert stats["status"]["n_missing"] == 1
    sql = warehouse.sql[0]
    assert sql.startswith(
        "SELECT CAST(`business_date` AS STRING) AS `business_date`, CAST(`order_id`"
    )
    assert "FROM `c`.`s`.`orders` WHERE" in sql and "md5" not in sql
    assert landing.tables() == {"right__orders": 2}
    target.window_statistics(MANIFEST, window)
    assert landing.tables() == {"right__orders": 2}


def test_pushed_sample_filters_in_the_warehouse(tmp_path):
    warehouse = FakeWarehouse([])
    config = {"table": "orders", "date_col": "dt"}
    target = pipeline.WarehouseTarget(warehouse, config, tmp_path, push_sample=True)
    manifest = {**MANIFEST, "sample": {"mode": "fraction", "threshold": 100, "rate": 0.1}}
    stats = target.window_statistics(manifest, {"bucket": "b", "partitions": ["2026-01-01"]})
    assert "md5(" in warehouse.sql[0] and stats["amount"]["n_total"] == 0


def test_landing_keeps_raw_text_per_bucket(tmp_path):
    csv = tmp_path / "w.csv"
    csv.write_text("ID,AMOUNT\n007,1.50\n")
    landing = pipeline.Landing(tmp_path / "d.duckdb")
    assert landing.land("left", "p", "2026-01-01", csv) == 1
    assert landing.land("left", "p", "2026-02-01", csv) == 1
    with duckdb.connect(str(tmp_path / "d.duckdb"), read_only=True) as connection:
        rows = connection.execute('SELECT * FROM "left__p" ORDER BY dtrack_bucket').fetchall()
    assert rows == [("007", "1.50", "2026-01-01"), ("007", "1.50", "2026-02-01")]
    assert json.loads(json.dumps(landing.tables())) == {"left__p": 2}


def test_cli_runs_the_plan_and_preflight_reports_failures(synthetic, tmp_path, monkeypatch, capsys):
    from dtrack_local.cli import main

    config = tmp_path / "c.json"
    config.write_text(json.dumps(pairs_config(synthetic.left)))
    opened = []

    def open_run(path, **kwargs):
        opened.append(kwargs)
        right = synthetic.root / "right"
        return pipeline.LocalRun(
            pipeline.load_config(path),
            kwargs["run_id"] or "cli",
            tmp_path / "runs" / (kwargs["run_id"] or "cli"),
            lambda spec: CsvAnchorTarget(spec, right, tmp_path / "rw"),
            pipeline.Landing(tmp_path / "d.duckdb"),
            kwargs["pairs"],
        )

    monkeypatch.setattr(pipeline, "open_run", open_run)
    assert (
        main(["run", "--config", str(config), "--pair", "orders", "--disposition", "inline"]) == 0
    )
    outcome = json.loads(capsys.readouterr().out)
    assert outcome["status"] == "DONE" and outcome["findings"]["row_count_diff"] == 1
    assert opened[0]["pairs"] == ["orders"] and opened[0]["disposition"] == "inline"
    (synthetic.root / "right" / "orders.csv").rename(tmp_path / "moved.csv")
    try:
        assert main(["preflight", "--config", str(config), "--run-id", "p"]) == 1
    finally:
        (tmp_path / "moved.csv").rename(synthetic.root / "right" / "orders.csv")
    assert json.loads(capsys.readouterr().out)["orders"]["right"]["ok"] is False
