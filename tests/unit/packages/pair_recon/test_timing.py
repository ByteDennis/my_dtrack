from pair_recon.app import timing


def test_observation_seconds_defaults_to_two_minutes():
    assert timing.observation_seconds("true") == 120
    assert timing.observation_seconds("enabled") == 120
    assert timing.observation_seconds("180") == 180


def test_left_pull_timing_keeps_sample_rate_and_prints_scenarios(monkeypatch):
    manifest = {
        "sample": {"mode": "fraction", "rate": 0.01, "threshold": 1},
        "windows": [
            {
                "bucket": f"2026-0{month}-01",
                "partitions": [f"2026-0{month}-01"],
                "n_left": 40_000_000,
            }
            for month in range(1, 5)
        ],
        "columns": ["AMOUNT"],
        "key_cols": ["ID"],
    }

    class Source:
        def __init__(self):
            self.config = {"date_type": "date"}
            self.sample = None

        def health_check(self):
            return {"ok": True}

        def partition_counts(self, _from, _to):
            return {f"2026-0{month}-01": 40_000_000 for month in range(1, 5)}

        def pull_window_sql(self, _partitions, _columns, **options):
            self.sample = options["sample_plan"]
            return "SELECT * FROM T", ["ID", "AMOUNT"]

        def measure_query_rate(self, _sql, **options):
            options["reporter"](3_000, 100.0, 30.0, 3)
            return {
                "rows": 3_000,
                "batches": 3,
                "records_per_second": 30.0,
                "observed_seconds": 100.0,
                "execute_seconds": 10.0,
                "first_batch_seconds": 30.0,
                "first_batch_rows": 1_000,
                "exhausted": False,
            }

    source = Source()
    monkeypatch.setattr(timing, "validate_config", lambda value: value)
    monkeypatch.setattr(timing, "public_pair_config", lambda value: value)
    monkeypatch.setattr(timing, "key_family_specs", lambda _pair, _keys: ["string"])
    monkeypatch.setattr(timing, "build_manifest", lambda *_args: manifest)
    output = []

    def printer(*values, **_options):
        output.append(" ".join(str(value) for value in values))

    report = timing.left_pull_timing(
        {"run_id": "orders-me-20261030"},
        {
            "pairs": {
                "orders": {
                    "partition": "day",
                    "fromDate": "2026-01-01",
                    "toDate": "2026-04-30",
                    "key": "ID",
                    "left": {"source": "oracle", "table": "T", "date_col": "DT"},
                    "right": {"source": "databricks", "table": "R", "date_col": "dt"},
                }
            }
        },
        timing_value="120",
        source_factory=lambda _config: source,
        printer=printer,
    )

    assert source.sample["rate"] == 0.01
    assert report["vintage_sample_size"] == 4
    assert report["estimated_pull_rows"] == 1_600_000
    assert report["records_per_second"] == 30.0
    assert report["execute_seconds"] == 10.0
    assert report["first_batch_seconds"] == 30.0
    assert any(
        row["vintages"] == 4 and row["sample_rate"] == 0.0025 and row["estimated_rows"] == 400_000
        for row in report["scenarios"]
    )
    rendered = "\n".join(output)
    assert "Timing | 30.0 rec/s | exec 10.0s | first 30.0s" in rendered
    assert "4 vintages | sample 1% | pull ~1.6M" in rendered
    assert "Options | 1v: 1%=400K/" in rendered
    assert "4v: 1%=1.6M/" in rendered
    assert rendered.rstrip().endswith("; approximate")
