from support import pairs_config

from dtrack_local.csv_target import CsvAnchorTarget


def _manifest(sample):
    return {
        "pair": "orders",
        "columns": ["AMOUNT"],
        "col_map": {"AMOUNT": "amount"},
        "right_key_cols": ["order_id"],
        "key_families": ["numeric"],
        "sample": sample,
        "strict": False,
    }


def test_counts(synthetic, tmp_path):
    config = pairs_config(synthetic.left)["pairs"]["orders"]["right"]
    target = CsvAnchorTarget(config, synthetic.right, tmp_path)
    assert target.preflight()["ok"]
    counts = target.partition_counts("2026-01-01", "2026-01-31")
    assert counts["2026-01-15"] == 27 and max(counts) <= "2026-01-31"
    manifest = _manifest({"mode": "fraction", "rate": 0.25})
    stats = target.window_statistics(
        manifest, {"bucket": "2026-01-01", "partitions": ["2026-01-02"]}
    )
    assert 0 < stats["amount"]["n_total"] < 30
    manifest["col_type_overrides"] = {"AMOUNT": "categorical"}
    stats = target.window_statistics(
        manifest, {"bucket": "2026-01-01", "partitions": ["2026-01-02"]}
    )
    assert stats["amount"]["col_type"] == "categorical"


def test_preflight(tmp_path):
    missing = CsvAnchorTarget({"table": "t", "date_col": "d"}, tmp_path, tmp_path)
    assert missing.preflight()["ok"] is False
    (tmp_path / "t.csv").write_text("x\n1\n")
    assert "missing column d" in missing.preflight()["error"]


def test_all_partition(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "t.csv").write_text("id,amount\n1,10\n2,20\n", encoding="utf-8")
    work = tmp_path / "work"
    target = CsvAnchorTarget({"table": "t", "partition_mode": "all"}, source, work)
    assert target.preflight()["ok"]
    assert target.partition_counts() == {"all": 2}
    manifest = {
        "pair": "t",
        "partition_mode": "all",
        "columns": ["AMOUNT"],
        "col_map": {"AMOUNT": "amount"},
        "right_key_cols": ["id"],
        "key_families": ["numeric"],
        "sample": {"mode": "all"},
        "strict": False,
    }
    stats = target.window_statistics(manifest, {"bucket": "all", "partitions": ["all"]})
    assert stats["amount"]["n_total"] == 2
    assert (work / "t" / "all" / "right.csv").read_text(encoding="utf-8").splitlines() == [
        "id,amount",
        "1,10",
        "2,20",
    ]


def test_date_type(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "t.csv").write_text("d,id,v\n24108.75,1,a\n24109.75,2,b\n")
    work = tmp_path / "work"
    target = CsvAnchorTarget(
        {"table": "t", "date_col": "d", "date_type": {"type": "sas"}}, source, work
    )
    assert target.partition_counts("2026-01-02", "2026-01-02") == {"2026-01-02": 1}
    manifest = {
        "pair": "t",
        "columns": ["V"],
        "col_map": {"V": "v"},
        "right_key_cols": ["id"],
        "key_families": ["numeric"],
        "sample": {"mode": "all"},
        "strict": False,
    }
    target.window_statistics(manifest, {"bucket": "2026-01-02", "partitions": ["2026-01-02"]})
    written = (work / "t" / "2026-01-02" / "right.csv").read_text()
    assert "24108.75,1,a" in written
    assert "24109.75,2,b" not in written
