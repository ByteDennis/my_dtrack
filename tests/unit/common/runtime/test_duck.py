import pytest

from pair_recon_left.common.runtime import duck


def test_size():
    assert duck.parse_size("16GB") == 16 * 2**30
    assert duck.parse_size("512 MiB") == 512 * 2**20
    assert duck.parse_size("2.5G") == int(2.5 * 2**30)
    assert duck.parse_size("1024") == 1024


def test_budget(monkeypatch):
    monkeypatch.setenv("PAIR_RECON_DUCKDB_MEMORY", "16GB")
    monkeypatch.setenv("STATS_WORKERS", "4")
    monkeypatch.setattr(duck.os, "cpu_count", lambda: 12)
    applied = duck.settings()
    assert applied["memory_limit"] == f"{4 * 1024}MiB"
    assert applied["threads"] == "3"
    assert applied["temp_directory"].endswith("pair_recon-duckdb")


def test_default(monkeypatch):
    monkeypatch.delenv("PAIR_RECON_DUCKDB_MEMORY", raising=False)
    monkeypatch.delenv("STATS_WORKERS", raising=False)
    monkeypatch.setattr(duck, "physical_memory", lambda: 32 * 2**30)
    assert duck.settings()["memory_limit"] == f"{int(32 * 1024 * 0.6 / 2)}MiB"


def test_connect(monkeypatch, tmp_path):
    pytest.importorskip("duckdb")
    monkeypatch.setenv("PAIR_RECON_DUCKDB_MEMORY", "2GB")
    monkeypatch.setenv("STATS_WORKERS", "2")
    monkeypatch.setenv("PAIR_RECON_DUCKDB_TEMP", str(tmp_path / "spill"))
    with duck.connect(ordered=False) as connection:
        setting = lambda name: connection.execute(f"SELECT current_setting('{name}')").fetchone()[0]
        assert setting("memory_limit").replace(" ", "") == "1.0GiB"
        assert setting("temp_directory") == str(tmp_path / "spill")
        assert setting("preserve_insertion_order") is False
    assert (tmp_path / "spill").is_dir()


def test_temp(monkeypatch, tmp_path):
    monkeypatch.delenv("PAIR_RECON_DUCKDB_TEMP", raising=False)
    duck.use_temp(None)
    assert duck.temp_directory().endswith("pair_recon-duckdb")
    duck.use_temp(tmp_path / "work" / "duckdb")
    assert duck.temp_directory() == str(tmp_path / "work" / "duckdb")
    monkeypatch.setenv("PAIR_RECON_DUCKDB_TEMP", str(tmp_path / "env"))
    assert duck.temp_directory() == str(tmp_path / "env")
    duck.use_temp(None)


def test_temp2(monkeypatch, tmp_path, caplog):
    pytest.importorskip("duckdb")
    blocker = tmp_path / "file"
    blocker.write_text("x")
    monkeypatch.setenv("PAIR_RECON_DUCKDB_TEMP", str(blocker / "nested"))
    with caplog.at_level("WARNING", logger="pair_recon.left.duck"), duck.connect() as connection:
        setting = connection.execute("SELECT current_setting('temp_directory')").fetchone()[0]
    assert setting.endswith("pair_recon-duckdb")
    assert "unusable" in caplog.text
