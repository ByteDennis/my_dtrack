import pytest

from dtrack_left.common import duck


def test_size_text_parses_binary_units():
    assert duck.parse_size("16GB") == 16 * 2**30
    assert duck.parse_size("512 MiB") == 512 * 2**20
    assert duck.parse_size("2.5G") == int(2.5 * 2**30)
    assert duck.parse_size("1024") == 1024


def test_budget_is_split_across_the_expected_connections(monkeypatch):
    monkeypatch.setenv("DTRACK_DUCKDB_MEMORY", "16GB")
    monkeypatch.setenv("DTRACK_DUCKDB_SHARE", "4")
    monkeypatch.setenv("DTRACK_DUCKDB_THREADS", "3")
    applied = duck.settings()
    assert applied["memory_limit"] == f"{4 * 1024}MiB"
    assert applied["threads"] == "3"
    assert applied["temp_directory"].endswith("dtrack-duckdb")


def test_default_budget_is_a_fraction_of_physical_memory(monkeypatch):
    monkeypatch.delenv("DTRACK_DUCKDB_MEMORY", raising=False)
    monkeypatch.delenv("DTRACK_DUCKDB_SHARE", raising=False)
    monkeypatch.setattr(duck, "physical_memory", lambda: 32 * 2**30)
    assert duck.settings()["memory_limit"] == f"{int(32 * 1024 * 0.6 / 4)}MiB"


def test_connect_applies_the_budget_and_spills_to_the_temp_directory(monkeypatch, tmp_path):
    pytest.importorskip("duckdb")
    monkeypatch.setenv("DTRACK_DUCKDB_MEMORY", "2GB")
    monkeypatch.setenv("DTRACK_DUCKDB_SHARE", "2")
    monkeypatch.setenv("DTRACK_DUCKDB_TEMP", str(tmp_path / "spill"))
    with duck.connect(ordered=False) as connection:
        setting = lambda name: connection.execute(f"SELECT current_setting('{name}')").fetchone()[0]
        assert setting("memory_limit").replace(" ", "") == "1.0GiB"
        assert setting("temp_directory") == str(tmp_path / "spill")
        assert setting("preserve_insertion_order") is False
    assert (tmp_path / "spill").is_dir()


def test_temp_prefers_env_then_the_registered_work_dir_then_system_temp(monkeypatch, tmp_path):
    monkeypatch.delenv("DTRACK_DUCKDB_TEMP", raising=False)
    duck.use_temp(None)
    assert duck.temp_directory().endswith("dtrack-duckdb")
    duck.use_temp(tmp_path / "work" / "duckdb")
    assert duck.temp_directory() == str(tmp_path / "work" / "duckdb")
    monkeypatch.setenv("DTRACK_DUCKDB_TEMP", str(tmp_path / "env"))
    assert duck.temp_directory() == str(tmp_path / "env")
    duck.use_temp(None)


def test_unwritable_temp_falls_back_to_the_system_temp(monkeypatch, tmp_path, caplog):
    pytest.importorskip("duckdb")
    blocker = tmp_path / "file"
    blocker.write_text("x")
    monkeypatch.setenv("DTRACK_DUCKDB_TEMP", str(blocker / "nested"))
    with caplog.at_level("WARNING", logger="dtrack.duck"), duck.connect() as connection:
        setting = connection.execute("SELECT current_setting('temp_directory')").fetchone()[0]
    assert setting.endswith("dtrack-duckdb")
    assert "unusable" in caplog.text
