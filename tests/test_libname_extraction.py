"""End-to-end test for connect_method=libname row extraction.

Drives gen_sas with a minimal in-memory config and asserts the generated
extract_row.sas:
  - mounts a `libname pb23 oracle ...` at the top
  - emits row datalines via rows_sas.sas (SAS-syntax) for the libname table
  - does NOT emit the libname table in the rows_oracle.sas (PROC SQL CONNECT) block
  - clears the libname at the end of the runner
"""

import json
import os
import tempfile

import pytest


def _write_minimal_config(path, *, connect_method):
    """One pair, left side = Oracle libname-mode (or sql), right side = unused.

    Right side is set to source=sas to avoid pulling in another connect path
    that would mask the libname-mode behaviour we're checking.
    """
    cfg = {
        "pairs": {
            "demo": {
                "left": {
                    "name": "src",
                    "table": "MYTBL",
                    "source": "oracle",
                    "conn_macro": "pb23",
                    "date_col": "DT",
                    "date_type": "date",
                    "connect_method": connect_method,
                },
                "right": {
                    "name": "snk",
                    "table": "snk_ds",
                    "source": "sas",
                    "conn_macro": "work",
                    "date_col": "DT",
                    "date_type": "date",
                },
                "fromDate": "2025-01-01",
                "toDate":   "2025-01-31",
            }
        },
        "metadata": {},
    }
    with open(path, "w") as f:
        json.dump(cfg, f)


@pytest.fixture
def isolated_env(monkeypatch):
    """Set the env vars gen_sas demands and silence credential lookups."""
    monkeypatch.setenv("PCDS_USR", "alice")
    monkeypatch.setenv("PB23_PWD", "topsecret")
    monkeypatch.setenv("EMAIL_TO", "test@example.com")
    monkeypatch.setenv("SAS_LIB", "/tmp/saslib")
    monkeypatch.setenv("OUT_DIR", "/tmp/saslib")
    monkeypatch.setenv("SEED", "42")
    yield


def test_libname_mode_emits_libname_mount_and_clear(isolated_env, tmp_path):
    cfg_path = tmp_path / "cfg.json"
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    _write_minimal_config(cfg_path, connect_method="libname")

    from dtrack.platforms.oracle import gen_sas
    gen_sas(str(cfg_path), str(out_dir), types=["row"])

    # gen_sas writes extract_row.sas (or extract.sas if both row+col).
    sas_files = sorted(out_dir.glob("*.sas"))
    assert sas_files, "gen_sas produced no .sas files"
    sas = sas_files[0].read_text()

    # 1. LIBNAME mount appears in the conn_macros block at the top.
    assert "/* SAS LIBNAME mounts (connect_method=libname) */" in sas
    assert 'libname pb23 oracle user="&iamusr" orapw="&pb23_pwd"' in sas
    assert 'path="@' in sas

    # 2. The libname table appears in the SAS-syntax `_sas_map` datalines
    #    block (rows_sas.sas template), NOT in the rows_oracle.sas template.
    #    Format: `<libref>.<table>|<dsname>|<qname>|<date_expr>|<where>`
    assert "pb23.MYTBL|" in sas
    # The rows_oracle.sas template, when active, defines `data _ora_map`.
    # With our only Oracle-source table in libname mode, that block is absent.
    assert "data _ora_map" not in sas

    # 3. Libname clear runs at the end of the runner.
    assert "libname pb23 clear;" in sas


def test_sql_mode_keeps_proc_sql_connect_path(isolated_env, tmp_path):
    """connect_method='sql' (default) → existing behavior preserved."""
    cfg_path = tmp_path / "cfg.json"
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    _write_minimal_config(cfg_path, connect_method="sql")

    from dtrack.platforms.oracle import gen_sas
    gen_sas(str(cfg_path), str(out_dir), types=["row"])

    sas_files = sorted(out_dir.glob("*.sas"))
    sas = sas_files[0].read_text()

    # No libname mount or clear when no table opted in.
    assert "/* SAS LIBNAME mounts" not in sas
    assert "libname pb23 clear;" not in sas
    # The PROC SQL CONNECT macro is still defined for pb23.
    assert "%macro pb23;" in sas
    assert 'connect to oracle(user="&iamusr" orapw=' in sas


def test_no_connect_method_field_defaults_to_sql(isolated_env, tmp_path):
    """A pair without `connect_method` keeps the existing PROC SQL CONNECT path."""
    cfg = {
        "pairs": {
            "demo": {
                "left":  {"name": "src", "table": "T", "source": "oracle",
                          "conn_macro": "pb23", "date_col": "DT", "date_type": "date"},
                "right": {"name": "snk", "table": "snk", "source": "sas",
                          "conn_macro": "work", "date_col": "DT", "date_type": "date"},
                "fromDate": "2025-01-01", "toDate": "2025-01-31",
            }
        },
        "metadata": {},
    }
    cfg_path = tmp_path / "cfg.json"
    cfg_path.write_text(json.dumps(cfg))
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    from dtrack.platforms.oracle import gen_sas
    gen_sas(str(cfg_path), str(out_dir), types=["row"])
    sas = sorted(out_dir.glob("*.sas"))[0].read_text()
    assert "/* SAS LIBNAME mounts" not in sas


# ---------------------------------------------------------------------------
# Col extraction libname mode
# ---------------------------------------------------------------------------

def _write_col_config(path, *, connect_method, columns):
    """One pair, left=oracle (with connect_method), right=sas (so col gen
    actually runs against a column list)."""
    cfg = {
        "pairs": {
            "demo": {
                "left": {
                    "name": "src", "table": "MYTBL", "source": "oracle",
                    "conn_macro": "pb23", "date_col": "DT", "date_type": "date",
                    "columns": columns,
                    "connect_method": connect_method,
                },
                "right": {
                    "name": "snk", "table": "snk_ds", "source": "sas",
                    "conn_macro": "work", "date_col": "DT", "date_type": "date",
                    "columns": {"AMT": "NUMBER"},
                },
                "fromDate": "2025-01-01", "toDate": "2025-01-31",
            }
        },
        "metadata": {},
    }
    with open(path, "w") as f:
        json.dump(cfg, f)


def test_libname_col_uses_libname_helper_macro(isolated_env, tmp_path):
    """connect_method=libname + types=col → emits the libname helper macro
    and routes the table through %_col_libname (not %_pull_one_col, not
    `connect to oracle`)."""
    cfg_path = tmp_path / "cfg.json"
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    _write_col_config(cfg_path, connect_method="libname",
                      columns={"AMT": "NUMBER", "STATUS": "VARCHAR2(10)"})

    from dtrack.platforms.oracle import gen_sas
    gen_sas(str(cfg_path), str(out_dir), types=["col"])

    sas = sorted(out_dir.glob("*.sas"))[0].read_text()

    # Helper macro is emitted.
    assert "%macro _col_libname" in sas
    # Driver dataset for libname mode exists.
    assert "data _driver_col_lib_;" in sas
    # Dispatcher invokes the libname helper (not _pull_one_col).
    assert "%nrstr(%_col_libname)()" in sas
    # Libname mount is at the top of the script.
    assert 'libname pb23 oracle user="&iamusr" orapw="&pb23_pwd"' in sas
    # The libname-mode oracle table is NOT in column_oracle.sas's bucket.
    # Key signature of column_oracle.sas: `connect to oracle` inside col
    # extraction (the static %MACRO pull_data also has it, but the col
    # extraction section is identifiable by the data driver name).
    assert "data _ora_col_map;" not in sas


def test_libname_col_default_sql_keeps_existing_oracle_path(isolated_env, tmp_path):
    """Without connect_method=libname, the col extraction goes through the
    existing column_oracle.sas (PROC SQL CONNECT) path."""
    cfg_path = tmp_path / "cfg.json"
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    _write_col_config(cfg_path, connect_method="sql",
                      columns={"AMT": "NUMBER"})

    from dtrack.platforms.oracle import gen_sas
    gen_sas(str(cfg_path), str(out_dir), types=["col"])

    sas = sorted(out_dir.glob("*.sas"))[0].read_text()

    # Libname helpers should NOT be emitted for the sql path.
    assert "%macro _col_libname" not in sas
    assert "data _driver_col_lib_;" not in sas
    # The Oracle column-extraction driver IS present.
    assert "data _ora_col_map;" in sas


def test_libname_col_driver_uses_libref_qualified_table_ref(isolated_env, tmp_path):
    """Driver dataline carries `<conn_macro>.<table>` — that's how
    %_col_libname's `&_sas_table` resolves to the libref-mounted dataset."""
    cfg_path = tmp_path / "cfg.json"
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    _write_col_config(cfg_path, connect_method="libname",
                      columns={"AMT": "NUMBER"})

    from dtrack.platforms.oracle import gen_sas
    gen_sas(str(cfg_path), str(out_dir), types=["col"])

    sas = sorted(out_dir.glob("*.sas"))[0].read_text()
    # Driver dataline assigns sas_table='pb23.MYTBL'.
    assert "sas_table='pb23.MYTBL'" in sas


def test_libname_col_categorical_uses_freq_post_processor(isolated_env, tmp_path):
    """Categorical column → emits the freq-pull SQL and routes to the
    shared %_col_categorical_freq post-processor."""
    cfg_path = tmp_path / "cfg.json"
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    _write_col_config(cfg_path, connect_method="libname",
                      columns={"STATUS": "VARCHAR2(10)"})

    from dtrack.platforms.oracle import gen_sas
    gen_sas(str(cfg_path), str(out_dir), types=["col"])

    sas = sorted(out_dir.glob("*.sas"))[0].read_text()
    # The categorical branch of %_col_libname pulls a freq table.
    assert "_lib_freq_" in sas
    # Re-uses the shared post-processor (defined once in _SAS_EXPORT_HELPER).
    assert "%_col_categorical_freq" in sas
