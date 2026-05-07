"""Tests for SAS script generators (libname / PROC SQL CONNECT scaffolds)."""

import pytest

from dtrack.platforms.sas_test import (
    gen_sas_script,
    hadoop_libname_script,
    hadoop_procsql_script,
    oracle_libname_script,
    oracle_procsql_script,
)


def test_oracle_libname_default_body_has_macros_and_libref():
    sas = oracle_libname_script("svc_a")
    assert "%let oracle_usr" in sas
    assert "%let oracle_pwd" in sas
    assert "libname ora oracle" in sas
    assert "path=svc_a" in sas
    assert "libname ora clear" in sas


def test_oracle_libname_user_body_inserted_between_mount_and_clear():
    body = "proc print data=ora.MYTBL(obs=5); run;"
    sas = oracle_libname_script("svc_a", body=body)
    mount_idx = sas.index("libname ora oracle")
    body_idx = sas.index(body)
    clear_idx = sas.index("libname ora clear")
    assert mount_idx < body_idx < clear_idx


def test_oracle_procsql_wraps_user_body_in_passthrough():
    sas = oracle_procsql_script("svc_a", body="select 1 from dual")
    assert "connect to oracle as ora" in sas
    assert "select * from connection to ora" in sas
    assert "select 1 from dual" in sas
    assert "disconnect from ora" in sas


def test_oracle_procsql_does_not_double_wrap_if_user_already_wrote_passthrough():
    body = "select * from connection to ora (select 1 from dual);"
    sas = oracle_procsql_script("svc_a", body=body)
    # Should appear exactly once.
    assert sas.count("from connection to ora") == 1


def test_hadoop_libname_has_jar_and_config_options_and_kerberos_uri():
    sas = hadoop_libname_script("hd.example.com", "REALM.EX.COM", "myschema",
                                body="proc print data=hdp.t; run;")
    assert "SAS_HADOOP_JAR_PATH" in sas
    assert "SAS_HADOOP_CONFIG_PATH" in sas
    assert "subprotocol=hive2" in sas
    assert "principal=hive/hd.example.com@REALM.EX.COM" in sas
    assert "sslTrustStore=" in sas
    assert "proc print data=hdp.t; run;" in sas


def test_hadoop_procsql_uses_macro_var_connect_pattern():
    sas = hadoop_procsql_script("hd", "R", "s", body="select * from s.t limit 5")
    # User's machine uses the %let hdp_server_osh / hadoop_cro_ded pattern
    # with single-arg connect, /default URI path, and unaliased `from
    # connection to hadoop`.
    assert "%let hdp_server_osh = hd" in sas
    assert "%let hadoop_cro_ded" in sas
    assert "/default;" in sas
    assert "connect to hadoop (&hadoop_cro_ded);" in sas
    assert "from connection to hadoop" in sas
    assert "select * from s.t limit 5" in sas
    assert "disconnect from hadoop;" in sas


def test_dispatch_validates_kind_and_method():
    with pytest.raises(ValueError, match="Unsupported"):
        gen_sas_script("teradata", "libname", svc="x")
    with pytest.raises(ValueError, match="Unsupported"):
        gen_sas_script("oracle", "rest_api", svc="x")


def test_dispatch_requires_oracle_svc():
    with pytest.raises(ValueError, match="svc"):
        gen_sas_script("oracle", "libname")


def test_dispatch_requires_hadoop_server_host_schema():
    with pytest.raises(ValueError, match="server"):
        gen_sas_script("hadoop", "libname", host="h", schema="s")
    with pytest.raises(ValueError, match="host"):
        gen_sas_script("hadoop", "libname", server="srv", schema="s")
    with pytest.raises(ValueError, match="schema"):
        gen_sas_script("hadoop", "libname", server="srv", host="h")


def test_custom_truststore_overrides_default():
    sas = hadoop_libname_script("hd", "R", "s", body="x",
                                truststore="/custom/path.jks")
    assert "/custom/path.jks" in sas
    assert "/sas/bin/Software/Hadoop/OSH/PROD/truststore.jks" not in sas


def test_empty_body_emits_proc_sql_probe_in_libname_mode():
    sas = oracle_libname_script("svc_a", body="")
    # libname empty → wrap a sane `select count(*) from ora.YOUR_TABLE` probe
    # in proc sql so the libref actually gets exercised.
    assert "proc sql;" in sas
    assert "select count(*) from ora.YOUR_TABLE" in sas
    assert "quit;" in sas


def test_empty_body_emits_passthrough_probe_in_procsql_mode():
    # procsql empty → wraps default 'select 1 from dual' as a sanity probe
    sas = oracle_procsql_script("svc_a", body="   ")
    assert "select * from connection to ora" in sas
    assert "select 1 from dual" in sas


def test_libname_select_body_is_wrapped_in_proc_sql():
    sas = oracle_libname_script("svc_a", body="select * from ora.MY_TBL where x > 1")
    assert "proc sql;" in sas
    assert "select * from ora.MY_TBL where x > 1" in sas
    assert "quit;" in sas


def test_libname_proc_print_body_passes_through():
    body = "proc print data=ora.T(obs=10); run;"
    sas = oracle_libname_script("svc_a", body=body)
    # No outer proc sql wrapper around a body that is itself a proc statement.
    assert sas.count("proc sql;") == 0
    assert body in sas


def test_oracle_creds_baked_when_provided():
    sas = oracle_libname_script("svc_a", body="select 1",
                                user="alice", password="secret")
    assert "%let oracle_usr = alice" in sas
    assert "%let oracle_pwd = secret" in sas
    assert "YOUR_USER" not in sas
    assert "YOUR_PASSWORD" not in sas


def test_oracle_creds_placeholder_when_missing():
    sas = oracle_libname_script("svc_a", body="select 1")
    assert "YOUR_USER" in sas
    assert "YOUR_PASSWORD" in sas
