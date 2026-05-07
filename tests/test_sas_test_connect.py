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


def test_hadoop_procsql_wraps_user_body():
    sas = hadoop_procsql_script("hd", "R", "s", body="select * from s.t limit 5")
    assert "connect to hadoop as hdp" in sas
    assert "from connection to hdp" in sas
    assert "select * from s.t limit 5" in sas
    assert "disconnect from hdp" in sas


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


def test_empty_body_falls_back_to_placeholder_or_default():
    sas = oracle_libname_script("svc_a", body="")
    assert "/* Type SAS here." in sas
    # procsql empty → wraps default 'select 1 from dual' as a sanity probe
    sas = oracle_procsql_script("svc_a", body="   ")
    assert "select * from connection to ora" in sas
    assert "select 1 from dual" in sas
