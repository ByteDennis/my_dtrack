"""Tests for the SAS test-connect script generators."""

import pytest

from dtrack.platforms.sas_test import (
    gen_test_connect,
    hadoop_libname_test,
    hadoop_procsql_test,
    oracle_libname_test,
    oracle_procsql_test,
)


def test_oracle_libname_includes_credentials_macros_and_table():
    sas = oracle_libname_test("svc_a", "S.T")
    assert "%let oracle_usr" in sas
    assert "%let oracle_pwd" in sas
    assert "libname ora oracle" in sas
    assert "&svc" in sas
    assert "svc_a" in sas
    assert "S.T" in sas
    assert "libname ora clear" in sas


def test_oracle_procsql_uses_connect_to_oracle():
    sas = oracle_procsql_test("svc_a", "S.T")
    assert "connect to oracle as ora" in sas
    assert "rownum <= 10" in sas
    assert "disconnect from ora" in sas


def test_hadoop_libname_has_jar_and_config_options_and_kerberos_uri():
    sas = hadoop_libname_test("hd.example.com", "REALM.EX.COM", "myschema", "myschema.t")
    assert "SAS_HADOOP_JAR_PATH" in sas
    assert "SAS_HADOOP_CONFIG_PATH" in sas
    assert "subprotocol=hive2" in sas
    assert "principal=hive/&hdp_server@&hdp_host" in sas
    assert "sslTrustStore=" in sas


def test_hadoop_procsql_uses_connect_to_hadoop_with_limit_10():
    sas = hadoop_procsql_test("hd", "R", "s", "s.t")
    assert "connect to hadoop as hdp" in sas
    assert "limit 10" in sas


def test_dispatch_validates_kind_and_method():
    with pytest.raises(ValueError, match="Unsupported"):
        gen_test_connect("teradata", "libname", svc="x", table="t")
    with pytest.raises(ValueError, match="Unsupported"):
        gen_test_connect("oracle", "rest_api", svc="x", table="t")


def test_dispatch_requires_oracle_svc():
    with pytest.raises(ValueError, match="svc"):
        gen_test_connect("oracle", "libname", table="t")


def test_dispatch_requires_hadoop_server_host_schema():
    with pytest.raises(ValueError, match="server"):
        gen_test_connect("hadoop", "libname", table="t", host="h", schema="s")
    with pytest.raises(ValueError, match="host"):
        gen_test_connect("hadoop", "libname", table="t", server="srv", schema="s")
    with pytest.raises(ValueError, match="schema"):
        gen_test_connect("hadoop", "libname", table="t", server="srv", host="h")


def test_custom_truststore_overrides_default():
    sas = hadoop_libname_test("hd", "R", "s", "s.t",
                              truststore="/custom/path.jks")
    assert "/custom/path.jks" in sas
    assert "/sas/bin/Software/Hadoop/OSH/PROD/truststore.jks" not in sas
