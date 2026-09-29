import os
import uuid
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from support import load_oracle_tables, oracle_container, synthetic_data

LIVE_ENV = Path(__file__).with_name("live.env")
RIGHT_SCHEMAS = {
    "orders": "business_date STRING, order_id BIGINT, amount DECIMAL(10,2), status STRING, "
    "region STRING",
    "customers": "snapshot_dt STRING, cust_id BIGINT, tier STRING, balance DECIMAL(12,2)",
}


# >>> tests/live/live.env loaded once, only when a live test actually runs <<< #
@pytest.fixture(scope="session")
def live_env():
    from dotenv import load_dotenv

    load_dotenv(LIVE_ENV, override=False)
    return os.environ


# >>> the named settings, or skip the test and say which ones are missing <<< #
@pytest.fixture(scope="session")
def need(live_env):
    def lookup(*names):
        missing = [name for name in names if not live_env.get(name)]
        if missing:
            pytest.skip(f"set {', '.join(missing)} in {LIVE_ENV}")
        values = [live_env[name].rstrip("/") for name in names]
        return values[0] if len(values) == 1 else values

    return lookup


@pytest.fixture(scope="session")
def keep(live_env):
    return live_env.get("DTRACK_LIVE_KEEP") == "1"


# >>> one id per session: names the S3 prefixes, tables, run and workspace folder of this run <<< #
@pytest.fixture(scope="session")
def tag():
    return f"live-{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:6]}"


@pytest.fixture(scope="session")
def synthetic(tmp_path_factory):
    root = tmp_path_factory.mktemp("synthetic")
    summary = synthetic_data(root)
    return SimpleNamespace(root=root, left=root / "left", right=root / "right", summary=summary)


# >>> delete every object under an s3:// prefix <<< #
def delete_prefix(uri: str) -> int:
    import boto3

    bucket, _, prefix = uri.removeprefix("s3://").partition("/")
    client = boto3.client("s3")
    deleted = 0
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        keys = [{"Key": row["Key"]} for row in page.get("Contents") or []]
        if keys:
            client.delete_objects(Bucket=bucket, Delete={"Objects": keys})
            deleted += len(keys)
    return deleted


# >>> a fresh scratch prefix on real S3, removed afterwards unless DTRACK_LIVE_KEEP=1 <<< #
@pytest.fixture(scope="session")
def s3_scratch(need, tag, keep):
    uri = f"{need('DTRACK_LIVE_S3')}/{tag}"
    yield uri
    if not keep:
        delete_prefix(uri)


@pytest.fixture(scope="session")
def workspace(need):
    need("DATABRICKS_CONFIG_PROFILE")
    sdk = pytest.importorskip("databricks.sdk", reason="uv sync --group live (Python 3.12)")
    client = sdk.WorkspaceClient()
    client.current_user.me()
    return client


# >>> a Databricks Connect session on serverless or the configured cluster <<< #
@pytest.fixture(scope="session")
def spark(need):
    need("DATABRICKS_CONFIG_PROFILE")
    connect = pytest.importorskip("databricks.connect", reason="uv sync --group live (Python 3.12)")
    compute = os.environ.get("DTRACK_LIVE_COMPUTE", "serverless")
    builder = connect.DatabricksSession.builder
    builder = builder.serverless() if compute == "serverless" else builder.clusterId(compute)
    session = builder.getOrCreate()
    yield session
    session.stop()


# >>> the synthetic Right CSVs as Unity Catalog tables <schema>.<name>_<tag>; dropped afterwards <<< #
@pytest.fixture(scope="session")
def right_tables(spark, need, synthetic, tag, keep):
    import pandas as pd

    schema = need("DTRACK_LIVE_SCHEMA")
    suffix = tag.replace("-", "_")
    tables = {}
    for name, ddl in RIGHT_SCHEMAS.items():
        frame = pd.read_csv(synthetic.right / f"{name}.csv", dtype=str)
        table = f"{schema}.{name}_{suffix}"
        columns = [f"cast({column} as {kind}) as {column}" for column, kind in _typed(ddl)]
        typed = spark.createDataFrame(frame).selectExpr(*columns)
        typed.write.mode("overwrite").saveAsTable(_quoted(table))
        tables[name] = table
    yield tables
    if not keep:
        for table in tables.values():
            spark.sql(f"DROP TABLE IF EXISTS {_quoted(table)}")


def _quoted(name):
    return ".".join(f"`{part}`" for part in name.split("."))


def _typed(ddl):
    for item in ddl.split(", "):
        column, kind = item.split(" ", 1)
        yield column, kind


# >>> an Oracle container holding the synthetic Left tables (the stand-in for the on-prem database) <<< #
@pytest.fixture(scope="session")
def oracle(synthetic):
    pytest.importorskip("testcontainers")
    for connect in oracle_container():
        load_oracle_tables(connect, synthetic.left)
        yield connect
