from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
ORACLE_IMAGE = "gvenzl/oracle-free:slim-faststart"
TRINO_IMAGE = "trinodb/trino:435"
HIVE_IMAGE = "apache/hive:4.0.0"


def pytest_addoption(parser):
    parser.addoption("--run-containers", action="store_true", default=False,
                     help="run real-engine integration tests (Docker / Spark)")


# >>> True when a Docker daemon is reachable <<< #
def _docker_available():
    try:
        import docker
        docker.from_env().ping()
        return True
    except Exception:
        return False


# >>> skip a docker-backed fixture cleanly when no daemon is reachable <<< #
def _require_docker():
    if not _docker_available():
        pytest.skip("Docker not available")


# >>> skip everything under tests/containers/ unless opted in <<< #
def pytest_collection_modifyitems(config, items):
    if config.getoption("--run-containers"):
        return
    skip = pytest.mark.skip(reason="needs --run-containers")
    for item in items:
        try:
            Path(item.path).resolve().relative_to(HERE)
        except (ValueError, AttributeError):
            continue
        item.add_marker(skip)


# >>> session-scoped Oracle Free container (gvenzl/oracle-free) <<< #
@pytest.fixture(scope="session")
def _oracle_container():
    _require_docker()
    from testcontainers.oracle import OracleDbContainer
    with OracleDbContainer(ORACLE_IMAGE, username="dtrack",
                           password="dtrack", dbname="FREEPDB1") as c:
        yield c


# >>> fresh oracledb cursor against the session container <<< #
@pytest.fixture
def oracle_cursor(_oracle_container):
    import oracledb
    host = _oracle_container.get_container_host_ip()
    port = _oracle_container.get_exposed_port(1521)
    conn = oracledb.connect(user="dtrack", password="dtrack",
                            dsn=f"{host}:{port}/FREEPDB1")
    cur = conn.cursor()
    try:
        yield cur
    finally:
        cur.close()
        conn.close()


# >>> poll until at least one Trino node can run a scheduled scan <<< #
def _wait_trino_ready(container, timeout=120):
    import time
    import trino
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            conn = trino.dbapi.connect(
                host=container.get_container_host_ip(),
                port=int(container.get_exposed_port(8080)),
                user="test", catalog="memory", schema="default")
            cur = conn.cursor()
            cur.execute("SELECT count(*) FROM tpch.tiny.nation")
            cur.fetchall()
            cur.close()
            conn.close()
            return
        except Exception as e:
            last = e
        time.sleep(2)
    raise RuntimeError(f"Trino not ready after {timeout}s: {last}")


# >>> session-scoped Trino container with a writable in-memory catalog <<< #
@pytest.fixture(scope="session")
def _trino_container():
    _require_docker()
    from testcontainers.trino import TrinoContainer
    c = TrinoContainer(TRINO_IMAGE)
    c.with_volume_mapping(str(HERE / "trino_catalog" / "memory.properties"),
                          "/etc/trino/catalog/memory.properties")
    with c as t:
        _wait_trino_ready(t)
        yield t


# >>> fresh trino dbapi cursor (catalog=memory, schema=default) <<< #
@pytest.fixture
def trino_cursor(_trino_container):
    import trino
    conn = trino.dbapi.connect(
        host=_trino_container.get_container_host_ip(),
        port=int(_trino_container.get_exposed_port(8080)),
        user="test", catalog="memory", schema="default")
    cur = conn.cursor()
    try:
        yield cur
    finally:
        cur.close()
        conn.close()


# >>> open an impyla HiveServer2 connection (SASL/PLAIN, anonymous) <<< #
def _hive_connect(container):
    from impala.dbapi import connect
    return connect(host=container.get_container_host_ip(),
                   port=int(container.get_exposed_port(10000)),
                   auth_mechanism="PLAIN", user="hive")


# >>> poll until HiveServer2 accepts queries <<< #
def _wait_hive_ready(container, timeout=180):
    import time
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            conn = _hive_connect(container)
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchall()
            cur.close()
            conn.close()
            return
        except Exception as e:
            last = e
        time.sleep(4)
    raise RuntimeError(f"Hive not ready after {timeout}s: {last}")


# >>> session-scoped standalone HiveServer2 container (apache/hive) <<< #
@pytest.fixture(scope="session")
def _hive_container():
    _require_docker()
    from testcontainers.core.container import DockerContainer
    from testcontainers.core.waiting_utils import wait_for_logs
    c = (DockerContainer(HIVE_IMAGE)
         .with_env("SERVICE_NAME", "hiveserver2")
         .with_exposed_ports(10000))
    with c as hc:
        wait_for_logs(hc, "Starting HiveServer2", timeout=180)
        _wait_hive_ready(hc)
        yield hc


# >>> fresh impyla Hive cursor against the session container <<< #
@pytest.fixture
def hive_cursor(_hive_container):
    conn = _hive_connect(_hive_container)
    cur = conn.cursor()
    try:
        yield cur
    finally:
        cur.close()
        conn.close()


# >>> session-scoped local PySpark session (Databricks Spark-SQL proxy) <<< #
@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    s = (SparkSession.builder
         .master("local[1]")
         .appName("dtrack-it")
         .config("spark.sql.shuffle.partitions", "1")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
    try:
        yield s
    finally:
        s.stop()
