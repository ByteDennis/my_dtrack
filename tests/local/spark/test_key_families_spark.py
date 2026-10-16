import pytest

from dtrack_right.common.hashing import FAMILIES, STEPS, key_family

pytestmark = pytest.mark.spark

VALUES = [
    None,
    "",
    "   ",
    "007",
    "+007.50",
    "-007.50",
    "0.50",
    "000",
    "7.00",
    "1e3",
    "-0",
    "\t42 \n",
    " AbC ",
    "o'k",
    "back\\slash",
    "CAFÉ",
    "123-45-6789",
    "ID-0042",
    "42.0",
    "2026-01-31",
    "20260131",
    "2026-01-31 00:00:00",
    "2026-01-31T13:05:00",
    "01/31/2026",
    "1/5/2026",
    "02JAN2026",
    "02-Jan-2026",
    "20261399",
    "not a date",
]
ARGS = {"lpad": "6", "strip_prefix": "ID-", "strip_suffix": ".0"}
SPECS = [
    *sorted(FAMILIES),
    *(
        [f"{name}:{ARGS[name]}"] if step.takes_arg else [name]
        for name, step in sorted(STEPS.items())
    ),
]


@pytest.fixture(scope="module")
def spark():
    pyspark = pytest.importorskip("pyspark")
    session = (
        pyspark.sql.SparkSession.builder.master("local[1]")
        .appName("dtrack-key-families")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.createDataFrame(
        [(i, v) for i, v in enumerate(VALUES)], "i int, k string"
    ).createOrReplaceTempView("keys")
    yield session
    session.stop()


@pytest.mark.parametrize("ansi", ["false", "true"])
@pytest.mark.parametrize("spec", SPECS, ids=str)
def test_family_steps(spark, ansi, spec):
    spark.conf.set("spark.sql.ansi.enabled", ansi)
    family = key_family(spec)
    rows = spark.sql(f"SELECT i, {family.sql('k', 'right')} FROM keys ORDER BY i").collect()
    got = [value for _, value in rows]
    expected = [family.normalize(value, "right") for value in VALUES]
    assert got == expected, [(v, g, e) for v, g, e in zip(VALUES, got, expected) if g != e]
