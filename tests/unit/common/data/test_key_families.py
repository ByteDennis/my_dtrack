import pytest


@pytest.mark.parametrize(
    ("family", "value", "expected"),
    [
        ("numeric", "007", "7"),
        ("numeric", "-007.50", "-7.5"),
        ("numeric", "0.50", "0.5"),
        ("numeric", "000", "0"),
        ("string", " AbC ", "abc"),
        ("exact", " AbC ", "AbC"),
        ("date", "2026-01-31 00:00:00", "2026-01-31"),
        ("date", "not a date", "<<NULL>>"),
        ("digits", "123-45-6789", "123456789"),
        (["trim", "upper", "strip_prefix:ID-"], " id-42", "42"),
        (["lpad:5"], "42", "00042"),
        (["strip_suffix:.0"], "42.0", "42"),
    ],
)
def test_builtin(common, family, value, expected):
    assert common.hashing.key_family(family).normalize(value) == expected


def test_transform(common):
    family = common.hashing.key_family("numeric")
    assert family.left_transform("007") == family.right_transform("007") == family.transform("007")
    assert family.left_transform_sql("x") == family.right_transform_sql("x")


def test_side(common):
    family = common.hashing.key_family(
        {"family": "numeric", "left_transform": ["trim", "strip_prefix:A"]}
    )
    assert family.normalize("A007", "left") == "007"
    assert family.normalize("007", "right") == "7"
    assert family.spec() == {
        "transform": ["trim", "strip_leading_zeros", "collapse_decimal"],
        "left_transform": ["trim", "strip_prefix:A"],
    }
    assert common.hashing.key_family(family.spec()) == family


def test_registered(common):
    keys = common.hashing

    @keys.register_family("test_branch")
    class Branch(keys.KeyFamily):
        steps = ("trim",)

        def left_transform(self, text):
            return text.strip().split("-")[-1]

        def left_transform_sql(self, text):
            return f"element_at(split(trim({text}), '-'), -1)"

    try:
        family = keys.key_family("test_branch")
        assert isinstance(family, Branch) and family.spec() == "test_branch"
        assert family.normalize("NY-0042", "left") == family.normalize("0042", "right") == "0042"
        assert "element_at" in family.sql("`b`", "left")
    finally:
        keys.FAMILIES.pop("test_branch")


@pytest.mark.parametrize(
    ("spec", "message"),
    [
        ("blob", "unsupported key family 'blob'"),
        (["trim", "nope"], "unknown key step 'nope'"),
        (["lpad"], "needs :arg"),
        (["trim:x"], "takes no :arg"),
        ({"steps": ["trim"]}, "unsupported key family fields: steps"),
        (7, "must be a name"),
    ],
)
def test_invalid(common, spec, message):
    with pytest.raises((TypeError, ValueError), match=message):
        common.hashing.key_family(spec)


def test_spark(common):
    assert common.hashing.spark_literal("o'k\\") == "'o\\'k\\\\'"


def test_date(common):
    family = common.hashing.key_family(
        {
            "family": "date_type",
            "left_date_type": {"type": "sas", "format": "days"},
            "right_date_type": {"type": "string", "format": "dash"},
        }
    )
    assert family.normalize("24108.0", "left") == "2026-01-02"
    assert family.normalize("2026-01-02", "right") == "2026-01-02"
    assert family.spec() == {
        "family": "date_type",
        "left_date_type": {"type": "sas", "format": "days"},
        "right_date_type": {"type": "string", "format": "dash"},
    }
    assert common.hashing.key_family(family.spec()) == family
