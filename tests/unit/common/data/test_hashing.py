import hashlib

import pandas as pd
import pytest

from pair_recon_left.common.model import hashing as left_hashing
from pair_recon_left.common.sql import sampling as csv_sampling


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("2.50", "2.5"),
        ("2.0", "2"),
        ("-0.10", "-0.1"),
        ("10", "10"),
        ("007", "007"),
        ("1e3", "1e3"),
        ("abc", "abc"),
        ("1.2.3", "1.2.3"),
    ],
)
def test_collapse(common, text, expected):
    assert common.hashing.collapse_decimal(text) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ('"xxx"', "xxx"),
        ("'xxx'", "xxx"),
        ('"""xxx"""', "xxx"),
        ("'''xxx'''", "xxx"),
        ("\"'\"xxx'\"'", "xxx"),
        ('"xxx', "xxx"),
        ("xxx'", "xxx"),
        ("O'Connor", "O'Connor"),
        ('say "hello" please', 'say "hello" please'),
        ('"O\'Connor"', "O'Connor"),
        ('" xxx "', " xxx "),
        ('xxx"\n', 'xxx"\n'),
        ("\"''\"", ""),
        ("", ""),
    ],
)
def test_strip_quotes_python_and_sql_agree(common, value, expected):
    import duckdb

    rules = common.hashing.canonical_normalization_rules(
        [{"rule_name": "strip_quotes", "source": "all"}]
    )
    assert common.hashing.normalize_system_value(value, rules) == expected
    sql = common.hashing.normalize_system_sql("v", "duckdb", rules)
    assert duckdb.sql(f"SELECT {sql} FROM (SELECT ?::VARCHAR AS v)", params=[value]).fetchone() == (
        expected,
    )
    assert common.hashing.normalize_system_value(value, []) == value


def test_strip_quotes_honors_source_scope_and_preserves_sql_null(common):
    import duckdb

    rules = [{"rule_name": "strip_quotes", "source": "oracle"}]
    assert common.hashing.active_normalization_rules(rules, "csv") == ()
    active = common.hashing.active_normalization_rules(rules, "oracle")
    assert common.hashing.normalize_system_value('"xxx"', active) == "xxx"
    sql = common.hashing.normalize_system_sql("NULL::VARCHAR", "duckdb", active)
    assert duckdb.sql(f"SELECT {sql}").fetchone() == (None,)


def test_registered_rule_drives_validation_and_both_execution_paths(common, monkeypatch):
    import duckdb

    hashing = common.hashing
    monkeypatch.setattr(hashing, "RULES", dict(hashing.RULES))

    @hashing.register_rule
    class ReverseRule(hashing.NormalizationRule):
        name = "test_reverse"

        def normalize_value(self, text):
            return text[::-1]

        def normalize_sql(self, expr, dialect):
            return f"reverse({expr})"

    assert isinstance(hashing.RULES["test_reverse"], ReverseRule)
    assert hashing.default_normalization_rules()[-1] == {
        "rule_name": "test_reverse",
        "source": "all",
    }
    rules = [
        {"rule_name": "trim", "source": "all"},
        {"rule_name": "test_reverse", "source": "csv"},
        {
            "rule_name": "reversed_text",
            "source": "all",
            "match_value": "cba",
            "canonical_value": "done",
        },
    ]
    assert hashing.canonical_normalization_rules(rules) == rules
    active = hashing.active_normalization_rules(rules, "csv")
    assert hashing.normalize_system_value(" abc ", active) == "done"
    sql = hashing.normalize_system_sql("v", "duckdb", active)
    assert duckdb.sql(f"SELECT {sql} FROM (SELECT ' abc ' AS v)").fetchone() == ("done",)
    scoped = hashing.active_normalization_rules(rules, "oracle")
    assert hashing.normalize_system_value(" abc ", scoped) == "abc"


def test_default_rule_registration_preserves_existing_order(common):
    assert common.hashing.default_normalization_rules() == [
        {"rule_name": name, "source": "all"}
        for name in ("trim", "strip_quotes", "lowercase", "decimal", "strip_datetime")
    ]


def test_rule_registration_rejects_invalid_or_incomplete_classes(common):
    hashing = common.hashing
    with pytest.raises(TypeError, match="NormalizationRule class"):
        hashing.register_rule(object)
    with pytest.raises(TypeError, match="NormalizationRule class"):
        hashing.register_rule("trim")

    class MissingName(hashing.NormalizationRule):
        def normalize_value(self, text):
            return text

        def normalize_sql(self, expr, dialect):
            return expr

    with pytest.raises(ValueError, match="declare a lowercase name"):
        hashing.register_rule(MissingName)

    class MissingSql(hashing.NormalizationRule):
        name = "test_missing_sql"

        def normalize_value(self, text):
            return text

    with pytest.raises(TypeError, match="abstract"):
        hashing.register_rule(MissingSql)
    assert "test_missing_sql" not in hashing.RULES


@pytest.mark.parametrize(
    ("value", "family", "expected"),
    [
        (None, "string", "<<NULL>>"),
        ("   ", "numeric", "<<NULL>>"),
        (" Abc ", "string", "abc"),
        ("CAFÉ", "string", "café"),
        ("02JAN2026", "date", "2026-01-02"),
        (" 7.50 ", "numeric", "7.5"),
        (7, "numeric", "7"),
    ],
)
def test_normalize(common, value, family, expected):
    assert common.hashing.key_family(family).normalize(value) == expected


def test_hash32(common):
    assert common.hashing.hash32("abc") == int(hashlib.md5(b"abc").hexdigest()[:8], 16)
    assert 0 <= common.hashing.hash32("anything") < common.hashing.HASH_BUCKET_SPACE


def test_selected(common):
    hashing = common.hashing
    value = hashing.hash32("abc|7.5")
    assert hashing.selected_fraction([" ABC", "7.50"], ["string", "numeric"], value + 1)
    assert not hashing.selected_fraction([" ABC", "7.50"], ["string", "numeric"], value)


def _keys_csv(tmp_path, rows):
    path = tmp_path / "rows.csv"
    pd.DataFrame(rows, columns=["id", "value"]).to_csv(path, index=False)
    return path


def test_sample(tmp_path):
    path = _keys_csv(tmp_path, [["1", "a"]])
    assert csv_sampling.sample_csv(path, key_columns=[], mode="all") == path


def test_sample2(tmp_path):
    path = _keys_csv(tmp_path, [["1", "a"]])
    with pytest.raises(ValueError, match="requires key columns"):
        csv_sampling.sample_csv(path, key_columns=[], mode="fraction", rate=0.5)
    with pytest.raises(ValueError, match="unsupported sample mode"):
        csv_sampling.sample_csv(path, key_columns=["id"], mode="stratified")


def test_sample3(tmp_path):
    rows = [[f"{i:05d}", "x"] for i in range(400)]
    path = _keys_csv(tmp_path, rows)
    sampled = pd.read_csv(
        csv_sampling.sample_csv(
            path, key_columns=["id"], mode="fraction", rate=0.25, families=["numeric"]
        ),
        dtype=object,
    )
    threshold = round(0.25 * 2**32)
    expected = [key for key, _ in rows if left_hashing.hash32(str(int(key))) < threshold]
    assert list(sampled["id"]) == expected and 60 < len(expected) < 140


def test_fixed(tmp_path):
    path = _keys_csv(tmp_path, [["1", "a"], ["2.0", "b"], ["3", "c"]])
    sampled = csv_sampling.sample_csv(
        path, key_columns=["id"], mode="fixed", count=1, key_tuples=[["2"]], families=["numeric"]
    )
    assert sampled.read_text().splitlines() == ["id,value", "2.0,b"]


def test_fixed2(
    tmp_path,
):
    path = _keys_csv(tmp_path, [["1", "a"], ["1", "b"], ["2", "c"], ["3", "d"], ["4", "e"]])
    sampled = pd.read_csv(
        csv_sampling.sample_csv(path, key_columns=["id"], mode="fixed", count=2), dtype=object
    )
    ranked = sorted("1234", key=lambda key: hashlib.md5(key.encode()).hexdigest())[:2]
    assert sorted(set(sampled["id"])) == sorted(ranked)
    assert len(sampled) == sum(2 if key == "1" else 1 for key in ranked)


@pytest.mark.parametrize(
    "value", ["", " ", "    ", "\t", "\r\n", " \t\n ", "\u00a0", "\u2003\u3000", "\x1c\u0085"]
)
def test_whitespace_synonym_matches_python_and_sql(common, value):
    import duckdb

    rules = [
        {
            "rule_name": "zz_missing_whitespace",
            "source": "all",
            "match_value": "@whitespace",
            "canonical_value": "@missing",
        }
    ]
    rules = common.hashing.canonical_normalization_rules(rules)
    assert common.hashing.normalize_system_value(value, rules) is None
    sql = common.hashing.normalize_system_sql("v", "duckdb", rules)
    assert (
        duckdb.sql(f"SELECT {sql} FROM (SELECT ?::VARCHAR AS v)", params=[value]).fetchone()[0]
        is None
    )


@pytest.mark.parametrize("value", [" 0100 ", "a b", "\tX\n", "\u200b"])
def test_whitespace_synonym_preserves_nonblank_text(common, value):
    import duckdb

    rules = [
        {
            "rule_name": "zz_missing_whitespace",
            "source": "all",
            "match_value": "@whitespace",
            "canonical_value": "@missing",
        }
    ]
    assert common.hashing.normalize_system_value(value, rules) == value
    sql = common.hashing.normalize_system_sql("v", "duckdb", rules)
    assert (
        duckdb.sql(f"SELECT {sql} FROM (SELECT ?::VARCHAR AS v)", params=[value]).fetchone()[0]
        == value
    )
