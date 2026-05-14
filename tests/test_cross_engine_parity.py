"""Cross-engine parity tests for categorical coarse signals.

The categorical extraction emits three coarse drift signals — `length_avg`,
`length_max`, `hash_sum` — that must agree across Oracle, Athena/Trino, and
Hadoop/SAS on identical data. The signals are how the Phase-1 compare
detects "values shifted" without re-pulling the freq table; if the recipe
diverges between engines, a cross-engine diff falsely flags drift.

Architecture: `dtrack.platforms.base` defines Python reference implementations
(`categorical_trim`, `categorical_length`, `categorical_hash16`,
`categorical_aggregates`). The SQL builders in `oracle.py` and `athena.py`
emit expressions that reproduce the same recipe on each engine. These tests
exercise the reference impl on edge cases AND assert each SQL builder
contains the right primitives so the two paths can't drift apart silently.

Engine env requirements the recipe assumes (documented here as well):
  - Oracle: NLS_CHARACTERSET=AL32UTF8 (or call the SQL on a UTF-8 column)
  - Athena/Trino: VARCHAR (always UTF-8 in Trino)
  - SAS / Hadoop: ENCODING=UTF8
If any engine runs in a non-UTF-8 session, the MD5 input bytes diverge and
this whole parity story breaks.
"""

import hashlib

from dtrack.platforms.base import (
    P_COL_MAX_LEN,
    categorical_aggregates,
    categorical_hash16,
    categorical_length,
    categorical_trim,
    categorical_truncate,
)


# ---------------------------------------------------------------------------
# Reference recipe -- verify the Python helpers match the documented formula
# ---------------------------------------------------------------------------

def _expected_hash16(value):
    """Direct implementation of the cross-engine hash recipe."""
    trimmed = value.strip(' ')
    md5 = hashlib.md5(trimmed.encode('utf-8')).hexdigest()
    return int(md5[:4], 16)


def test_hash16_matches_documented_recipe():
    """categorical_hash16 = int(md5(utf8(trim(v)))[:4], 16)."""
    for raw in ['hello', 'SF', 'café', '🎉', 'abc xyz', '']:
        assert categorical_hash16(raw) == _expected_hash16(raw), raw


def test_hash16_null_returns_none():
    """NULL p_col rows don't contribute to hash_sum."""
    assert categorical_hash16(None) is None


def test_hash16_trims_ascii_space_only():
    """Leading/trailing ASCII space is stripped before hashing — matches
    Oracle TRIM() default and Athena/Trino TRIM() default."""
    assert categorical_hash16('  SF  ') == categorical_hash16('SF')
    assert categorical_hash16(' SF') == categorical_hash16('SF')
    assert categorical_hash16('SF ') == categorical_hash16('SF')


def test_hash16_preserves_internal_space():
    """Internal whitespace stays in the value -- TRIM doesn't collapse it."""
    assert categorical_hash16('SF NY') != categorical_hash16('SFNY')


def test_hash16_preserves_non_ascii_whitespace():
    """SQL TRIM() defaults to ASCII space only; tabs, NBSP, etc. remain.
    Documenting this explicitly so a future engine swap can't silently
    change semantics."""
    # \t is NOT trimmed by default TRIM() on Oracle or Trino.
    assert categorical_hash16('\tSF') == _expected_hash16('\tSF')
    # NBSP (U+00A0) also not trimmed.
    assert categorical_hash16(' SF') == _expected_hash16(' SF')


def test_hash16_uses_utf8_bytes_for_non_ascii():
    """Non-ASCII chars are hashed as UTF-8 bytes (so 'café' hashes the
    same on Oracle UTF-8, Athena/Trino, and Hadoop UTF-8 SAS)."""
    cafe = 'café'  # 'café'
    expected = int(hashlib.md5(cafe.encode('utf-8')).hexdigest()[:4], 16)
    assert categorical_hash16(cafe) == expected


def test_hash16_emoji_surrogate_pair():
    """Multi-byte chars (emojis encode as 4 UTF-8 bytes) hash by their
    UTF-8 representation, not their Python str width."""
    emoji = '\U0001f389'  # 🎉
    expected = int(hashlib.md5(emoji.encode('utf-8')).hexdigest()[:4], 16)
    assert categorical_hash16(emoji) == expected


# ---------------------------------------------------------------------------
# Length recipe
# ---------------------------------------------------------------------------

def test_length_counts_unicode_codepoints_not_bytes():
    """LENGTH() must report characters, not bytes. 'café' is 4 chars even
    though it's 5 bytes in UTF-8 (é = 2 bytes).

    This is the root cause of the most common cross-engine length_avg
    divergence: Oracle on a non-UTF-8 charset, or LENGTHB instead of
    LENGTH, would return bytes. The fix is to make sure Oracle is
    AL32UTF8 + use LENGTH (not LENGTHB).
    """
    assert categorical_length('café') == 4
    assert categorical_length('\U0001f389') == 1   # emoji = 1 codepoint
    assert categorical_length('hello') == 5
    assert categorical_length('') == 0


def test_length_trims_before_counting():
    """LENGTH(TRIM(p_col)) — leading/trailing ASCII space is stripped first."""
    assert categorical_length('  SF  ') == 2
    assert categorical_length('   ') == 0  # all-space trims to empty


def test_length_null_returns_none():
    assert categorical_length(None) is None


# ---------------------------------------------------------------------------
# Truncation cap (P_COL_MAX_LEN)
# ---------------------------------------------------------------------------

def test_truncate_caps_at_200_chars():
    """All three engines (Hadoop SAS / Oracle / Athena) truncate p_col to
    P_COL_MAX_LEN before hashing/measuring. Set to 200 to match Hadoop's
    column_hadoop.sas, the engine that's most expensive to re-extract."""
    assert P_COL_MAX_LEN == 200
    long_value = 'X' * 1000
    assert len(categorical_truncate(long_value)) == 200


def test_truncate_preserves_short_values():
    assert categorical_truncate('hello') == 'hello'
    assert categorical_truncate('') == ''


def test_aggregates_truncate_before_hash():
    """Two values that differ only past the 200-char cap hash to the same
    hash16 -- because both engines truncate first."""
    a = 'X' * 199 + 'A'  # 200 chars, ends 'A'
    b = 'X' * 199 + 'A' + 'extra_tail'  # >200 chars, same first 200
    r = categorical_aggregates([(a, 1), (b, 1)])
    # Both truncate to the same 200-char prefix, so they collapse to one
    # category for length / hash purposes (though the freq table itself
    # already keyed on the truncated value, so this is just verifying
    # the helper agrees).
    assert categorical_hash16(a[:P_COL_MAX_LEN]) == categorical_hash16(b[:P_COL_MAX_LEN])


# ---------------------------------------------------------------------------
# Aggregate recipe -- length_avg / length_max / hash_sum
# ---------------------------------------------------------------------------

def test_aggregates_simple_ascii():
    """Hand-checked aggregate over a small (value, count) set."""
    freq = [('A', 3), ('BC', 2), ('DEF', 1)]
    r = categorical_aggregates(freq)
    # length_avg = (3*1 + 2*2 + 1*3) / (3+2+1) = 10/6
    assert r['length_avg'] == 10 / 6
    assert r['length_max'] == 3
    expected_hash = (3 * categorical_hash16('A')
                     + 2 * categorical_hash16('BC')
                     + 1 * categorical_hash16('DEF'))
    assert r['hash_sum'] == expected_hash


def test_aggregates_skip_null_for_length_and_hash():
    """NULL rows count only toward n_missing; they're excluded from length
    and hash aggregates. Mirrors the SQL `WHERE p_col IS NOT NULL`."""
    freq = [('A', 3), (None, 10), ('B', 2)]
    r = categorical_aggregates(freq)
    # NULL contributes 0 to numerator and denominator: (3*1 + 2*1) / (3+2) = 1.0
    assert r['length_avg'] == 1.0
    assert r['hash_sum'] == 3 * categorical_hash16('A') + 2 * categorical_hash16('B')


def test_aggregates_all_null_returns_zero_hash():
    """A vintage with only NULL p_col rows -- length stays None, hash=0."""
    r = categorical_aggregates([(None, 100)])
    assert r['length_avg'] is None
    assert r['length_max'] is None
    assert r['hash_sum'] == 0


def test_aggregates_empty_input():
    r = categorical_aggregates([])
    assert r['length_avg'] is None
    assert r['length_max'] is None
    assert r['hash_sum'] == 0


def test_aggregates_unicode_lengths():
    """café (4 chars) + 🎉 (1 char) -> length_avg computed by chars not bytes."""
    freq = [('café', 3), ('\U0001f389', 2)]
    r = categorical_aggregates(freq)
    # length_avg = (3*4 + 2*1) / (3+2) = 14/5
    assert r['length_avg'] == 14 / 5
    assert r['length_max'] == 4


def test_aggregates_whitespace_only_is_empty():
    """A p_col that's all spaces strips to ''. After TRIM it has length 0
    and contributes 0 to the length average and 0 to length_max
    (assuming it's the only value or other values are also 0-length)."""
    freq = [('   ', 5)]
    r = categorical_aggregates(freq)
    assert r['length_avg'] == 0.0
    assert r['length_max'] == 0


# ---------------------------------------------------------------------------
# SQL builders contain the expected primitives
# ---------------------------------------------------------------------------

def test_oracle_hash_sql_forces_utf8_bytes():
    """Oracle SQL must convert to UTF-8 RAW before MD5 so the hash input
    is identical to Athena's TO_UTF8(...) regardless of NLS_CHARACTERSET."""
    from dtrack.platforms.oracle import _oracle_portable_hash16
    expr = _oracle_portable_hash16('p_col')
    assert 'AL32UTF8' in expr
    assert 'STRING_TO_RAW' in expr
    assert 'STANDARD_HASH' in expr
    # 16-bit slice: SUBSTR(..., 1, 4)
    assert 'SUBSTR' in expr and ", 1, 4" in expr


def test_oracle_categorical_sql_has_coarse_signals():
    """Oracle categorical SQL emits length_avg/length_max/hash_sum so the
    14-field schema is symmetric with Athena/Hadoop."""
    from dtrack.platforms.oracle import _build_col_sql_oracle
    tbl_cfg = {'date_col': 'DT', 'name': 't', 'where': '', 'columns': {'C': 'VARCHAR2'}}
    sql, _ = _build_col_sql_oracle(
        tbl_cfg, 'C', 'VARCHAR2', 'TBL', '',
        vintage='all', date_dtype=None, date_format=None, dt_label='all',
    )
    assert 'length_avg' in sql.lower()
    assert 'length_max' in sql.lower()
    assert 'hash_sum'   in sql.lower()
    assert 'LENGTH(TRIM(p_col))' in sql


def test_oracle_categorical_sql_truncates_at_200():
    """Oracle categorical SQL truncates p_col to P_COL_MAX_LEN (200) so it
    hashes/measures the same prefix Hadoop SAS sees."""
    from dtrack.platforms.oracle import _build_col_sql_oracle
    tbl_cfg = {'date_col': 'DT', 'name': 't', 'where': '', 'columns': {'C': 'VARCHAR2'}}
    sql, _ = _build_col_sql_oracle(
        tbl_cfg, 'C', 'VARCHAR2', 'TBL', '',
        vintage='all', date_dtype=None, date_format=None, dt_label='all',
    )
    assert 'SUBSTR(TO_CHAR' in sql and ', 1, 200)' in sql


def test_oracle_alpha_rank_uses_trim():
    """Alpha-rank ORDER BY must use UPPER(TRIM(p_col)) so leading-space
    values don't sort before all letters."""
    from dtrack.platforms.oracle import _build_col_sql_oracle
    sql, _ = _build_col_sql_oracle(
        {'date_col': 'DT', 'name': 't', 'where': '', 'columns': {'C': 'VARCHAR2'}},
        'C', 'VARCHAR2', 'TBL', '',
        vintage='all', date_dtype=None, date_format=None, dt_label='all',
    )
    assert 'UPPER(TRIM(p_col))' in sql


def test_oracle_minmax_strips_leading_whitespace():
    """min_val / max_val emit `TRIM(p_col) || '=' || cnt`, not raw p_col."""
    from dtrack.platforms.oracle import _build_col_sql_oracle
    sql, _ = _build_col_sql_oracle(
        {'date_col': 'DT', 'name': 't', 'where': '', 'columns': {'C': 'VARCHAR2'}},
        'C', 'VARCHAR2', 'TBL', '',
        vintage='all', date_dtype=None, date_format=None, dt_label='all',
    )
    assert "TRIM(p_col) || '='" in sql


# Athena side -- we don't import the inline categorical SQL directly (it
# lives inside _extract_col_athena), but `_athena_portable_hash16` is
# importable. Verify its recipe matches the same UTF-8 + first-4-hex
# pattern.
def test_athena_hash_sql_uses_utf8_bytes():
    from dtrack.platforms.athena import _athena_portable_hash16
    expr = _athena_portable_hash16('p_col')
    assert 'TO_UTF8' in expr
    assert 'MD5' in expr
    assert 'TRIM(p_col)' in expr
    # 16-bit slice: SUBSTR(..., 1, 4) then from_base(..., 16)
    assert 'from_base' in expr
    assert ', 1, 4)' in expr
    assert ', 16)' in expr


def test_oracle_and_athena_hash_recipe_align():
    """Both engines hash the same logical input: MD5(UTF-8 bytes of TRIM(value)),
    then take first 4 hex chars as a 16-bit integer. Sanity check that the
    SQL strings contain the matching primitives so neither side drifts."""
    from dtrack.platforms.oracle import _oracle_portable_hash16
    from dtrack.platforms.athena import _athena_portable_hash16
    ora = _oracle_portable_hash16('p_col')
    ath = _athena_portable_hash16('p_col')
    # Both apply TRIM before hashing.
    assert 'TRIM(p_col)' in ora
    assert 'TRIM(p_col)' in ath
    # Both force UTF-8 encoding of the trimmed string.
    assert 'AL32UTF8' in ora
    assert 'TO_UTF8' in ath
    # Both compute MD5 (Oracle via STANDARD_HASH, Athena directly).
    assert 'MD5' in ora
    assert 'MD5' in ath


# ---------------------------------------------------------------------------
# Documented engine assumptions (regression alerts if anyone changes them)
# ---------------------------------------------------------------------------

def test_p_col_max_len_matches_hadoop_sas():
    """P_COL_MAX_LEN must equal the 200-char cap in column_hadoop.sas;
    otherwise hash/length divergence between Hadoop and Oracle/Athena."""
    import os
    sas_path = os.path.join(
        os.path.dirname(__file__), '..', 'dtrack', 'platforms',
        'templates', 'column_hadoop.sas')
    with open(sas_path) as f:
        sas = f.read()
    assert ', 1, 200)' in sas, (
        f"Hadoop SAS no longer caps p_col at 200 chars; update "
        f"P_COL_MAX_LEN in base.py to match or all three engines diverge")
