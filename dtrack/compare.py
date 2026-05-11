"""Comparison functionality for row counts and column statistics."""

import json
import re
from fnmatch import fnmatch
from .db import get_row_counts, get_col_stats, get_table_pair, list_table_pairs
from .date_utils import parse_date
from .constants import STAT_ROUND_DECIMALS


def resolve_col_filter(col_map, include_patterns=None, exclude_patterns=None):
    """Apply include/exclude glob patterns against col_map keys.

    Patterns match LEFT column names only. Comparison is case-insensitive.
    Include empty → all mapped pairs. Exclude is applied after include.
    Patterns that match left names NOT present in col_map are surfaced
    separately so the UI can warn about them.

    Returns dict:
        pairs:            [(left, right), ...] — effective (left, right) tuples
        total_mapped:     len(col_map)
        unmapped_matches: [left_name, ...] — patterns hit non-mapped left cols
    """
    include_patterns = [p.strip() for p in (include_patterns or []) if p and p.strip()]
    exclude_patterns = [p.strip() for p in (exclude_patterns or []) if p and p.strip()]

    mapped_left_lower = {l.lower(): l for l in (col_map or {}).keys()}

    def _matches_any(name_lower, pats):
        return any(fnmatch(name_lower, p.lower()) for p in pats)

    if include_patterns:
        kept_lower = {
            k: orig for k, orig in mapped_left_lower.items()
            if _matches_any(k, include_patterns)
        }
    else:
        kept_lower = dict(mapped_left_lower)

    if exclude_patterns:
        kept_lower = {
            k: orig for k, orig in kept_lower.items()
            if not _matches_any(k, exclude_patterns)
        }

    kept = sorted(kept_lower.values(), key=str.lower)
    pairs = [(l, col_map[l]) for l in kept]

    # "Unmapped matches" — user's include pattern hit something, but it's not
    # in col_map. Only meaningful when include patterns were given; otherwise
    # nothing is "matched" explicitly.
    unmapped_matches = []
    if include_patterns:
        all_left_cols = set()  # caller may pass a wider set via kwarg if desired
        # We can only know mapped cols here; the caller (API endpoint) is
        # responsible for comparing patterns against _column_meta too.
        unmapped_matches = []

    return {
        "pairs": pairs,
        "total_mapped": len(col_map or {}),
        "effective_count": len(pairs),
        "unmapped_matches": unmapped_matches,
    }


def _safe_int(val):
    """Parse a value to int, returning 0 for empty/None."""
    if val is None or val == '':
        return 0
    return int(float(str(val)))


def _safe_float(val):
    """Parse a value to float, returning None for empty/None."""
    if val is None or val == '':
        return None
    return float(str(val))


def _stat_match_key(val, prefix, ndigits=STAT_ROUND_DECIMALS):
    """Build a string match key for mean/std/min/max.

    Equality is a string compare with float-drift handled via rounding to
    STAT_ROUND_DECIMALS. Returns None for missing values.

    For min/max specifically, two extra normalizations:
      - strip the "cat=count" suffix (SAS fine path) so it agrees with the
        plain "cat" form (Athena coarse path) when the alpha-first /
        alpha-last value matches but the trailing count doesn't carry.
      - numeric mins/maxes like "0" vs "0.00" round to the same key.
    """
    if val is None or val == '':
        return None
    s = str(val).strip()
    if prefix in ("mn", "mx") and "=" in s:
        # Strip "=count" first; numeric round below sees just the value.
        s = s.rsplit("=", 1)[0].strip()
    try:
        return f"{prefix}_{round(float(s), ndigits)}"
    except (ValueError, TypeError):
        return f"{prefix}_{s}"


def _parse_top10(s):
    """Parse top_10 string (semicolon or JSON format) into {value: count} dict."""
    if not s or s == '[]':
        return {}
    s = str(s).strip()
    if s.startswith('['):
        try:
            entries = json.loads(s)
            return {str(e.get('value', '')): int(e.get('count', 0)) for e in entries}
        except (json.JSONDecodeError, TypeError):
            return {}
    result = {}
    for entry in s.split('; '):
        entry = entry.strip()
        if '(' in entry and entry.endswith(')'):
            val = entry[:entry.rfind('(')].strip()
            try:
                cnt = int(entry[entry.rfind('(') + 1:-1].strip())
            except ValueError:
                continue
            result[val] = cnt
    return result


def _compare_top10(left_str, right_str):
    """Compare two top_10 strings. Returns True if different."""
    left = _parse_top10(left_str)
    right = _parse_top10(right_str)
    return left != right


def parse_col_map_string(col_map_str):
    """Parse column mapping string like "AMT=amount,STATUS=status" into dict."""
    if not col_map_str:
        return {}
    mappings = {}
    for pair in col_map_str.split(','):
        pair = pair.strip()
        if '=' not in pair:
            continue
        left, right = pair.split('=', 1)
        mappings[left.strip()] = right.strip()
    return mappings


def get_column_mapping(db_path, table_left, table_right, pair_name=None, col_map_override=None):
    """Get column mapping between two tables.

    Priority: col_map_override > pair_name lookup > auto-detect > empty dict.
    """
    if col_map_override is not None:
        return col_map_override

    if pair_name:
        pair = get_table_pair(db_path, pair_name)
        if pair and pair.get('col_mappings'):
            return pair['col_mappings']

    pairs = list_table_pairs(db_path)
    for pair in pairs:
        if pair['table_left'] == table_left and pair['table_right'] == table_right:
            return pair.get('col_mappings', {})
        if pair['table_left'] == table_right and pair['table_right'] == table_left:
            mappings = pair.get('col_mappings', {})
            return {v: k for k, v in mappings.items()}

    return {}


def compare_row_counts(db_path, table_left, table_right, from_date=None, to_date=None):
    """Compare row counts between two tables.

    Returns dict with only_left, only_right, matching, mismatched, summary.
    """
    rows_left = get_row_counts(db_path, table_left, from_date, to_date)
    rows_right = get_row_counts(db_path, table_right, from_date, to_date)

    dict_left = {dt: count for dt, count in rows_left if dt}
    dict_right = {dt: count for dt, count in rows_right if dt}
    all_dates = set(dict_left.keys()) | set(dict_right.keys())

    only_left = []
    only_right = []
    matching = []
    mismatched = []

    for dt in sorted(all_dates):
        if dt in dict_left and dt in dict_right:
            if dict_left[dt] == dict_right[dt]:
                matching.append((dt, dict_left[dt]))
            else:
                mismatched.append((dt, dict_left[dt], dict_right[dt]))
        elif dt in dict_left:
            only_left.append((dt, dict_left[dt]))
        else:
            only_right.append((dt, dict_right[dt]))

    summary = {
        "date_range_left": (
            min(dict_left.keys()) if dict_left else None,
            max(dict_left.keys()) if dict_left else None,
        ),
        "date_range_right": (
            min(dict_right.keys()) if dict_right else None,
            max(dict_right.keys()) if dict_right else None,
        ),
        "total_left": sum(dict_left.values()),
        "total_right": sum(dict_right.values()),
        "count_left": len(dict_left),
        "count_right": len(dict_right),
    }

    return {
        "only_left": only_left,
        "only_right": only_right,
        "matching": matching,
        "mismatched": mismatched,
        "summary": summary,
    }


# ============================================================================
# Column type resolution
# ============================================================================

def resolve_col_type(left_type, right_type, overrides=None, col_name=None):
    """Resolve disagreeing column types. Categorical is the safe default.

    >>> resolve_col_type("categorical", "numeric")
    'categorical'
    >>> resolve_col_type("numeric", "numeric")
    'numeric'
    """
    if overrides and col_name and col_name in overrides:
        return overrides[col_name]
    if left_type == right_type:
        return left_type
    return "categorical"


def compare_column_stats(
    db_path, table_left, table_right,
    columns=None, col_mappings=None,
    from_date=None, to_date=None,
    matched_dates=None,
    col_type_overrides=None,
    level="fine",
):
    """Compare column statistics between two tables.

    `level="coarse"` returns only the cheap-aggregate fields
    (n_total/n_missing/n_unique/min/max/length_avg/length_max/hash_sum). Used
    for the first-pass "this column might be worth more inspection" check
    against coarse-extracted rows.

    `level="fine"` (default) returns everything (rank-weighted mean/std,
    top_10, etc.). Used for the drill-down view after the user picks
    suspicious (col, dt) cells from the coarse view.

    Now resolves col_type disagreements using resolve_col_type().
    """
    stats_left = get_col_stats(db_path, table_left, from_date=from_date, to_date=to_date)
    stats_right = get_col_stats(db_path, table_right, from_date=from_date, to_date=to_date)

    def organize_stats(stats):
        organized = {}
        for stat in stats:
            col = stat['column_name']
            raw = stat['dt']
            # Try to canonicalize to YYYY-MM-DD, but pass through sentinel
            # labels ('all', 'sample') and vintage bucket keys (e.g. week /
            # month tags) that parse_date doesn't recognize. Both sides agree
            # on the same label per bucket, so equality-keyed grouping still
            # works for the comparison join.
            if raw:
                try:
                    dt = parse_date(raw)
                except ValueError:
                    dt = raw
            else:
                dt = raw
            if col not in organized:
                organized[col] = {}
            organized[col][dt] = stat
        return organized

    left_by_col = organize_stats(stats_left)
    right_by_col = organize_stats(stats_right)

    if columns is None:
        columns = list(left_by_col.keys())
    if col_mappings is None:
        col_mappings = {}

    result = {}

    for left_col in columns:
        right_col = col_mappings.get(left_col, left_col)

        if left_col not in left_by_col or right_col not in right_by_col:
            continue

        left_dates = left_by_col[left_col]
        right_dates = right_by_col[right_col]

        comparisons = []
        all_dates = set(left_dates.keys()) | set(right_dates.keys())
        if matched_dates is not None:
            all_dates = all_dates & matched_dates

        for dt in sorted(all_dates):
            if dt not in left_dates or dt not in right_dates:
                continue

            left_stat = left_dates[dt]
            right_stat = right_dates[dt]

            # Resolve column type
            left_type = left_stat["col_type"]
            right_type = right_stat["col_type"]
            resolved_type = resolve_col_type(left_type, right_type, col_type_overrides, left_col)

            l_total = _safe_int(left_stat["n_total"])
            r_total = _safe_int(right_stat["n_total"])
            l_missing = _safe_int(left_stat["n_missing"])
            r_missing = _safe_int(right_stat["n_missing"])
            l_unique = _safe_int(left_stat["n_unique"])
            r_unique = _safe_int(right_stat["n_unique"])

            comparison = {
                "dt": dt,
                "vintage_label": left_stat.get("vintage_label") or dt,
                "col_type": resolved_type,
                "col_type_left": left_type,
                "col_type_right": right_type,
                "left_col": left_col,
                "right_col": right_col,
                "n_total_left": l_total,
                "n_total_right": r_total,
                "n_total_diff": r_total - l_total,
                "n_missing_left": l_missing,
                "n_missing_right": r_missing,
                "n_missing_diff": r_missing - l_missing,
                "n_unique_left": l_unique,
                "n_unique_right": r_unique,
                "n_unique_diff": r_unique - l_unique,
            }

            # min/max are emitted as "val=cnt" on every coarse-and-fine path
            # now (SAS Oracle/Hadoop categorical + Athena categorical), and
            # as plain numeric scalars for numeric columns. The match key
            # strips the "=cnt" suffix before comparing so older extractions
            # (or any platform that drops the suffix) still compare equal on
            # the value alone. For display we only strip whitespace — the
            # trailing "=cnt" is useful information about which value
            # actually anchors the alpha-min / alpha-max.
            def _norm_min_max(v):
                if v is None:
                    return ""
                return str(v).strip()
            l_min_key = _stat_match_key(left_stat["min_val"], "mn")
            r_min_key = _stat_match_key(right_stat["min_val"], "mn")
            l_max_key = _stat_match_key(left_stat["max_val"], "mx")
            r_max_key = _stat_match_key(right_stat["max_val"], "mx")
            comparison.update({
                "min_left": _norm_min_max(left_stat["min_val"]),
                "min_right": _norm_min_max(right_stat["min_val"]),
                "min_match": (l_min_key == r_min_key) if (l_min_key is not None and r_min_key is not None) else None,
                "max_left": _norm_min_max(left_stat["max_val"]),
                "max_right": _norm_min_max(right_stat["max_val"]),
                "max_match": (l_max_key == r_max_key) if (l_max_key is not None and r_max_key is not None) else None,
            })

            # Coarse-only signals. Four cases per field:
            #   - both populated, agree            → match=True
            #   - both populated, disagree         → match=False (diff)
            #   - both empty                       → match=None
            #   - one populated, the other empty   → see below
            #
            # length_avg / length_max are comparable across platforms (a
            # string's length is universal), so asymmetric presence is a
            # real diff worth flagging.
            #
            # hash_sum is NOT comparable across platforms — Oracle/Hive/
            # Athena use different hash functions, so cross-platform hash
            # comparison is intrinsically noise. Asymmetric presence on
            # hash_sum stays match=None (don't paint every cross-platform
            # column red just because the hashes don't share a function).
            for field in ("length_avg", "length_max", "hash_sum"):
                raw_l = left_stat.get(field)
                raw_r = right_stat.get(field)
                lv = raw_l.strip() if isinstance(raw_l, str) else (raw_l or "")
                rv = raw_r.strip() if isinstance(raw_r, str) else (raw_r or "")
                if lv and rv:
                    if field == "hash_sum":
                        # Relative tolerance: at the 1e12-1e17 magnitudes
                        # hash_sum lives in, DOUBLE summation-order
                        # differences between engines can be ~hundreds
                        # in absolute terms but ~1e-15 relative. Real
                        # drift moves the sum by orders of magnitude
                        # (a single changed value shifts cnt × hash by
                        # at least 1, often much more), well above the
                        # 1e-9 floor. So abs(diff)/max(|l|, |r|) < 1e-9
                        # passes on identical data even with FP noise
                        # while still flagging real drift.
                        try:
                            lf, rf = float(lv), float(rv)
                            denom = max(abs(lf), abs(rf), 1.0)
                            match = abs(lf - rf) / denom < 1e-9
                        except (ValueError, TypeError):
                            match = (lv == rv)
                    else:
                        # length_* values are small (single/double digits
                        # for typical strings), so absolute tolerance is
                        # fine and avoids spurious matches on tiny
                        # relative diffs that ARE real drift.
                        try:
                            match = abs(float(lv) - float(rv)) < 1e-4
                        except (ValueError, TypeError):
                            match = (lv == rv)
                elif lv or rv:
                    # Asymmetric presence.
                    match = None if field == "hash_sum" else False
                else:
                    match = None
                comparison[f"{field}_left"] = lv
                comparison[f"{field}_right"] = rv
                comparison[f"{field}_match"] = match

            if level == "fine":
                # Fine-only fields: rank-weighted mean/std for categorical,
                # arithmetic mean/std for numeric, plus top_10. Same match-key
                # logic as before so float-formatted values normalize cleanly.
                l_mean_key = _stat_match_key(left_stat["mean"], "m")
                r_mean_key = _stat_match_key(right_stat["mean"], "m")
                l_std_key = _stat_match_key(left_stat["std"], "s")
                r_std_key = _stat_match_key(right_stat["std"], "s")
                comparison.update({
                    "mean_left": left_stat["mean"],
                    "mean_right": right_stat["mean"],
                    "mean_match": (l_mean_key == r_mean_key) if (l_mean_key is not None and r_mean_key is not None) else None,
                    "std_left": left_stat["std"],
                    "std_right": right_stat["std"],
                    "std_match": (l_std_key == r_std_key) if (l_std_key is not None and r_std_key is not None) else None,
                })
                if resolved_type != "numeric":
                    comparison.update({
                        "top_10_left": left_stat.get("top_10", ""),
                        "top_10_right": right_stat.get("top_10", ""),
                    })

            comparisons.append(comparison)

        if comparisons:
            result[left_col] = comparisons

    return result


def _has_col_differences(comp):
    """Check if a column comparison has any statistical differences.

    Covers both coarse signals (n_total/n_missing/n_unique/min/max +
    length_avg/length_max/hash_sum) and fine signals (mean/std). Fields
    absent from the comparison (match=None) don't contribute — that lets
    a coarse comparison work without triggering false diffs on
    fine-only fields, and vice versa for legacy rows lacking length_*/
    hash_sum.
    """
    if comp.get('n_total_diff', 0) != 0:
        return True
    if comp.get('n_missing_diff', 0) != 0:
        return True
    if comp.get('n_unique_diff', 0) != 0:
        return True

    if comp.get('mean_match') is False:
        return True
    if comp.get('std_match') is False:
        return True

    # min/max use match keys (same rounding/string-prefix logic as mean/std)
    # so "0" vs "0.00" is a match for numeric columns, while "{cat}={count}"
    # strings still compare exactly for categorical columns.
    if comp.get('min_match') is False:
        return True
    if comp.get('max_match') is False:
        return True

    # Coarse-only signals: cheap structural & multiset-checksum hints.
    # Treated as a diff only if both sides have a value and they disagree.
    if comp.get('length_avg_match') is False:
        return True
    if comp.get('length_max_match') is False:
        return True
    if comp.get('hash_sum_match') is False:
        return True

    return False


def match_columns_from_dicts(left_cols, right_cols, left_label="left", right_label="right", outfile=None):
    """Compare two column dictionaries and match by case-insensitive name."""
    left_lower = {k.lower(): k for k in left_cols}
    right_lower = {k.lower(): k for k in right_cols}

    matched = {}
    left_only = []
    right_only = []

    for lower_name, left_name in sorted(left_lower.items()):
        if lower_name in right_lower:
            right_name = right_lower[lower_name]
            matched[left_name] = right_name
        else:
            left_only.append({"name": left_name, "type": left_cols[left_name]})

    for lower_name, right_name in sorted(right_lower.items()):
        if lower_name not in left_lower:
            right_only.append({"name": right_name, "type": right_cols[right_name]})

    result = {
        "matched": matched,
        f"{left_label}_only": left_only,
        f"{right_label}_only": right_only,
        "manual_mapping": {},
    }

    if outfile:
        with open(outfile, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2)
        print(f"\nWritten to: {outfile}")

    return result


def _wildcard_transform(value, pat_from, pat_to):
    """Transform a value by matching pat_from (fnmatch with *) and applying pat_to.

    The * in pat_from captures the variable portion, which replaces * in pat_to.

    >>> _wildcard_transform("AMT_TOTAL", "AMT_*", "amount_*")
    'amount_TOTAL'
    >>> _wildcard_transform("STATUS", "AMT_*", "amount_*")
    """
    # Convert fnmatch pattern to regex to capture the * part
    if '*' not in pat_from:
        if value == pat_from:
            return pat_to
        return None

    # Escape everything except *, then replace * with capture group
    regex = ''
    for ch in pat_from:
        if ch == '*':
            regex += '(.*)'
        else:
            regex += re.escape(ch)

    m = re.fullmatch(regex, value, re.IGNORECASE)
    if not m:
        return None

    captured = m.group(1)
    return pat_to.replace('*', captured)


def apply_column_rules(rules, unmatched_left, unmatched_right):
    """Evaluate wildcard/regex rules against unmatched columns.

    Args:
        rules: list of rule dicts with pattern_left, pattern_right, type
        unmatched_left: list of column name strings
        unmatched_right: list of column name strings

    Returns:
        (new_mappings, rule_sources) where:
        - new_mappings: {left_col: right_col} for matched columns
        - rule_sources: {left_col: "rule:N"} indicating which rule matched
    """
    new_mappings = {}
    rule_sources = {}
    right_set = set(unmatched_right)

    for rule_idx, rule in enumerate(rules):
        pat_left = rule.get("pattern_left", "")
        pat_right = rule.get("pattern_right", "")
        rule_type = rule.get("type", "wildcard")

        for left_col in list(unmatched_left):
            if left_col in new_mappings:
                continue

            if rule_type == "wildcard":
                transformed = _wildcard_transform(left_col, pat_left, pat_right)
                if transformed and transformed in right_set:
                    new_mappings[left_col] = transformed
                    rule_sources[left_col] = f"rule:{rule_idx}"
                    right_set.discard(transformed)
            elif rule_type == "regex":
                try:
                    m = re.fullmatch(pat_left, left_col, re.IGNORECASE)
                    if m:
                        transformed = m.expand(pat_right)
                        if transformed in right_set:
                            new_mappings[left_col] = transformed
                            rule_sources[left_col] = f"rule:{rule_idx}"
                            right_set.discard(transformed)
                except re.error:
                    continue

    return new_mappings, rule_sources
