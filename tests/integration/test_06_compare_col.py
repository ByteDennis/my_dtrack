"""Live: match-columns + compare-col produce the column-comparison report.

extract(col) with db_path populates _column_meta, so match-columns can build
the col_map before compare-col diffs the per-column stats.
"""


def test_compare_col(pipe):
    pipe.extract("col")
    pipe.load("col")
    pipe.match_columns()
    pipe.compare("col")
    pipe.assert_html("col")
