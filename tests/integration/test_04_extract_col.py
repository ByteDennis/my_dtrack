"""Live: col extraction runs against the real backend and returns col stats.

This is the path the identifier-quoting / backtick logic runs in.
"""


def test_extract_col(pipe):
    pipe.extract("col")
    pipe.assert_real_csv("col")
