"""Live: row extraction runs against the real backend and returns rows."""


def test_extract_row(pipe):
    pipe.extract("row")
    pipe.assert_real_csv("row")
