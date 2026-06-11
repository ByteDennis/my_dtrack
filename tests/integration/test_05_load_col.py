"""Live: load-col ingests the extracted col-stats CSVs into _col_stats."""


def test_load_col(pipe):
    pipe.extract("col")
    pipe.load("col")
    pipe.assert_loaded("col")
