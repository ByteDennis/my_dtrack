"""Live: load-row ingests the extracted row CSVs into _row_counts."""


def test_load_row(pipe):
    pipe.extract("row")
    pipe.load("row")
    pipe.assert_loaded("row")
