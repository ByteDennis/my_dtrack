"""Live: compare-row reconciles both sides and produces its HTML report."""


def test_compare_row(pipe):
    pipe.extract("row")
    pipe.load("row")
    pipe.compare("row")
    pipe.assert_html("row")
