import json

import pytest
from support import oracle_config

from dtrack_left.backends.store import S3Store
from dtrack_left.cli import main

pytestmark = [pytest.mark.live, pytest.mark.docker]


# >>> step 3: doctor + row-check read Oracle and land counts on real S3, with no secret in them <<< #
def test_row_check_from_oracle_lands_on_real_s3(s3_scratch, oracle, tmp_path, capsys):
    path = tmp_path / "config.json"
    path.write_text(json.dumps(oracle_config(oracle)))
    assert main(["tools", "doctor", "--config", str(path)]) == 0
    capsys.readouterr()
    runs = f"{s3_scratch}/runs"
    assert (
        main(["row-check", "--config", str(path), "--run-s3", runs, "--run-id", "oracle"])
        == 0
    )
    store = S3Store(f"{runs}/oracle")
    orders = store.get_json("row_check/orders.left.json")
    assert len(orders["counts"]) == 58 and orders["counts"]["2026-01-15"] == 30
    assert "password" not in json.dumps(orders) and "dsn" not in orders["left"]
    assert store.get_json("row_check/_LEFT_CLOSED")["pairs"] == ["customers", "orders"]
