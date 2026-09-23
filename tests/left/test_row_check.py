from support import MemoryStore

from dtrack_left.row_check import run_row_check


class Fake:
    def preflight(self):
        return {"ok": True}

    def partition_counts(self, *_args):
        return {"2026-01-01": 1, "20260102": 2}


def test_row_check_publishes_counts_and_public_config_without_secrets():
    config = {
        "pairs": {
            "p": {
                "left": {
                    "source": "oracle",
                    "table": "A",
                    "date_col": "dt",
                    "dsn": "host:1521/SVC",
                    "user_env": "PCDS_USER",
                    "password": "secret",
                },
                "right": {
                    "source": "databricks",
                    "catalog": "c",
                    "schema": "s",
                    "table": "a",
                    "date_col": "dt",
                    "server_hostname": "adb.example.net",
                    "access_token_env": "TOK",
                },
                "col_map": {"amount": "amount"},
                "sample": 0.1,
                "key": "id",
            }
        }
    }
    store = MemoryStore()
    run_id, documents = run_row_check(
        config, store, run_id="run", source_factory=lambda _row: Fake()
    )
    doc = store.get_json("row_check/p.left.json")
    assert run_id == "run" and len(documents) == 1
    assert doc["counts"] == {"2026-01-01": 1, "20260102": 2}
    assert doc["pair_config"]["sample"] == 0.1
    assert "dsn" not in doc["left"] and "password" not in doc["left"]
    assert "user_env" not in doc["left"]
    assert doc["right"] == {
        "source": "databricks",
        "catalog": "c",
        "schema": "s",
        "table": "a",
        "date_col": "dt",
    }
    assert store.get_json("row_check/_LEFT_CLOSED")["pairs"] == ["p"]
    assert not any(key.startswith("manifest/") for key in store.objects)
