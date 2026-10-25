import io
from types import SimpleNamespace as NS

import pytest

from pair_recon_dbx import cache


def result(columns, rows):
    return {
        "statement_id": "s1",
        "source": "cache",
        "columns": columns,
        "rows": rows,
        "total_rows": len(rows),
    }


def test_cache_table_uses_pair_table_catalog_and_schema():
    assert cache.cache_table("main.recon.pair_recon_pair_meta") == (
        "main.recon.pair_recon_cache_index"
    )
    with pytest.raises(ValueError, match="catalog.schema.table"):
        cache.cache_table("recon.pairs")


def test_show_and_explain(monkeypatch):
    responses = [
        result(["cache_key", "uri", "format"], [["abc", "/Volumes/x/a.json", "json"]]),
        result(
            ["cache_type", "statistic_name", "state", "artifacts", "size_bytes"],
            [["statistics", "hash_sum", "complete", "2", "100"]],
        ),
    ]
    monkeypatch.setattr(cache.runtime, "run_sql", lambda *_args, **_kwargs: responses.pop(0))

    assert cache.show(object(), "w", "main.s.cache", "abc")["uri"].endswith("a.json")
    summary = cache.explain(object(), "w", "main.s.cache", "ORDERS")
    assert summary["total_artifacts"] == 2
    assert summary["total_size_bytes"] == 100


def test_peek_json_and_parquet(monkeypatch):
    json_record = result(["cache_key", "uri", "format"], [["abc", "/Volumes/x/a.json", "json"]])
    parquet_record = result(
        ["cache_key", "uri", "format"], [["def", "/Volumes/x/a.parquet", "parquet"]]
    )
    preview = result(["id"], [["1"]])
    responses = [json_record, parquet_record, preview]
    monkeypatch.setattr(cache.runtime, "run_sql", lambda *_args, **_kwargs: responses.pop(0))
    workspace = NS(files=NS(download=lambda _uri: NS(contents=io.BytesIO(b'{"ok":true}'))))

    assert cache.peek(workspace, "w", "main.s.cache", "abc")["preview"] == {"ok": True}
    assert cache.peek(workspace, "w", "main.s.cache", "def")["preview"]["rows"] == [["1"]]


def test_show_rejects_missing_key(monkeypatch):
    monkeypatch.setattr(cache.runtime, "run_sql", lambda *_args, **_kwargs: result([], []))
    with pytest.raises(ValueError, match="does not exist"):
        cache.show(object(), "w", "main.s.cache", "missing")


def test_cache_console_formatters():
    explained = cache.format_explain(
        {
            "pair": "ORDERS",
            "artifacts": [
                {
                    "cache_type": "statistics",
                    "statistic_name": "hash_sum",
                    "state": "complete",
                    "artifacts": "2",
                    "size_bytes": "100",
                }
            ],
            "total_artifacts": 2,
            "total_size_bytes": 100,
        }
    )
    assert "statistics/hash_sum: complete" in explained
    peeked = cache.format_peek(
        {
            "cache": {
                "cache_key": "abc",
                "cache_type": "extract",
                "format": "json",
                "uri": "/Volumes/a.json",
            },
            "preview": {"ok": True},
        }
    )
    assert '"ok": true' in peeked
