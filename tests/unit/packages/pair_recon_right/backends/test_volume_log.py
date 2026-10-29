import logging

from pair_recon_right.engine.backends import volume_log
from pair_recon_right.engine.backends.volume_log import VolumeLogHandler
from pair_recon_right.engine.common.model.protocol import RIGHT_LOG


class Store:
    def __init__(self, failures=0):
        self.writes = []
        self.failures = failures

    def put_bytes(self, key, body, *, content_type=""):
        if self.failures:
            self.failures -= 1
            raise OSError(5, "Input/output error")
        self.writes.append((key, body.decode("utf-8")))


def _logger(handler):
    logger = logging.getLogger("pair_recon.test.volume_log")
    logger.handlers = [handler]
    logger.propagate = False
    logger.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
    return logger


def test_flushes_the_whole_log_only_when_new_lines_arrived():
    store = Store()
    handler = VolumeLogHandler(store, interval=3600)
    logger = _logger(handler)
    handler.flush()
    assert store.writes == []
    logger.info("first 好")
    logger.warning("second")
    handler.flush()
    handler.flush()
    logger.info("third")
    handler.close()
    assert [key for key, _ in store.writes] == [RIGHT_LOG, RIGHT_LOG]
    assert store.writes[0][1] == "INFO first 好\nWARNING second\n"
    assert store.writes[1][1] == "INFO first 好\nWARNING second\nINFO third\n"


def test_volume_failures_are_retried_on_the_next_flush_without_raising(caplog):
    store = Store(failures=1)
    handler = VolumeLogHandler(store, interval=3600)
    logger = _logger(handler)
    logger.info("kept")
    with caplog.at_level(logging.INFO, logger="pair_recon.right.log"):
        handler.flush()
        assert store.writes == [] and handler.failing
        handler.flush()
    assert store.writes == [(RIGHT_LOG, "INFO kept\n")] and not handler.failing
    assert "Volume log write failed" in caplog.text and "recovered" in caplog.text
    handler.close()


def test_oldest_lines_are_dropped_above_the_size_cap(monkeypatch):
    store = Store()
    handler = VolumeLogHandler(store, interval=3600, max_bytes=40)
    logger = _logger(handler)
    for i in range(6):
        logger.info("line %d", i)
    handler.close()
    body = store.writes[-1][1]
    assert body.startswith(volume_log.DROPPED)
    assert "line 0" not in body and "line 5" in body
    assert handler.size <= 40
