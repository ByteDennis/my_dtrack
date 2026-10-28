from pathlib import Path

from pair_recon_left.common.runtime.run_logging import (
    close_left_logging,
    configure_left_logging,
    get_left_logger,
)


def test_reset_closes_the_previous_handler_before_deleting_the_log(monkeypatch, tmp_path):
    target = tmp_path / "left.log"
    configure_left_logging(target, "UTC")
    old_handler = next(
        handler
        for handler in get_left_logger().handlers
        if getattr(handler, "pair_recon_left_handler", False)
    )
    original_unlink = Path.unlink
    events = []

    def unlink(path, *args, **kwargs):
        if path == target:
            assert old_handler not in get_left_logger().handlers
            assert old_handler.stream is None or old_handler.stream.closed
            events.append("unlinked")
        return original_unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", unlink)
    try:
        configure_left_logging(target, "UTC", reset=True)
        assert events == ["unlinked"]
        assert target.exists()
        assert any(
            getattr(handler, "pair_recon_left_handler", False)
            for handler in get_left_logger().handlers
        )
    finally:
        close_left_logging()
