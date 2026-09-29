import json
import logging

import click
from click.testing import CliRunner

from dtrack_left.backends.store import S3Store
from dtrack_left.cli import options


# >>> a throwaway command wired with every shared decorator, echoing what it received <<< #
@click.command(context_settings=options.CONTEXT_SETTINGS)
@options.store_option("--where", "DTRACK_UNIT_WHERE", "somewhere")
@options.pair_option
@click.option("--code", default=0)
@options.log_level_option
@options.emit_json
def probe(store, pairs, code):
    return {
        "type": type(store).__name__,
        "uri": store.root_uri,
        "pairs": pairs,
        "exit_code": code,
    }


def _invoke(args, **env):
    return CliRunner().invoke(probe, args, env=env, standalone_mode=False)


def test_decorators_build_the_store_and_pair_list_and_print_one_json_line():
    result = _invoke(
        ["--where", "s3://b/p", "--pair", "a", "--pair", "b"],
        AWS_DEFAULT_REGION="us-east-1",
    )
    assert result.return_value == 0
    assert json.loads(result.output) == {
        "pairs": ["a", "b"],
        "type": S3Store.__name__,
        "uri": "s3://b/p",
    }


def test_options_fall_back_to_environment_variables_and_exit_code_is_returned():
    result = _invoke(
        ["--code", "3"], DTRACK_UNIT_WHERE="s3://env/p", AWS_DEFAULT_REGION="us-east-1"
    )
    assert result.return_value == 3
    assert json.loads(result.output)["uri"] == "s3://env/p"
    assert json.loads(result.output)["pairs"] is None


def test_log_level_is_applied_before_the_command_body(monkeypatch):
    seen = []
    monkeypatch.setattr(options, "setup_logging", seen.append)
    _invoke(["--where", "s3://b/p", "--log-level", "warning"], AWS_DEFAULT_REGION="us-east-1")
    assert seen == ["WARNING"]


def test_setup_logging_writes_utc_timestamps_to_stderr(monkeypatch):
    root = logging.getLogger()
    monkeypatch.setattr(root, "handlers", [])
    monkeypatch.setattr(root, "level", root.level)
    options.setup_logging("debug")
    assert root.level == logging.DEBUG
    assert root.handlers[0].formatter._fmt.startswith("%(asctime)s.%(msecs)03dZ")
