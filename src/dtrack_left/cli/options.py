from __future__ import annotations

import functools
import json
import logging
import sys
import time

import click

from ..backends.store import S3Store
from .config import load_config

CONTEXT_SETTINGS = {"help_option_names": ["-h", "--help"], "max_content_width": 100}
LOG_LEVELS = click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"], case_sensitive=False)


# >>> UTC timestamps on stderr, so stdout stays one JSON line per command <<< #
def setup_logging(level: str) -> None:
    logging.Formatter.converter = time.gmtime
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s.%(msecs)03dZ %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stderr,
    )


# >>> --log-level on every command, applied before the command body runs <<< #
log_level_option = click.option(
    "--log-level",
    type=LOG_LEVELS,
    default="INFO",
    show_default=True,
    envvar="DTRACK_LOG_LEVEL",
    help="log verbosity on stderr",
    expose_value=False,
    is_eager=True,
    callback=lambda ctx, param, value: setup_logging(value),
)


# >>> --config PATH loaded and validated into a `config` dict <<< #
def config_option(func):
    @click.option(
        "--config",
        "config",
        required=True,
        type=click.Path(exists=True, dir_okay=False),
        envvar="DTRACK_CONFIG",
        help="pairs config JSON",
    )
    @functools.wraps(func)
    def wrapper(*args, config, **kwargs):
        return func(*args, config=load_config(config), **kwargs)

    return wrapper


# >>> an S3 location option (named by `flag`) opened as a `store` <<< #
def store_option(
    flag: str = "--s3",
    envvar: str = "DTRACK_S3",
    help: str = "run location, s3://bucket/prefix/<run>",
):
    def decorator(func):
        @click.option(flag, "store_uri", required=True, envvar=envvar, help=help)
        @functools.wraps(func)
        def wrapper(*args, store_uri, **kwargs):
            return func(*args, store=S3Store(store_uri), **kwargs)

        return wrapper

    return decorator


# >>> print the command's dict as one JSON line; its `exit_code` key (default 0) becomes the exit code <<< #
def emit_json(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        outcome = dict(func(*args, **kwargs))
        code = outcome.pop("exit_code", 0)
        click.echo(json.dumps(outcome, sort_keys=True))
        return code

    return wrapper


# >>> --pair NAME, repeatable, as a list or None <<< #
def pair_option(func):
    @click.option("--pair", "pairs", multiple=True, help="limit to this pair (repeatable)")
    @functools.wraps(func)
    def wrapper(*args, pairs, **kwargs):
        return func(*args, pairs=list(pairs) or None, **kwargs)

    return wrapper
