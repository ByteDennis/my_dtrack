from __future__ import annotations

import functools
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import ClassVar

import click
from click.core import UNSET
from dotenv import find_dotenv, load_dotenv

from ..backends.store import S3Store
from .config import load_config

CONTEXT_SETTINGS = {"help_option_names": ["-h", "--help"], "max_content_width": 100}
LOG_LEVELS = click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"], case_sensitive=False)


class CommaSeparatedText(click.types.StringParamType):
    envvar_list_splitter = ","

    def convert(self, value, param, ctx):
        return super().convert(value, param, ctx).strip()


class SemicolonSeparatedText(CommaSeparatedText):
    envvar_list_splitter = ";"


COMMA_TEXT = CommaSeparatedText()
SEMICOLON_TEXT = SemicolonSeparatedText()


def _display_default(value) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, (list, tuple)):
        return ", ".join(str(item) for item in value)
    return str(value)


def _configured_default(ctx, name: str | None):
    defaults = ctx.default_map or {}
    return defaults[name] if name and name in defaults else UNSET


# >>> help shows the effective safe runtime value from env/config, not only Click's static default <<< #
class EffectiveDefaultOption(click.Option):
    def value_from_envvar(self, ctx):
        configured = _configured_default(ctx, self.name)
        if configured is not UNSET:
            return None
        return super().value_from_envvar(ctx)

    def get_help_extra(self, ctx):
        extra = super().get_help_extra(ctx)
        environment = self.resolve_envvar_value(ctx)
        configured = _configured_default(ctx, self.name)
        effective = configured if configured is not UNSET else environment
        if effective is not UNSET and effective is not None:
            extra["default"] = _display_default(effective)
        return extra


def runtime_option(*param_decls, **attrs):
    attrs.setdefault("cls", EffectiveDefaultOption)
    attrs.setdefault("show_default", True)
    if attrs.get("envvar"):
        attrs.setdefault("show_envvar", True)
    return click.option(*param_decls, **attrs)


def debug_option(func):
    @runtime_option(
        "--debug",
        type=click.Choice(["ipdb", "attach"], case_sensitive=False),
        envvar="DTRACK_DEBUG",
        help="run under ipdb or wait for debugpy on port 5678",
    )
    @functools.wraps(func)
    def wrapper(*args, debug, **kwargs):
        if debug is None:
            return func(*args, **kwargs)
        from .debugging import run_under_debugger

        target = lambda _argv: func(*args, **kwargs)
        return run_under_debugger(target, [], attach=5678 if debug.lower() == "attach" else None)

    return wrapper


def config_path_from_args(args: list[str], *, env_commands: set[str] | None = None) -> str | None:
    if env_commands is not None and (not args or args[0] not in env_commands):
        return None
    for index, item in enumerate(args):
        if item == "--config" and index + 1 < len(args):
            return args[index + 1]
        if item.startswith("--config="):
            return item.split("=", 1)[1]
    return os.getenv("DTRACK_CONFIG")


def normalized_options(
    section,
    *,
    allowed: set[str],
    label: str,
    aliases: dict[str, str] | None = None,
    comma_lists: set[str] | None = None,
    repeatables: set[str] | None = None,
) -> dict:
    if not isinstance(section, dict):
        raise click.UsageError(f"config {label} must be an object")
    aliases = aliases or {}
    comma_lists = comma_lists or set()
    repeatables = repeatables or set()
    values = {}
    for key, value in section.items():
        name = aliases.get(key.replace("-", "_"), key.replace("-", "_"))
        if name not in allowed:
            raise click.UsageError(f"config {label} does not support: {key}")
        if name in comma_lists and isinstance(value, str):
            value = [item.strip() for item in value.split(",") if item.strip()]
        elif name in repeatables and isinstance(value, str):
            value = [value]
        values[name] = value
    return values


class ConfigDefaultsGroup(click.Group):
    env_commands: set[str] | None = None

    def config_defaults(self, document: dict) -> dict[str, dict]:
        raise NotImplementedError

    def make_context(self, info_name, args, parent=None, **extra):
        load_dotenv(find_dotenv(usecwd=True))
        path = config_path_from_args(args, env_commands=self.env_commands)
        if path:
            try:
                with Path(path).open(encoding="utf-8") as handle:
                    defaults = self.config_defaults(json.load(handle))
            except (OSError, json.JSONDecodeError) as exc:
                raise click.UsageError(f"cannot read config defaults from {path}: {exc}") from None
            inherited = extra.get("default_map") or {}
            extra["default_map"] = {
                command: {**values, **(inherited.get(command) or {})}
                for command, values in defaults.items()
            }
        return super().make_context(info_name, args, parent=parent, **extra)


DISTRIBUTED_OPTIONS = {
    "left-run": {
        "store_uri",
        "run_id",
        "pairs",
        "export_sas",
        "import_sas",
        "workers",
        "work_dir",
        "pull_workers",
        "stat_workers",
        "wip",
        "force",
        "log_level",
        "debug",
    },
    "row-check": {
        "store_uri",
        "run_id",
        "pairs",
        "workers",
        "force",
        "log_level",
        "debug",
    },
    "col-check": {
        "store_uri",
        "run_id",
        "pairs",
        "work_dir",
        "pull_workers",
        "stat_workers",
        "wip",
        "wait_timeout",
        "poll_interval",
        "force",
        "log_level",
        "debug",
    },
}


class DistributedGroup(ConfigDefaultsGroup):
    env_commands: ClassVar[set[str]] = {"left-run", "row-check", "col-check"}

    def config_defaults(self, document: dict) -> dict[str, dict]:
        return {
            command: normalized_options(
                document.get(command) or {},
                allowed=allowed,
                label=command,
                aliases={"pair": "pairs", "run_s3": "store_uri"},
                comma_lists={"pairs"},
            )
            for command, allowed in DISTRIBUTED_OPTIONS.items()
        }


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
log_level_option = runtime_option(
    "--log-level",
    type=LOG_LEVELS,
    default="INFO",
    envvar="DTRACK_LOG_LEVEL",
    help="log verbosity on stderr",
    expose_value=False,
    is_eager=True,
    callback=lambda ctx, param, value: setup_logging(value),
)


# >>> --config PATH loaded and validated into a `config` dict <<< #
def config_option(func):
    @runtime_option(
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
    flag: str = "--run-s3",
    envvar: str = "DTRACK_RUN_S3",
    help: str = "runs root, s3://bucket/prefix/runs (run_id is appended)",
):
    def decorator(func):
        @runtime_option(flag, "store_uri", required=True, envvar=envvar, help=help)
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
    @runtime_option(
        "--pair",
        "pairs",
        multiple=True,
        type=COMMA_TEXT,
        envvar="DTRACK_PAIRS",
        help="limit to this pair (repeatable; env uses comma-separated names)",
    )
    @functools.wraps(func)
    def wrapper(*args, pairs, **kwargs):
        return func(*args, pairs=list(pairs) or None, **kwargs)

    return wrapper
