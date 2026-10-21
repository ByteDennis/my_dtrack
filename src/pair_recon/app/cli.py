import argparse
import json
import os
import sys
from pathlib import Path

import yaml
from dotenv import find_dotenv, load_dotenv

from pair_recon.core.debug import run_debugger
from pair_recon.mail import send_outlook
from pair_recon_left.common.model.attribute_checks import attribute_check_names
from pair_recon_left.common.model.protocol import local_timezone_name
from pair_recon_left.common.runtime.run_logging import configure_console_logging, get_logger

from .catalog import Catalog
from .config import load_config, load_defaults, local_output_root, run_config, save_defaults
from .runner import (
    StatementSql,
    launch,
    make_run_id,
    pair_config,
    workspace,
    write_request_snapshot,
)

log = get_logger("cli")
DEFAULTS = Path(".pair_recon.yaml")


def ask(label: str, default=None, *, required=True) -> str:
    shown = "" if default in (None, "") else f" [{default}]"
    value = input(f"{label}{shown}: ").strip()
    if value:
        return value
    if default not in (None, ""):
        return str(default)
    if required:
        raise ValueError(f"{label} is required")
    return ""


def choose(label: str, values: list[str], default=None) -> str:
    for index, value in enumerate(values, 1):
        print(f"  {index}. {value}")
    answer = ask(label, default or values[0])
    if answer.isdigit() and 1 <= int(answer) <= len(values):
        return values[int(answer) - 1]
    if answer not in values:
        raise ValueError(f"choose one of: {', '.join(values)}")
    return answer


def choose_many(label: str, values: list[str], default) -> list[str]:
    raw = default if isinstance(default, list | tuple) else str(default).split(",")
    selected = {str(value).strip() for value in raw if str(value).strip()}
    unknown = selected - set(values)
    if unknown:
        raise ValueError(f"unknown default choices: {', '.join(sorted(unknown))}")
    while True:
        print(label)
        for index, value in enumerate(values, 1):
            print(f"  [{'x' if value in selected else ' '}] {index}. {value}")
        answer = input("Toggle by number/name, comma-separated; Enter confirms: ").strip()
        if not answer:
            if not selected:
                raise ValueError(f"choose one or more of: {', '.join(values)}")
            return [value for value in values if value in selected]
        toggles = set()
        for item in (value.strip() for value in answer.split(",")):
            choice = (
                values[int(item) - 1] if item.isdigit() and 1 <= int(item) <= len(values) else item
            )
            if choice not in values:
                raise ValueError(f"choose one or more of: {', '.join(values)}")
            toggles.add(choice)
        selected.symmetric_difference_update(toggles)


def yes_no(label: str, default=False) -> bool:
    answer = ask(label, "yes" if default else "no").lower()
    if answer not in {"y", "yes", "n", "no"}:
        raise ValueError("answer yes or no")
    return answer in {"y", "yes"}


def timezone_default() -> str:
    return local_timezone_name()


def sample_value(value: str):
    text = value.strip().lower()
    if text in {"", "all", "false", "none"}:
        return False
    number = float(text)
    return int(number) if number.is_integer() and number > 1 else number


def csv_defaults(path: str, pair: str, side: str) -> dict:
    request = Path(path).with_name("request.yaml")
    if not request.is_file():
        return {}
    value = yaml.safe_load(request.read_text(encoding="utf-8")) or {}
    if value.get("format") != "pair-recon-sas-v1" or value.get("pair") != pair:
        return {}
    generated = value.get("generated") or {}
    if side not in generated or (value.get("outputs") or {}).get(side) != Path(path).name:
        return {}
    rate = generated[side].get("rate", value.get("rate"))
    return {**value, "sample": rate}


def prompts(defaults: dict, *, workspace_factory=workspace):
    owner = ask("Run owner", os.getenv("USERNAME") or defaults.get("owner"))
    profile = ask("Databricks profile", defaults.get("profile", "corp"))
    warehouse = ask("SQL warehouse id", defaults.get("warehouse_id"))
    pair_table = ask("Pair metadata table", defaults.get("pair_table"))
    status_table = ask("Run status table", defaults.get("status_table"))
    error_table = ask("Run error table", defaults.get("error_table"))
    progress_default = defaults.get("progress_table")
    if not progress_default and status_table.count(".") == 2:
        progress_default = status_table.rsplit(".", 1)[0] + ".pair_recon_progress_snapshot"
    progress_table = ask("Progress snapshot table", progress_default)
    volume_root = ask("Volume runs path", defaults.get("volume_root"))
    email_to = ask("Outlook recipients", defaults.get("email_to"), required=False)
    client = workspace_factory(profile)
    catalog = Catalog(
        StatementSql(client, warehouse),
        pair_table,
        status_table,
        error_table,
        progress_table,
    )
    rows = catalog.pairs()
    by_name = {row["pair_name"]: row for row in rows}
    if not by_name:
        raise RuntimeError("pair metadata has no enabled pair")
    pair = choose("Table pair", list(by_name), defaults.get("pair"))
    metadata = pair_config(by_name[pair])
    run_mode = choose("Run mode", ["full", "left-only"], defaults.get("run_mode", "full"))
    left_input = choose(
        "Left input", ["catalog", "hadoop", "csv"], defaults.get("left_input", "catalog")
    )
    left_csv = str(defaults.get("left_csv") or "")
    right_csv = str(defaults.get("right_csv") or "")
    hadoop_schema = str(defaults.get("hadoop_schema") or "")
    provenances = []
    if left_input == "csv":
        left_csv = ask("Left CSV path", left_csv)
        provenances.append(csv_defaults(left_csv, pair, "left"))
        inherited = metadata["left"]
        metadata = {
            **metadata,
            "pre_sampled": True,
            "left": {
                "source": "csv",
                "path": left_csv,
                **{
                    name: inherited[name] for name in ("date_col", "date_type") if name in inherited
                },
            },
        }
    elif left_input == "hadoop":
        hadoop_schema = ask("Hadoop schema", hadoop_schema or metadata["left"].get("schema"))
        inherited = metadata["left"]
        if not inherited.get("table"):
            raise ValueError("Hadoop override requires a table in pair metadata")
        metadata = {
            **metadata,
            "left": {
                "source": "hadoop",
                "schema": hadoop_schema,
                "table": inherited["table"],
                **{
                    name: inherited[name]
                    for name in ("date_col", "date_type", "where", "dsn", "autocommit")
                    if name in inherited
                },
            },
        }
    saved_right_input = str(defaults.get("right_input") or "catalog")
    if saved_right_input not in {"catalog", "csv"}:
        saved_right_input = "catalog"
    right_input = "none"
    right_job_id = str(defaults.get("right_job_id") or "")
    if run_mode == "full":
        right_input = choose("Right input", ["catalog", "csv"], saved_right_input)
        if right_input == "csv":
            if not right_csv and left_csv:
                right_csv = str(Path(left_csv).with_name("right_sample.csv"))
            right_csv = ask("Right CSV path", right_csv)
            provenances.append(csv_defaults(right_csv, pair, "right"))
            inherited = metadata["right"]
            metadata = {
                **metadata,
                "right_pre_sampled": True,
                "right": {
                    "source": "csv",
                    "path": right_csv,
                    "pre_sampled": True,
                    **{
                        name: inherited[name]
                        for name in ("date_col", "date_type")
                        if name in inherited
                    },
                },
            }
        else:
            right_job_id = ask("Right job id", right_job_id)
    provenances = [value for value in provenances if value]
    provenance = provenances[0] if provenances else {}
    for value in provenances[1:]:
        fields = ("partition_mode", "from_date", "to_date", "vintage", "sample")
        if any(value.get(field) != provenance.get(field) for field in fields):
            raise ValueError("Left and Right CSV provenance does not match")
    timezone = ask("IANA timezone", defaults.get("timezone", timezone_default()))
    mode = choose(
        "Partition mode",
        ["date", "all"],
        provenance.get(
            "partition_mode",
            defaults.get("partition_mode", metadata.get("partition_mode", "date")),
        ),
    )
    selected = choose_many(
        "Checks",
        ["row_count", *attribute_check_names()],
        defaults.get("checks", "row_count,hash_sum,n_unique"),
    )
    answers = {
        "partition_mode": mode,
        "checks": selected,
        "sample": sample_value(
            ask("Sample", provenance.get("sample", defaults.get("sample", "all")))
        ),
        "strict": yes_no("Strict normalization", bool(defaults.get("strict", False))),
    }
    if mode == "date":
        answers.update(
            from_date=ask("From date", provenance.get("from_date", defaults.get("from_date"))),
            to_date=ask("To date", provenance.get("to_date", defaults.get("to_date"))),
            vintage=choose(
                "Vintage",
                ["day", "week", "month", "quarter", "year"],
                provenance.get("vintage", defaults.get("vintage", metadata.get("vintage", "day"))),
            ),
        )
    generated = make_run_id(pair, owner, timezone)
    if defaults.get("last_run_id"):
        print(f"Previous run id: {defaults['last_run_id']}")
    run_id = ask("Run id", generated)
    existing = catalog.run(run_id)
    action = (
        choose("Existing run action", ["resume", "restart", "cancel"], "resume")
        if existing
        else "new"
    )
    if action == "cancel":
        raise KeyboardInterrupt
    settings = {
        "owner": owner,
        "profile": profile,
        "warehouse_id": warehouse,
        "pair_table": pair_table,
        "status_table": status_table,
        "error_table": error_table,
        "progress_table": progress_table,
        "volume_root": volume_root,
        "right_job_id": right_job_id,
        "email_to": email_to,
        "pair": pair,
        "run_mode": run_mode,
        "left_input": left_input,
        "right_input": right_input,
        "left_csv": left_csv,
        "right_csv": right_csv,
        "hadoop_schema": hadoop_schema,
        "run_id": run_id,
        "last_run_id": run_id,
        "timezone": timezone,
        "partition_mode": mode,
        "checks": ",".join(selected),
        "sample": answers["sample"],
        "strict": answers["strict"],
        "work_dir": str(local_output_root(defaults)),
        "timeout": defaults.get("timeout", 120),
        "progress_interval": defaults.get("progress_interval", 10),
        "pull_workers": defaults.get("pull_workers", 2),
        "_catalog_row": by_name[pair],
    }
    for key in ("from_date", "to_date", "vintage"):
        if key in answers:
            settings[key] = answers[key]
    config = run_config(pair, metadata, answers, timezone=timezone)
    return settings, config, action, client, catalog


def execute(args) -> int:
    load_dotenv(find_dotenv(filename=".pair_recon.env", usecwd=True))
    settings = None
    try:
        defaults = load_config(args.config) if args.config else load_defaults(DEFAULTS)
        settings, config, action, client, catalog = prompts(defaults)
        configure_console_logging(args.log_level, settings["timezone"])
        print(
            json.dumps(
                {
                    "settings": {
                        key: value for key, value in settings.items() if not key.startswith("_")
                    },
                    "pair": next(iter(config["pairs"])),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        if not yes_no("Run now", True):
            return 0
        snapshot = write_request_snapshot(settings, config)
        log.info("request snapshot: %s", snapshot)
        save_defaults(
            DEFAULTS,
            {
                key: value
                for key, value in settings.items()
                if key != "run_id" and not key.startswith("_")
            },
        )
        result = launch(settings, config, action=action, workspace_client=client, catalog=catalog)
        result["request_snapshot"] = str(snapshot)
        if settings.get("email_to"):
            try:
                result["email"] = send_outlook(
                    settings["email_to"],
                    f"pair_recon {result['state']}: {result['pair']} {result['run_id']}",
                    json.dumps(result, ensure_ascii=False, indent=2),
                    [result["left_log"]],
                )
            except Exception as exc:
                log.exception("Outlook notification failed")
                result["email"] = {"status": "failed", "error": str(exc)}
                print(json.dumps(result, ensure_ascii=False, indent=2))
                return 2
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except KeyboardInterrupt:
        print("Cancelled")
        return 130
    except Exception as exc:
        log.exception("Run failed")
        if settings and settings.get("email_to"):
            try:
                send_outlook(
                    settings["email_to"],
                    f"pair_recon failed: {settings['pair']} {settings['run_id']}",
                    str(exc),
                    [path]
                    if (
                        path := Path(settings["work_dir"]) / settings["run_id"] / "left.log"
                    ).is_file()
                    else (),
                )
            except Exception:
                log.exception("Outlook notification failed")
        return 1


def main_program(argv=None) -> int:
    values = list(sys.argv[1:] if argv is None else argv)
    parser = argparse.ArgumentParser(prog="pair_recon")
    parser.add_argument("--config", type=Path)
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--debug", nargs="?", const="ipdb", choices=("ipdb", "attach"))
    args = parser.parse_args(values)
    configure_console_logging(args.log_level)
    return run_debugger(args.debug, lambda: execute(args))


def main(argv=None) -> int:
    values = list(sys.argv[1:] if argv is None else argv)
    if values[:1] == ["dbx"]:
        from pair_recon_dbx.cli import main as dbx_main

        return dbx_main(values[1:])
    if values[:1] == ["gitlab"]:
        from pair_recon_gitlab.cli import main as gitlab_main

        return gitlab_main(values[1:])
    return main_program(values)
