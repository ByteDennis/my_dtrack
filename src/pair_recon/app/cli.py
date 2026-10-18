import argparse
import json
from pathlib import Path

from dotenv import find_dotenv, load_dotenv
from tzlocal import get_localzone_name

from pair_recon.mail import send_outlook
from pair_recon_left.common.runtime.run_logging import configure_console_logging, get_logger

from .catalog import Catalog
from .config import load_defaults, run_config, save_defaults
from .runner import StatementSql, launch, make_run_id, pair_config, workspace

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


def yes_no(label: str, default=False) -> bool:
    answer = ask(label, "yes" if default else "no").lower()
    if answer not in {"y", "yes", "n", "no"}:
        raise ValueError("answer yes or no")
    return answer in {"y", "yes"}


def timezone_default() -> str:
    return get_localzone_name()


def sample_value(value: str):
    text = value.strip().lower()
    if text in {"", "all", "false", "none"}:
        return False
    number = float(text)
    return int(number) if number.is_integer() and number > 1 else number


def prompts(defaults: dict, *, workspace_factory=workspace):
    owner = ask("Run owner", defaults.get("owner"))
    profile = ask("Databricks profile", defaults.get("profile", "corp"))
    warehouse = ask("SQL warehouse id", defaults.get("warehouse_id"))
    pair_table = ask("Pair metadata table", defaults.get("pair_table"))
    status_table = ask("Run status table", defaults.get("status_table"))
    error_table = ask("Run error table", defaults.get("error_table"))
    volume_root = ask("Volume runs path", defaults.get("volume_root"))
    right_job_id = ask("Right job id", defaults.get("right_job_id"))
    email_to = ask("Outlook recipients", defaults.get("email_to"), required=False)
    client = workspace_factory(profile)
    catalog = Catalog(StatementSql(client, warehouse), pair_table, status_table, error_table)
    rows = catalog.pairs()
    by_name = {row["pair_name"]: row for row in rows}
    if not by_name:
        raise RuntimeError("pair metadata has no enabled pair")
    pair = choose("Table pair", list(by_name), defaults.get("pair"))
    metadata = pair_config(by_name[pair])
    timezone = ask("IANA timezone", defaults.get("timezone", timezone_default()))
    mode = choose(
        "Partition mode",
        ["date", "all"],
        defaults.get("partition_mode", metadata.get("partition_mode", "date")),
    )
    checks = ask("Checks", defaults.get("checks", "row_count,hash_sum,n_unique"))
    selected = [value.strip() for value in checks.split(",") if value.strip()]
    allowed = {"row_count", "hash_sum", "n_unique"}
    if not selected or set(selected) - allowed:
        raise ValueError("checks must use row_count, hash_sum, n_unique")
    answers = {
        "partition_mode": mode,
        "checks": selected,
        "sample": sample_value(ask("Sample", defaults.get("sample", "all"))),
        "strict": yes_no("Strict normalization", bool(defaults.get("strict", False))),
    }
    if mode == "date":
        answers.update(
            from_date=ask("From date", defaults.get("from_date")),
            to_date=ask("To date", defaults.get("to_date")),
            vintage=choose(
                "Vintage",
                ["day", "week", "month", "quarter", "year"],
                defaults.get("vintage", metadata.get("vintage", "day")),
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
        "volume_root": volume_root,
        "right_job_id": right_job_id,
        "email_to": email_to,
        "pair": pair,
        "run_id": run_id,
        "last_run_id": run_id,
        "timezone": timezone,
        "partition_mode": mode,
        "checks": ",".join(selected),
        "sample": answers["sample"],
        "strict": answers["strict"],
        "work_dir": defaults.get("work_dir", ".pair_recon-work"),
        "timeout": defaults.get("timeout", 120),
    }
    for key in ("from_date", "to_date", "vintage"):
        if key in answers:
            settings[key] = answers[key]
    config = run_config(pair, metadata, answers, timezone=timezone)
    return settings, config, action, client, catalog


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="pair_recon")
    parser.add_argument("--defaults", type=Path, default=DEFAULTS)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)
    configure_console_logging(args.log_level)
    load_dotenv(find_dotenv(filename=".pair_recon.env", usecwd=True))
    settings = None
    try:
        settings, config, action, client, catalog = prompts(load_defaults(args.defaults))
        configure_console_logging(args.log_level, settings["timezone"])
        print(
            json.dumps(
                {"settings": settings, "pair": next(iter(config["pairs"]))},
                ensure_ascii=False,
                indent=2,
            )
        )
        if not yes_no("Run now", True):
            return 0
        save_defaults(
            args.defaults, {key: value for key, value in settings.items() if key != "run_id"}
        )
        result = launch(settings, config, action=action, workspace_client=client, catalog=catalog)
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
