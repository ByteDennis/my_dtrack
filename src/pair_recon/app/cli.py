import argparse
import json
import os
import sys
from pathlib import Path

import yaml

from pair_recon.core.debug import debugging, run_debugger
from pair_recon.core.metadata.workbook import (
    changed_pair_configurations,
    read_pair_workbook,
    upsert_pair_configuration,
)
from pair_recon.mail import send_outlook
from pair_recon_dbx.deploy import (
    DEFAULT_JOB_NAME,
    JOB_NAME_ENV,
    WORKSPACE_DIR_ENV,
    create_job,
    repoint_job,
    sync_right,
)
from pair_recon_dbx.workspace import resource_does_not_exist
from pair_recon_left.common.model.attribute_checks import attribute_check_names
from pair_recon_left.common.model.protocol import local_timezone_name
from pair_recon_left.common.runtime.run_logging import configure_console_logging, get_logger

from .catalog import Catalog
from .config import (
    env_value,
    load_config,
    load_defaults,
    load_environment,
    local_output_root,
    run_config,
    save_defaults,
)
from .runner import (
    StatementSql,
    launch,
    local_run_directory,
    make_run_id,
    pair_config,
    resolve_job_id,
    workspace,
    write_request_snapshot,
)

log = get_logger("cli")
DEFAULTS = Path(".pair_recon.yaml")
PAIR_WORKBOOK = Path(__file__).resolve().parents[3] / "setup" / "pairs.xlsx"
PAIR_WORKBOOK_ENV = "DBX_PAIR_WORKBOOK"


def console(message: str):
    print(message, flush=True)


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


def positive_int(label: str, default: int) -> int:
    raw = ask(label, default)
    try:
        value = int(raw)
    except (TypeError, ValueError):
        raise ValueError(f"{label} must be a positive integer") from None
    if value < 1 or str(value) != str(raw).strip():
        raise ValueError(f"{label} must be a positive integer")
    return value


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


def pair_workbook_path(defaults: dict | None = None) -> Path:
    configured = env_value(
        PAIR_WORKBOOK_ENV,
        default=str((defaults or {}).get("pair_workbook") or PAIR_WORKBOOK),
    )
    source = Path(str(configured)).expanduser()
    if not source.is_file():
        raise FileNotFoundError(f"Pair workbook not found: {source}")
    return source


def synchronize_pair_workbook(sql, catalog, pair_table: str, workbook_path: str | Path):
    source = Path(workbook_path)
    rows = read_pair_workbook(source)
    if not rows:
        return rows, {"rows": 0, "pairs": [], "source": str(source)}
    changed = changed_pair_configurations(rows, catalog.configuration_rows())
    result = (
        upsert_pair_configuration(sql, pair_table, changed)
        if changed
        else {"pair_meta_table": pair_table, "rows": 0, "sql_rows": 0, "pairs": []}
    )
    return rows, {**result, "source": str(source)}


def prompts(defaults: dict, *, workspace_factory=workspace):
    owner = ask("Run owner", os.getenv("USERNAME") or defaults.get("owner"))
    profile = ask(
        "Databricks profile",
        env_value("DBX_PROFILE", default=defaults.get("profile", "corp")),
    )
    warehouse = ask(
        "SQL warehouse id",
        env_value("DBX_WAREHOUSE_ID", default=defaults.get("warehouse_id")),
    )
    pair_table = ask(
        "Pair metadata table",
        env_value("DBX_PAIR_TABLE", default=defaults.get("pair_table")),
    )
    status_table = ask(
        "Run status table",
        env_value("DBX_STATUS_TABLE", default=defaults.get("status_table")),
    )
    error_table = ask(
        "Run error table",
        env_value("DBX_ERROR_TABLE", default=defaults.get("error_table")),
    )
    progress_default = defaults.get("progress_table")
    if not progress_default and status_table.count(".") == 2:
        progress_default = status_table.rsplit(".", 1)[0] + ".pair_recon_progress_snapshot"
    progress_table = ask("Progress snapshot table", progress_default)
    volume_root = ask(
        "Volume runs path",
        env_value("DBX_RUN_PATH", default=defaults.get("volume_root")),
    )
    email_to = ask("Outlook recipients", defaults.get("email_to"), required=False)
    workbook_path = pair_workbook_path(defaults)
    console(f"Initializing Databricks client for profile {profile!r}...")
    client = workspace_factory(profile)
    console(f"Checking {workbook_path} against {pair_table} through SQL warehouse {warehouse!r}.")
    statement_sql = StatementSql(client, warehouse, reporter=console)
    catalog = Catalog(
        statement_sql,
        pair_table,
        status_table,
        error_table,
        progress_table,
    )
    workbook_rows, sync = synchronize_pair_workbook(
        statement_sql, catalog, pair_table, workbook_path
    )
    if sync["rows"]:
        console(
            f"Updated {sync['rows']} Pair(s) from {workbook_path} in "
            f"{sync['pair_meta_table']}: {', '.join(sync['pairs'])}"
        )
    elif workbook_rows:
        console("Pair workbook metadata is already current; no remote upsert was needed.")
    else:
        console("Pair workbook has no configuration rows; remote metadata was not changed.")
    console("Loading enabled pairs, column mappings, and pair SQL from Databricks...")
    by_name = {row["pair_name"]: row for row in catalog.pairs()}
    console(f"Loaded {len(by_name)} enabled pair(s).")
    pair_names = list(by_name)
    if not pair_names:
        raise RuntimeError(f"Catalog and {workbook_path} have no enabled pair")
    saved_pair = defaults.get("pair")
    pair = choose(
        "Table pair",
        pair_names,
        saved_pair if saved_pair in pair_names else None,
    )
    metadata = pair_config(by_name[pair])
    run_defaults = metadata.get("_run_defaults") or {}
    same_pair = defaults.get("pair") == pair

    def run_default(name, fallback=None):
        if same_pair and name in defaults:
            return defaults[name]
        return run_defaults.get(name, fallback)

    run_mode = choose("Run mode", ["full", "left-only"], run_default("run_mode", "full"))
    left_source = str(metadata["left"].get("source") or "").casefold()
    right_source = str(metadata["right"].get("source") or "").casefold()
    provenances = []
    if left_source == "csv":
        provenance = csv_defaults(metadata["left"]["path"], pair, "left")
        provenances.append(provenance)
        if provenance:
            metadata = {**metadata, "pre_sampled": True}
    if run_mode == "full" and right_source == "csv":
        provenance = csv_defaults(metadata["right"]["path"], pair, "right")
        provenances.append(provenance)
        if provenance:
            metadata = {
                **metadata,
                "right_pre_sampled": True,
                "right": {**metadata["right"], "pre_sampled": True},
            }
    remote_right = run_mode == "full" and right_source == "databricks"
    right_job_id = str(
        defaults.get("right_job_id") or env_value(JOB_NAME_ENV, default=DEFAULT_JOB_NAME)
    )
    if remote_right:
        right_job_id = ask("Right job id or name", right_job_id)
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
            run_default("partition_mode", metadata.get("partition_mode", "date")),
        ),
    )
    selected = choose_many(
        "Checks",
        ["row_count", *attribute_check_names()],
        run_default("checks", "row_count,hash_sum,n_unique"),
    )
    answers = {
        "partition_mode": mode,
        "checks": selected,
        "sample": sample_value(
            ask("Sample", provenance.get("sample", run_default("sample", "all")))
        ),
        "strict": yes_no("Strict normalization", bool(run_default("strict", False))),
        "audit": yes_no("Audit SQL", bool(run_default("audit", False))),
    }
    left_workers = positive_int("Left source worker budget", run_default("left_workers", 2))
    right_workers = (
        positive_int("Databricks query worker budget", run_default("right_workers", 8))
        if remote_right
        else 1
    )
    sync_remote = (
        yes_no("Sync remote", bool(run_default("sync_remote", True))) if remote_right else False
    )
    if mode == "date":
        answers.update(
            from_date=ask("From date", provenance.get("from_date", defaults.get("from_date"))),
            to_date=ask("To date", provenance.get("to_date", defaults.get("to_date"))),
            vintage=choose(
                "Vintage",
                ["day", "week", "month", "quarter", "year"],
                provenance.get("vintage", run_default("vintage", metadata.get("vintage", "day"))),
            ),
        )
    run_id = make_run_id(pair, owner, timezone)
    existing = catalog.run(run_id)
    statement_sql.reporter = None
    action = "restart" if existing else "new"
    if existing:
        print(f"Replacing today's run: {run_id}")
    settings = {
        "owner": owner,
        "profile": profile,
        "warehouse_id": warehouse,
        "pair_table": pair_table,
        "pair_workbook": str(workbook_path),
        "status_table": status_table,
        "error_table": error_table,
        "progress_table": progress_table,
        "volume_root": volume_root,
        "right_job_id": right_job_id,
        "email_to": email_to,
        "pair": pair,
        "run_mode": run_mode,
        "run_id": run_id,
        "timezone": timezone,
        "partition_mode": mode,
        "checks": ",".join(selected),
        "sample": answers["sample"],
        "strict": answers["strict"],
        "audit": answers["audit"],
        "sync_remote": sync_remote,
        "work_dir": str(local_output_root(defaults)),
        "timeout": defaults.get("timeout", 120),
        "progress_interval": defaults.get("progress_interval", 10),
        "left_workers": left_workers,
        "right_workers": right_workers,
        "_catalog_row": by_name[pair],
    }
    for key in ("from_date", "to_date", "vintage"):
        if key in answers:
            settings[key] = answers[key]
    config = run_config(pair, metadata, answers, timezone=timezone)
    return settings, config, action, client, catalog


# >>> mirror pair_recon_right to the workspace and repoint the right job <<< #
def sync_right_package(settings: dict, client) -> dict:
    synced = sync_right(client, env_value(WORKSPACE_DIR_ENV))
    reference = str(settings.get("right_job_id") or "").strip()
    job_id = resolve_job_id(client, reference, required=False)
    if job_id is not None:
        try:
            client.jobs.get(job_id)
        except Exception as exc:
            if not resource_does_not_exist(exc):
                raise
            job_id = None
    if job_id is None:
        cluster_id = env_value("DBX_CLUSTER_ID")
        if not cluster_id:
            raise ValueError("DBX_CLUSTER_ID is required to create the Right job")
        job_name = (
            reference
            if reference and not reference.isdigit()
            else env_value(JOB_NAME_ENV, default=DEFAULT_JOB_NAME)
        )
        job_id = create_job(client, job_name, synced["notebook"], cluster_id)
        synced.update(job_id=job_id, job_created=True, job_updated=False)
    else:
        synced.update(
            job_id=job_id,
            job_created=False,
            job_updated=repoint_job(client, job_id, synced["notebook"]),
        )
    settings["right_job_id"] = str(job_id)
    return synced


def execute(args, *, raise_errors: bool = False) -> int:
    load_environment()
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
        if settings["sync_remote"]:
            if not env_value(WORKSPACE_DIR_ENV):
                raise ValueError(f"Sync remote requires {WORKSPACE_DIR_ENV} in .pair_recon.env")
            log.info("syncing Right package files to Databricks Workspace")
            synced = sync_right_package(settings, client)
            log.info("Right package sync completed: %s", json.dumps(synced, ensure_ascii=False))
        else:
            log.info("Right package sync skipped by run option")
        save_defaults(
            DEFAULTS,
            {
                key: value
                for key, value in settings.items()
                if key != "run_id" and not key.startswith("_")
            },
        )
        snapshot = write_request_snapshot(settings, config)
        log.info("request snapshot: %s", snapshot)
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
                        path := local_run_directory(
                            settings["work_dir"],
                            settings["run_id"],
                            settings["timezone"],
                        )
                        / "left.log"
                    ).is_file()
                    else (),
                )
            except Exception:
                log.exception("Outlook notification failed")
        if raise_errors:
            raise
        return 1


def main_program(argv=None) -> int:
    values = list(sys.argv[1:] if argv is None else argv)
    parser = argparse.ArgumentParser(prog="pair_recon")
    parser.add_argument("--config", type=Path)
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--debug", nargs="?", const="console", choices=("console", "attach"))
    args = parser.parse_args(values)
    configure_console_logging(args.log_level)
    raise_errors = debugging(args.debug)
    return run_debugger(
        args.debug,
        lambda: execute(args, raise_errors=raise_errors),
    )


def main(argv=None) -> int:
    values = list(sys.argv[1:] if argv is None else argv)
    if values[:1] == ["dbx"]:
        from pair_recon_dbx.cli import main as dbx_main

        return dbx_main(values[1:])
    if values[:1] == ["gitlab"]:
        from pair_recon_gitlab.cli import main as gitlab_main

        return gitlab_main(values[1:])
    return main_program(values)
