import argparse
import json
import os
import sys
from contextvars import ContextVar
from datetime import date, timedelta
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
from pair_recon_left.common.sql.dates import contiguous_runs, resolve_date_type

from .catalog import Catalog
from .config import (
    env_boolean,
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
    mismatched_partitions,
    pair_config,
    resolve_job_id,
    workspace,
    write_request_snapshot,
)

log = get_logger("cli")
DEFAULTS = Path(".pair_recon.yaml")
PAIR_WORKBOOK = Path(__file__).resolve().parents[3] / "setup" / "pairs.xlsx"
PAIR_WORKBOOK_ENV = "DBX_PAIR_WORKBOOK"
SYNC_PAIR_METADATA_ENV = "DBX_SYNC_PAIR_METADATA"
AUTO_EXCLUDE_RETRIES = 3


class UndoPrompt(Exception):
    pass


class RestartPrompts(Exception):
    pass


class PromptSession:
    def __init__(self):
        self.answers = []
        self.cache = {}
        self.cursor = 0
        self.replay_target = None

    def begin(self):
        self.cursor = 0

    def _step(self, default, prompt):
        index = self.cursor
        self.cursor += 1
        if self.replay_target is not None and index < self.replay_target:
            return self.answers[index]
        selected_default = self.answers[index] if index < len(self.answers) else default
        try:
            value = prompt(selected_default)
        except UndoPrompt:
            self.replay_target = max(0, index - 1)
            raise RestartPrompts from None
        if index < len(self.answers):
            self.answers[index] = value
            del self.answers[index + 1 :]
        else:
            self.answers.append(value)
        if self.replay_target == index:
            self.replay_target = None
        return value

    def answer(self, label, default=None, *, required=True):
        return self._step(default, lambda value: ask(label, value, required=required))

    def select(self, label, values, default=None):
        return self._step(default, lambda value: choose(label, values, value))

    def select_many(self, label, values, default):
        return self._step(default, lambda value: choose_many(label, values, value))

    def confirm(self, label, default=False):
        return self._step(default, lambda value: yes_no(label, value))

    def worker_budget(self, label, default):
        return self._step(default, lambda value: positive_int(label, value))


_PROMPT_SESSION = ContextVar("pair_recon_prompt_session", default=None)


def console(message: str):
    print(message, flush=True)


def _read_prompt(message: str, default: str) -> tuple[str, bool]:
    if sys.stdin.isatty() and sys.stdout.isatty():
        from prompt_toolkit import prompt

        return prompt(message, default=default), True
    return input(message), False


def ask(label: str, default=None, *, required=True) -> str:
    shown = "" if default in (None, "") else f" [{default}]"
    initial = "" if default in (None, "") else str(default)
    value, editable = _read_prompt(f"{label}{shown}: ", initial)
    value = value.strip()
    if value.casefold() == "undo":
        raise UndoPrompt
    if value:
        return value
    if editable:
        if required:
            raise ValueError(f"{label} is required")
        return ""
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
    print(label)
    for index, value in enumerate(values, 1):
        print(f"  [{'x' if value in selected else ' '}] {index}. {value}")
    answer = ask(
        "Selections (comma-separated names or numbers)",
        ",".join(value for value in values if value in selected),
    )
    chosen = set()
    for item in (value.strip() for value in answer.split(",")):
        choice = values[int(item) - 1] if item.isdigit() and 1 <= int(item) <= len(values) else item
        if choice not in values:
            raise ValueError(f"choose one or more of: {', '.join(values)}")
        chosen.add(choice)
    return [value for value in values if value in chosen]


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


def date_type_text(value) -> str:
    selected = "string" if value in (None, "") else value
    if isinstance(selected, dict):
        return json.dumps(selected, ensure_ascii=False, separators=(",", ":"))
    return str(selected)


def date_type_value(value):
    selected = value
    if isinstance(value, str):
        text = value.strip()
        selected = json.loads(text) if text.startswith("{") else text
    return resolve_date_type(selected).spec(portable=True)


def excluded_partition_values(value, date_type=None) -> list[str]:
    if value in (None, ""):
        return []
    raw_items = value if isinstance(value, list | tuple) else [value]
    items = [part.strip() for raw in raw_items for part in str(raw).split(",") if part.strip()]
    expanded = []

    def iso_day(text: str) -> date:
        try:
            parsed = date.fromisoformat(text)
        except ValueError:
            parsed = None
        if parsed is None or parsed.isoformat() != text:
            raise ValueError(
                "Excluded partitions must use YYYY-MM-DD or YYYY-MM-DD->YYYY-MM-DD, comma-separated"
            )
        return parsed

    for item in items:
        if "->" not in item:
            expanded.append(iso_day(item).isoformat())
            continue
        bounds = [part.strip() for part in item.split("->")]
        if len(bounds) != 2:
            raise ValueError(f"Invalid excluded partition range: {item!r}")
        start, end = map(iso_day, bounds)
        if start > end:
            raise ValueError(f"Excluded partition range starts after it ends: {item!r}")
        expanded.extend(
            (start + timedelta(days=offset)).isoformat() for offset in range((end - start).days + 1)
        )
    partition_type = resolve_date_type(date_type)
    return sorted({partition_type.partition_key(value) for value in expanded})


def excluded_partitions_text(value, date_type=None) -> str:
    values = excluded_partition_values(value, date_type)
    return ", ".join(
        start if start == end else f"{start}->{end}" for start, end in contiguous_runs(values)
    )


def positive_int(label: str, default: int) -> int:
    raw = ask(label, default)
    try:
        value = int(raw)
    except (TypeError, ValueError):
        raise ValueError(f"{label} must be a positive integer") from None
    if value < 1 or str(value) != str(raw).strip():
        raise ValueError(f"{label} must be a positive integer")
    return value


def default_answer(label: str, default=None, *, required=True) -> str:
    if default not in (None, ""):
        return str(default)
    if required:
        raise ValueError(f"{label} is required in --defaults mode")
    return ""


def default_choice(label: str, values: list[str], default=None) -> str:
    if default in (None, ""):
        raise ValueError(f"{label} is required in --defaults mode")
    selected = str(default)
    if selected not in values:
        raise ValueError(f"{label} must be one of: {', '.join(values)}")
    return selected


def default_choices(label: str, values: list[str], default) -> list[str]:
    raw = default if isinstance(default, list | tuple) else str(default).split(",")
    selected = {str(value).strip() for value in raw if str(value).strip()}
    unknown = selected - set(values)
    if unknown:
        raise ValueError(f"unknown {label} choices: {', '.join(sorted(unknown))}")
    if not selected:
        raise ValueError(f"{label} requires one or more of: {', '.join(values)}")
    return [value for value in values if value in selected]


def default_positive_int(label: str, default: int) -> int:
    try:
        value = int(default)
    except (TypeError, ValueError):
        raise ValueError(f"{label} must be a positive integer") from None
    if value < 1 or isinstance(default, float) and not default.is_integer():
        raise ValueError(f"{label} must be a positive integer")
    return value


def default_boolean(label: str, default=False) -> bool:
    if not isinstance(default, bool):
        raise TypeError(f"{label} must be true or false in --defaults mode")
    return default


def right_job_reference(value: object) -> str:
    reference = str(value or "").strip()
    if not reference or reference.casefold() in {"-", "clear"}:
        return str(env_value(JOB_NAME_ENV, default=DEFAULT_JOB_NAME))
    return reference


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
    return Path(str(configured)).expanduser()


def synchronize_pair_workbook(sql, catalog, pair_table: str, workbook_path: str | Path):
    source = Path(workbook_path)
    if not source.exists():
        return [], {
            "rows": 0,
            "pairs": [],
            "source": str(source),
            "skipped": "missing",
        }
    if not source.is_file():
        raise FileNotFoundError(f"Pair workbook is not a file: {source}")
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


def prompts(defaults: dict, *, workspace_factory=workspace, interactive: bool = True):
    session = _PROMPT_SESSION.get() if interactive else None
    if interactive:
        session = session or PromptSession()
        answer = session.answer
        select = session.select
        select_many = session.select_many
        confirm = session.confirm
        worker_budget = session.worker_budget
    else:
        answer = default_answer
        select = default_choice
        select_many = default_choices
        confirm = default_boolean
        worker_budget = default_positive_int

    owner = answer("Run owner", os.getenv("USERNAME") or defaults.get("owner"))
    profile = answer(
        "Databricks profile",
        env_value("DBX_PROFILE", default=defaults.get("profile", "corp")),
    )
    warehouse = answer(
        "SQL warehouse id",
        env_value("DBX_WAREHOUSE_ID", default=defaults.get("warehouse_id")),
    )
    pair_table = answer(
        "Pair metadata table",
        env_value("DBX_PAIR_TABLE", default=defaults.get("pair_table")),
    )
    status_table = answer(
        "Run status table",
        env_value("DBX_STATUS_TABLE", default=defaults.get("status_table")),
    )
    error_table = answer(
        "Run error table",
        env_value("DBX_ERROR_TABLE", default=defaults.get("error_table")),
    )
    progress_default = defaults.get("progress_table")
    if not progress_default and status_table.count(".") == 2:
        progress_default = status_table.rsplit(".", 1)[0] + ".pair_recon_progress_snapshot"
    progress_table = answer("Progress snapshot table", progress_default)
    volume_root = answer(
        "Volume runs path",
        env_value("DBX_RUN_PATH", default=defaults.get("volume_root")),
    )
    email_to = answer("Outlook recipients", defaults.get("email_to"), required=False)
    workbook_path = pair_workbook_path(defaults)
    sync_pair_metadata = env_boolean(SYNC_PAIR_METADATA_ENV, default=True)
    metadata_key = (
        profile,
        warehouse,
        pair_table,
        status_table,
        error_table,
        progress_table,
        str(workbook_path),
        sync_pair_metadata,
    )
    cached = session.cache.get("metadata") if session else None
    if cached and cached[0] == metadata_key:
        client, statement_sql, catalog, by_name = cached[1:]
        statement_sql.reporter = console
    else:
        console(f"Initializing Databricks client for profile {profile!r}...")
        client = workspace_factory(profile)
        if sync_pair_metadata and workbook_path.exists():
            console(
                f"Checking {workbook_path} against {pair_table} through SQL warehouse {warehouse!r}."
            )
        statement_sql = StatementSql(client, warehouse, reporter=console)
        catalog = Catalog(
            statement_sql,
            pair_table,
            status_table,
            error_table,
            progress_table,
        )
        workbook_rows = []
        sync = {"rows": 0, "pairs": [], "source": str(workbook_path)}
        if sync_pair_metadata:
            workbook_rows, sync = synchronize_pair_workbook(
                statement_sql, catalog, pair_table, workbook_path
            )
        if not sync_pair_metadata:
            console(
                f"{SYNC_PAIR_METADATA_ENV}=false; skipping local workbook synchronization "
                "and using Databricks metadata as the source of truth."
            )
        elif sync.get("skipped") == "missing":
            console(
                f"Pair workbook not found at {workbook_path}; skipping workbook synchronization "
                "and reading existing metadata from Databricks."
            )
        elif sync["rows"]:
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
        if session:
            session.cache["metadata"] = (metadata_key, client, statement_sql, catalog, by_name)
    pair_names = list(by_name)
    if not pair_names:
        raise RuntimeError(f"Catalog and {workbook_path} have no enabled pair")
    saved_pair = defaults.get("pair")
    pair = select(
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

    run_mode = select("Run mode", ["full", "left-only"], run_default("run_mode", "full"))
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
        entered = answer("Right job id or name", right_job_id, required=False)
        right_job_id = right_job_reference(entered)
        if entered != right_job_id:
            console(f"Saved Right job id cleared; using {JOB_NAME_ENV}={right_job_id!r}.")
    provenances = [value for value in provenances if value]
    provenance = provenances[0] if provenances else {}
    for value in provenances[1:]:
        fields = ("partition_mode", "from_date", "to_date", "vintage", "sample")
        if any(value.get(field) != provenance.get(field) for field in fields):
            raise ValueError("Left and Right CSV provenance does not match")
    timezone = answer("IANA timezone", defaults.get("timezone", timezone_default()))
    mode = select(
        "Partition mode",
        ["date", "all"],
        provenance.get(
            "partition_mode",
            run_default("partition_mode", metadata.get("partition_mode", "date")),
        ),
    )
    date_answers = {}
    if mode == "date":
        snapshot = bool(metadata.get("snapshot", False))
        left_date_col = answer(
            "Left date column",
            run_default("left_date_col", metadata["left"].get("date_col")),
            required=not snapshot,
        )
        date_answers["left_date_col"] = left_date_col
        if left_date_col:
            date_answers["left_date_type"] = date_type_value(
                answer(
                    "Left date type (date/string/number/sas or JSON)",
                    date_type_text(
                        run_default("left_date_type", metadata["left"].get("date_type"))
                    ),
                )
            )
        date_answers["right_date_col"] = answer(
            "Right date column",
            run_default("right_date_col", metadata["right"].get("date_col")),
        )
        date_answers["right_date_type"] = date_type_value(
            answer(
                "Right date type (date/string/number/sas or JSON)",
                date_type_text(run_default("right_date_type", metadata["right"].get("date_type"))),
            )
        )
    selected = select_many(
        "Checks",
        ["row_count", *attribute_check_names()],
        run_default("checks", "row_count,hash_sum,n_unique"),
    )
    answers = {
        "partition_mode": mode,
        "checks": selected,
        "sample": sample_value(
            answer("Sample", provenance.get("sample", run_default("sample", "all")))
        ),
        "strict": confirm("Strict normalization", run_default("strict", False)),
        "audit": confirm("Audit SQL", run_default("audit", False)),
        **date_answers,
    }
    left_workers = worker_budget("Left source worker budget", run_default("left_workers", 2))
    right_workers = (
        worker_budget("Databricks query worker budget", run_default("right_workers", 8))
        if remote_right
        else 1
    )
    sync_remote = (
        confirm("Sync remote", run_default("sync_remote", True)) if remote_right else False
    )
    if mode == "date":
        answers.update(
            from_date=answer(
                "From date",
                provenance.get("from_date", run_default("from_date", defaults.get("from_date"))),
            ),
            to_date=answer(
                "To date",
                provenance.get("to_date", run_default("to_date", defaults.get("to_date"))),
            ),
        )
        excluded_default = run_default(
            "excluded_partitions", metadata.get("excluded_partitions") or []
        )
        answers["excluded_partitions"] = excluded_partition_values(
            answer(
                "Excluded partitions (YYYY-MM-DD or start->end, comma-separated)",
                excluded_partitions_text(excluded_default, answers.get("left_date_type")),
                required=False,
            ),
            answers.get("left_date_type"),
        )
        answers["vintage"] = select(
            "Vintage",
            ["day", "week", "month", "quarter", "year"],
            provenance.get("vintage", run_default("vintage", metadata.get("vintage", "day"))),
        )
    run_id = make_run_id(pair, owner, timezone)
    existing = catalog.run(run_id)
    statement_sql.reporter = None
    action = "resume" if existing else "new"
    if existing:
        print(f"Resuming today's run: {run_id}")
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
    for key in (
        "left_date_col",
        "left_date_type",
        "right_date_col",
        "right_date_type",
        "from_date",
        "to_date",
        "excluded_partitions",
        "vintage",
    ):
        if key in answers:
            settings[key] = answers[key]
    config = run_config(pair, metadata, answers, timezone=timezone)
    return settings, config, action, client, catalog


# >>> mirror pair_recon_right to the workspace and repoint the right job <<< #
def sync_right_package(settings: dict, client) -> dict:
    synced = sync_right(client, env_value(WORKSPACE_DIR_ENV))
    reference = str(settings.get("right_job_id") or "").strip()
    job_id = resolve_job_id(client, reference, required=False)
    job_updated = False
    if job_id is not None:
        try:
            job_updated = repoint_job(client, job_id, synced["notebook"])
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
            job_updated=job_updated,
        )
    settings["right_job_id"] = str(job_id)
    return synced


def persistent_settings(settings: dict) -> dict:
    transient = {"run_id", "autorun", "force"}
    return {
        key: value
        for key, value in settings.items()
        if key not in transient and not key.startswith("_")
    }


def add_mismatched_partitions(settings: dict, config: dict, error: BaseException) -> list[str]:
    pair = settings["pair"]
    detected = mismatched_partitions(error, pair)
    if not detected:
        return []
    normalized = excluded_partition_values(detected, settings.get("left_date_type"))
    existing = set(settings.get("excluded_partitions") or [])
    added = sorted(set(normalized) - existing)
    if not added:
        return []
    merged = sorted(existing | set(normalized))
    settings["excluded_partitions"] = merged
    config["pairs"][pair]["excluded_partitions"] = merged
    return added


def execute(args, *, raise_errors: bool = False) -> int:
    load_environment()
    settings = None
    try:
        config_path = getattr(args, "config", None)
        defaults_path = getattr(args, "defaults_path", None)
        selected_defaults = defaults_path or DEFAULTS
        defaults = load_config(config_path) if config_path else load_defaults(selected_defaults)
        interactive = defaults_path is None
        prompt_session = PromptSession() if interactive else None
        token = _PROMPT_SESSION.set(prompt_session)
        try:
            while True:
                settings = None
                if prompt_session:
                    prompt_session.begin()
                try:
                    prompt_result = (
                        prompts(defaults) if interactive else prompts(defaults, interactive=False)
                    )
                    settings, config, action, client, catalog = prompt_result
                    settings["autorun"] = bool(getattr(args, "autorun", False))
                    settings["force"] = bool(getattr(args, "force", False))
                    if settings["force"]:
                        action = "restart"
                    configure_console_logging(args.log_level, settings["timezone"])
                    print(
                        json.dumps(
                            {
                                "settings": {
                                    key: value
                                    for key, value in settings.items()
                                    if not key.startswith("_")
                                },
                                "pair": next(iter(config["pairs"])),
                            },
                            ensure_ascii=False,
                            indent=2,
                        )
                    )
                    run_now = prompt_session.confirm("Run now", True) if prompt_session else True
                    save_defaults(selected_defaults, persistent_settings(settings))
                    if not run_now:
                        return 0
                    break
                except RestartPrompts:
                    console("Returning to the previous question...")
        finally:
            _PROMPT_SESSION.reset(token)
        if settings["sync_remote"]:
            if not env_value(WORKSPACE_DIR_ENV):
                raise ValueError(f"Sync remote requires {WORKSPACE_DIR_ENV} in .pair_recon.env")
            log.info("syncing Right package files to Databricks Workspace")
            synced = sync_right_package(settings, client)
            log.info("Right package sync completed: %s", json.dumps(synced, ensure_ascii=False))
            save_defaults(selected_defaults, persistent_settings(settings))
        else:
            log.info("Right package sync skipped by run option")
        snapshot = write_request_snapshot(settings, config)
        log.info("request snapshot: %s", snapshot)
        attempts = 0
        while True:
            try:
                result = launch(
                    settings,
                    config,
                    action=action,
                    workspace_client=client,
                    catalog=catalog,
                )
                break
            except Exception as exc:
                added = (
                    add_mismatched_partitions(settings, config, exc)
                    if settings["autorun"] and attempts < AUTO_EXCLUDE_RETRIES
                    else []
                )
                if not added:
                    raise
                attempts += 1
                action = "restart" if settings["force"] else "resume"
                save_defaults(selected_defaults, persistent_settings(settings))
                snapshot = write_request_snapshot(settings, config)
                log.warning(
                    "autorun retry %d/%d: added %d excluded partition(s) and %s "
                    "the same run_id=%s: %s",
                    attempts,
                    AUTO_EXCLUDE_RETRIES,
                    len(added),
                    "restarting" if settings["force"] else "resuming",
                    settings["run_id"],
                    ", ".join(added),
                )
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
    inputs = parser.add_mutually_exclusive_group()
    inputs.add_argument("--config", type=Path)
    inputs.add_argument(
        "--defaults",
        "--default",
        dest="defaults_path",
        nargs="?",
        const=DEFAULTS,
        type=Path,
        help="run without prompts using YAML (default: .pair_recon.yaml)",
    )
    parser.add_argument(
        "--autorun",
        action="store_true",
        help="exclude reported mismatched partitions and automatically resume",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="ignore PairRecon caches and overwrite this run's existing results",
    )
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
