import time

from pair_recon_left.backends.sources import source_from_config
from pair_recon_left.backends.store import VolumeStore
from pair_recon_left.cli.config import validate_config
from pair_recon_left.common.checks.row_check import public_pair_config
from pair_recon_left.common.model.manifest import (
    configured_sample_plan,
    key_family_specs,
    modular_sample_seed,
    right_key_columns,
    window_sample_plan,
)
from pair_recon_left.common.model.protocol import get_right_state
from pair_recon_left.common.runtime.run_logging import get_left_logger
from pair_recon_right.catalog import status_run_values
from pair_recon_right.engine.backends.targets import CsvTarget
from pair_recon_right.engine.job import SMOKE_REQUEST
from pair_recon_right.engine.job import smoke as right_smoke

from ..runner import RightJobMonitor, resolve_job_id, right_job_params, workspace

log = get_left_logger("smoke")
SMOKE_COLUMNS = 5
SMOKE_ROWS = 100


def _text(value):
    return None if value is None else str(value)


# >>> cheap end-to-end check: both health checks, 100 sampled Left rows, a Volume round trip, and the Right re-selecting those keys <<< #
def smoke(settings: dict, config: dict, *, workspace_client=None) -> dict:
    config = validate_config(config)
    pair = next(iter(config["pairs"]))
    pair_config = config["pairs"][pair]
    run_id = f"{settings['run_id']}-smoke"
    partition = "all" if pair_config.get("snapshot") else pair_config.get("partition", "day")
    keys = [
        value.strip() for value in str(pair_config.get("key") or "").split(",") if value.strip()
    ]
    families = key_family_specs(pair_config, keys)
    sample = configured_sample_plan(settings["run_id"], pair, pair_config)
    smoke_window = {"bucket": "smoke"}
    if sample.get("modular"):
        smoke_window["sample_seed"] = modular_sample_seed(
            sample["modular"]["run_date"], smoke_window["bucket"]
        )
    sample_manifest = {"sample": sample}
    col_map = pair_config.get("col_map") or {}
    columns = [column for column in col_map if column not in keys][:SMOKE_COLUMNS]
    bounds = (pair_config.get("fromDate"), pair_config.get("toDate"))
    report = {"pair": pair, "run_id": run_id, "partition": partition, "keys": keys}
    source = source_from_config({**pair_config["left"], "partition": partition})
    health = source.health_check()
    report["left"] = {"health": health}
    log.info("left health check %s: %s", pair, "OK" if health.get("ok") else health.get("error"))
    if not health.get("ok"):
        raise RuntimeError(f"Left health check failed: {health.get('error')}")
    started = time.monotonic()
    names, rows = source.sample_rows(
        *bounds,
        columns,
        key_columns=keys,
        key_families=families,
        sample=window_sample_plan(sample_manifest, smoke_window, "left"),
        limit=SMOKE_ROWS,
    )
    positions = [
        next(i for i, name in enumerate(names) if name.casefold() == key.casefold()) for key in keys
    ]
    key_tuples = [[_text(row[i]) for i in positions] for row in rows]
    report["left"].update(
        {"rows": len(rows), "columns": names, "elapsed_s": round(time.monotonic() - started, 3)}
    )
    log.info(
        "left sample: pair=%s rows=%d elapsed=%.1fs", pair, len(rows), time.monotonic() - started
    )
    client = workspace_client or workspace(settings["profile"])
    store = VolumeStore(settings["volume_root"], files=client.files).child(run_id)
    public = public_pair_config(pair_config)
    date_col = str(pair_config["left"].get("date_col") or "").lower()
    request = {
        "pair": pair,
        "partition": partition,
        "right": public["right"],
        "from_date": bounds[0],
        "to_date": bounds[1],
        "key_columns": right_key_columns(pair, pair_config, keys, partition, date_col),
        "key_families": families,
        "sample": window_sample_plan(sample_manifest, smoke_window, "right"),
        "key_tuples": key_tuples,
        "columns": [col_map[column] for column in columns],
    }
    store.put_json(SMOKE_REQUEST, request)
    if store.get_json(SMOKE_REQUEST) != request:
        raise RuntimeError("Volume round trip returned a different document")
    report["volume"] = {"ok": True, "path": f"{store.root_uri}/{SMOKE_REQUEST}"}
    log.info("volume round trip ok: %s", report["volume"]["path"])
    if str(pair_config["right"].get("source") or "").lower() == "csv":
        right = right_smoke(store, lambda values: CsvTarget(values))
    else:
        job_id = resolve_job_id(client, settings["right_job_id"])
        params = {
            **right_job_params(settings, pair, run_id, status_run_values(pair_config)),
            "smoke": "1",
        }
        log.info("starting Right smoke job: job_id=%s run_id=%s", job_id, run_id)
        monitor = RightJobMonitor(
            client.jobs.run_now(job_id, notebook_params=params), jobs=client.jobs
        ).start()
        monitor.result()
        right = get_right_state(store)
        if right.get("phase") != "smoke":
            raise RuntimeError("Right job finished without writing its smoke state to the Volume")
    report["right"] = {name: right.get(name) for name in ("health", "matched", "expected")}
    matched, expected = right.get("matched") or 0, right.get("expected") or 0
    if not rows:
        report["verdict"] = "left_empty"
    elif matched < expected:
        report["verdict"] = "sample_mismatch"
        report["hint"] = (
            f"Right re-selected {matched}/{expected} of the Left's sampled keys with the same predicate; "
            "check key types, date mapping, and the sampling columns"
        )
    else:
        report["verdict"] = "ok"
    log.info("smoke verdict: pair=%s %s", pair, report["verdict"])
    return report
