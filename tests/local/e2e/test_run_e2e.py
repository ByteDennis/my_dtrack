import json
import os
import subprocess
from pathlib import Path

import pytest

pytestmark = [pytest.mark.e2e, pytest.mark.docker]
ROOT = Path(__file__).resolve().parents[3]


# >>> the whole two-process run against Moto; asserts the documented outcome of examples/e2e <<< #
def test_examples_e2e_runs_clean_and_reports_the_injected_findings(moto_endpoint):
    env = {**os.environ, "DTRACK_S3_ENDPOINT_URL": moto_endpoint}
    completed = subprocess.run(
        ["bash", "examples/e2e/run_e2e.sh"],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=900,
        check=False,
    )
    record = (ROOT / "examples/e2e/execution.txt").read_text()
    assert completed.returncode == 0, completed.stderr[-2000:]
    ends = [line for line in record.splitlines() if " END " in line]
    failed = [line for line in ends if "rc=0" not in line and "12_right_" not in line]
    assert not failed, failed
    summary = json.loads((ROOT / "examples/e2e/output/report/summary.json").read_text())
    assert summary["status"] == "DONE"
    assert summary["findings"] == {"row_count_diff": 1, "partition_missing": 2, "col_stat_diff": 2}
    assert (
        "work store objects: left=2 verdicts=2 manifests=2 fixed_keys=1 units=11 index=11 results=11 dead=0"
        in record
    )
