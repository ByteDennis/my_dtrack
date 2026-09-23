---
tags: [dtrack, s3, protocol, report]
updated: 2026-09-23
---

# 03 Protocol and Report

Index: [[00 Overview]] · Previous: [[02 Right]] · Next: [[04 End to End]]

The two sides never talk; they exchange files. The whole protocol is one rule: **write the data
object first, the pointer object second.**

## Layout, with the writer of each key

```
<work>/dtrack/v1/runs/{run_id}/
  _STATUS.json                    whoever works (role: left | right)
  _HEARTBEAT                      LEFT  {"epoch_s": ...}, refreshed per window during col-check
  row_check/{pair}.left.json      LEFT  counts + credential-free pair config    ← Right's first signal
  row_check/_LEFT_CLOSED          LEFT
  row_check/{pair}.json           RIGHT verdict per Partition (all days, for people)
  row_check/_CLOSED               RIGHT                                         ← Left col-check waits for it
  reports/{pair}_compare_row.csv  RIGHT
  manifest/{pair}.json            RIGHT plan: admitted days only, sampling rule, columns
  manifest/{pair}.keys.json       LEFT  fixed-N key list
  manifest/_CLOSED                LEFT  {pairs, expected_units}
  units/{unit_id}.json            LEFT  ← data (one column's statistics)
  _INDEX/{seq:08d}-{unit_id}.json LEFT  ← pointer; the Right side only scans this
  results/{unit_id}.json          RIGHT
  dead/{unit_id}.json             RIGHT retries exhausted
<report>/
  history/{run_id}/…              RIGHT immutable
  latest/pointer.json             RIGHT written last
```

## Atomic commit and incremental discovery

```python
# dtrack_left/s3.py and dtrack_right/s3.py are the same class
def commit_unit(self, sequence, unit_id, payload, pointer):
    self.put_json(f"units/{unit_id}.json", payload)                                    # 1. data
    self.put_json(f"_INDEX/{sequence:08d}-{unit_id}.json", {**pointer, "unit_id", "payload_key", "sequence"})   # 2. pointer

def list_keys(self, prefix, *, start_after=None):     # ListObjectsV2 with StartAfter, paginated
```

S3 object PUT is atomic and LIST is strongly consistent, so a pointer always names a complete
payload. Zero-padded sequence numbers let the Right side list only what is new. `ready_unclaimed()`
(= `|_INDEX| − |results| − |dead|`) is the Left side's backpressure gauge.

## Two storage backends on the Right (`dtrack_right/store.py`)

```python
def open_store(location):        # "s3://..." -> S3Store (boto3, instance profile); anything else -> VolumeStore (plain files)
class VolumeStore:               # same methods; writes go to ".part" then replace, so readers never see partial files
def resolve_run(location, run_id=None):   # run folder | root + run_id | root -> newest run whose _STATUS.json is not complete
```

Mount the work bucket as a Unity Catalog Volume and the cluster needs no AWS key; the Left side
writes the same bucket with boto3. `tests/right/test_right.py` covers data-before-pointer,
`StartAfter` and run resolution for `VolumeStore`; `tests/integration/test_s3_protocol.py` covers
`S3Store` on Moto.

## Report (`dtrack_right/report.py`, `report_html.py`, `report_excel.py`)

Built by the Right job after the drain; the Left side only downloads it.

```python
def collect(store):        # row_check/<pair>.json (not .left.json), results/, dead/, manifests, manifest/_CLOSED
def _summary(...):         # status DONE | PARTIAL (dead units, missing units, or no _CLOSED); findings vs errors kept apart
def build_report(store, out_dir, *, title, subtitle):
    summary.json · <pair>_compare_row.csv · <pair>_compare_col.csv · index.html · report.xlsx
def publish_report(report_store, run_id, files, summary):
    history/<run_id>/<file> for every file, then latest/pointer.json = {run_id, finished_at, status, findings, base}
```

- `index.html` is self-contained and reuses the original dtrack `html_export.py` look: page
  chrome, collapsible `<details>` per pair, three-column mismatch tables, red/green summaries.
- `report.xlsx` follows the legacy `excel.py` layout: `<pair>_row` (header block, one line per day,
  findings first in red) and `<pair>_col` (per window, both sides transposed, diff cells red,
  `hash_sum` stored as text).
- `findings` (row_count_diff, partition_missing, col_stat_diff) and `errors` (dead units) never mix.
- Pointer last: S3 has no multi-object transaction, so overwriting `latest/` file by file would let
  a reader see a half-old, half-new run. `tests/right/test_report.py` asserts the order and a
  publish → `fetch-report` byte-identical round trip.
