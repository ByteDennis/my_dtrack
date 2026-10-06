# Live tests: set it up yourself, one step at a time

`tests/unit` proves the code with mocks; `tests/local` proves it against emulators (Moto, local
pyspark, an Oracle container). `tests/live` proves **your** setup: real S3, a real Unity Catalog
Volume on that S3, Databricks Connect, and a real Databricks job. Each test is one setup step and
fails with a message that names the step, so run them in order and fix one thing at a time.

```text
this PC                                  AWS                         Databricks
Oracle (container) -> dtrack row-check -> s3://.../work  ==========  /Volumes/.../work  -> notebook.py job
                      dtrack col-check                               (external Volume)     spark.sql on UC tables
dtrack tools fetch-report <- s3://.../report  =====  /Volumes/.../report <- report published by the job
```

## 0. Two environments

The main `.venv` (Python 3.13) never has a Databricks package. The live tests run in their own
`.venv-live` on Python 3.12, because `databricks-connect` supports exactly one Python minor per
release and cannot live next to `pyspark`.

```bash
just live sync                      # .venv-live: dtrack + dev tools + databricks-sdk + databricks-connect
just live setup                     # provisions step by step and writes tests/live/live.env
```

`just live setup` (`tests/live/provision.py`) asks before every call and records what it created
in `tests/live/.provision.json`; `just live teardown` removes exactly that, newest first.

| Setup step | What it does |
| --- | --- |
| `aws_identity` | `sts.get_caller_identity`: which account and region boto3 uses |
| `databricks_login` | writes a `~/.databrickscfg` profile (`auth_type = external-browser`, or a token) and signs in |
| `s3_bucket` | creates the private bucket; `dtrack/work`, `dtrack/report`, `dtrack-live` live in it |
| `uc_schema` | creates `<catalog>.dtrack_live` for the test tables |
| `storage` | makes the job reach `work/` and `report/`, one of three modes below |
| `probe_job` | a 10-line notebook on serverless that lists `work/` the same way the real job will |

| Mode | When | How the job reaches S3 |
| --- | --- | --- |
| `existing` | company workspace: an admin already mapped a Volume to S3 | you type the Volume path and its S3 URL; the script checks they are the same place |
| `external` | you are metastore admin on Databricks on AWS | IAM role -> storage credential -> external location -> two external Volumes |
| `bridge` | Databricks Free Edition (no storage credentials) | a bucket-scoped IAM user key in a secret scope; the job reads `s3://` with boto3 |

Free Edition only has serverless compute and limits outbound traffic to trusted domains until the
account is verified, so `probe_job` tells you whether `bridge` can reach S3 before step 8 does.

Compute: Databricks Connect and notebook jobs need Spark compute (serverless or a cluster). A SQL
warehouse only runs SQL, so the tests do not use it.

Pin `databricks-connect` to your compute: serverless takes the newest release that supports your
workspace's serverless environment; a classic cluster needs the same major.minor as its Databricks
Runtime (DBR 16.4 -> `databricks-connect==16.4.*`). Edit the `live` group in `pyproject.toml`.

## 1. Credentials

| Service | How the tests authenticate | Check by hand |
| --- | --- | --- |
| AWS S3 | boto3 default chain: `AWS_PROFILE`, SSO, or env keys | `aws s3 ls s3://my-bucket/dtrack-live/` |
| Databricks | profile `DATABRICKS_CONFIG_PROFILE` in `~/.databrickscfg`; `auth_type = databricks-cli` or `external-browser` opens the browser once | `databricks auth login --profile corp`, then `python -c "from databricks.sdk import WorkspaceClient; print(WorkspaceClient(profile='corp').current_user.me().user_name)"` |

`WorkspaceClient(profile="corp")` and Databricks Connect read the same profile, so the browser
login you already use is enough. Databricks Connect additionally needs *compute*: set
`DTRACK_LIVE_COMPUTE=serverless`, or a cluster id (or put `serverless_compute_id = auto` /
`cluster_id = ...` in the profile).

## 2. The steps

| Step | Test | What it proves | Settings it needs |
| --- | --- | --- | --- |
| 1 | `dtrack_left/test_s3.py::…contract…` | this PC can put/list/get/delete on S3 | `DTRACK_LIVE_S3` |
| 2 | `dtrack_left/test_s3.py::…fetch_report…` | `dtrack tools fetch-report` works against real S3 | `DTRACK_LIVE_S3` |
| 3 | `dtrack_left/test_oracle_to_s3.py` | `doctor` + `row-check` read Oracle and land counts on S3, no secrets in them | `DTRACK_LIVE_S3`, Docker |
| 4 | `dtrack_right/test_databricks_target.py::…counts…` | Databricks Connect works; the synthetic Right tables were created in your schema | profile, compute, `DTRACK_LIVE_SCHEMA` |
| 5 | `dtrack_right/test_databricks_target.py::…statistics…` | `SparkTarget`'s one query on the cluster equals the pandas rule | same |
| 6 | `dtrack_right/test_volume.py::…work…` | the work Volume **is** the work S3 prefix, both directions | `DTRACK_LIVE_S3_WORK`, `DTRACK_LIVE_VOLUME_WORK` |
| 7 | `dtrack_right/test_volume.py::…report…` | the report Volume is the report S3 prefix | `DTRACK_LIVE_S3_REPORT`, `DTRACK_LIVE_VOLUME_REPORT` |
| 8 | `test_job_flow.py` | the whole run: package uploaded to the workspace, one-time job on `notebook.py`, `row-check` + `col-check` here, report fetched from S3 with the expected findings | all of the above + `DTRACK_LIVE_WORKSPACE_DIR` |

```bash
just live test -k "test_s3"                 # steps 1-2
just live test dtrack_left                  # steps 1-3
just live test dtrack_right                 # steps 4-7
just live test test_job_flow.py             # step 8 (10-20 min: container, job start, drain)
just live test                              # everything
```

## 3. What each Databricks object is

- **External location + storage credential**: lets Unity Catalog read/write the bucket. An admin
  usually owns these.
- **External Volume** `CREATE EXTERNAL VOLUME dev.dtrack.work LOCATION 's3://my-bucket/dtrack/work'`:
  gives the S3 prefix the path `/Volumes/dev/dtrack/work`. The job reads and writes the run folder
  through this path with plain file I/O (`VolumeStore`), so the cluster needs no AWS key.
- **Schema** `dev.dtrack_live`: the tests create `orders_<tag>` and `customers_<tag>` from the
  synthetic Right CSVs and drop them afterwards.
- **Workspace folder** `DTRACK_LIVE_WORKSPACE_DIR`: step 8 uploads `src/dtrack_right` there
  (`notebook.py` becomes a notebook, the rest plain files) and submits a one-time run with the same
  parameters a scheduled Job would pass: `work`, `run_id`, `report`, `gate`, `idle_timeout`,
  `wait_timeout`.

## 4. Clean-up

Every object is named with a per-session tag (`live-<UTC stamp>-<hex>`). S3 scratch, tables,
probe files and the uploaded folder are removed at the end. `DTRACK_LIVE_KEEP=1` keeps them so
you can look at the run folder, the job run page and the report.
