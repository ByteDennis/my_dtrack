---
tags: [dtrack, distributed, databricks, job, right]
step: 0
updated: 2026-09-22
---

# 12 Databricks 作业

下一步：[[01 配置与连接档案]] · 索引：[[00 索引]]

云端只有一个 notebook 和一个自包含的 python 包。启动一次，中途不用碰，做完自己退出。

## 部署：同步目录即可，不需要 wheel

```
databricks/
  consumer_notebook.py      # Databricks notebook（# Databricks notebook source）
  dtrack_cloud/             # 自包含：s3 / store / targets / stage1_right / scheduler / consumer / reporting ...
```

把 `databricks/` 放进一个 Repo（或 workspace folder）。notebook 所在目录在 `sys.path` 上，
`import dtrack_cloud` 直接可用。包里没有一行 import 本地的 `dtrack_distributed`
（`tests/test_dependencies.py::test_cloud_package_never_imports_the_local_package`）。

Job cluster 每次都是新环境，所以 notebook 第一格是 `%pip install -q -r ./requirements.txt`
（`openpyxl` 出 Excel，`boto3` 只在 `work` 为 `s3://` 时用），随后 `dbutils.library.restartPython()`。
也可以把 `databricks/requirements.txt` 挂成 Job library 然后删掉那一格。pyspark、pandas 是 runtime 自带。

## notebook 做什么

```python
# databricks/consumer_notebook.py（缩写）
sys.path.insert(0, os.getcwd())
from dtrack_cloud.app import run
from dtrack_cloud.targets import SparkTarget

dbutils.widgets.text("work",   "/Volumes/main/dtrack/work/dtrack/v1/runs")   # runs 根，或某个 run；也可 s3://...
dbutils.widgets.text("run_id", "")                                           # 空 = 自动挑最新未完成的 run
dbutils.widgets.text("report", "/Volumes/main/dtrack/report")
dbutils.widgets.text("gate",   "auto")

outcome = run(dbutils.widgets.get("work"), lambda config: SparkTarget(spark, config),
              run_id=dbutils.widgets.get("run_id") or None,
              report=dbutils.widgets.get("report"), gate=..., idle_timeout=..., wait_timeout=...)
```

`work` 给 `s3://` 就走 boto3（集群要有 instance profile），给 `/Volumes/...` 就走
`VolumeStore`（纯文件 IO，集群上不需要 AWS key）。两种后端同一套契约（[[05 S3 提交协议]]）。

## 整个作业的生命周期

```python
# databricks/dtrack_cloud/app.py
def run(work, target_factory, *, report=None, gate="auto", poll_interval=2, idle_timeout=900, wait_timeout=1800, ...):
    while True:                                            # 1. 找到 run 并等左侧的第一个 left.json
        store, run_id = resolve_run(work, run_id)         #    根目录 -> 最新未完成的 run；给了 run_id 就进它
        if store is not None and store.list_keys("stage1/"): break
        if elapsed > wait_timeout: raise TimeoutError
        time.sleep(poll_interval)
    reconcile_pending()                                   # 2. 右侧计数 + 对账 + manifest（[[03 Stage 1 右侧对账]]）
    consumer = Consumer(store, target_factory, on_idle=reconcile_pending, ...)
    results = consumer.run()                              # 3. B 预取右侧、A 比对，直到 _CLOSED 且计数齐（[[08 Consumer 调度]]）
    files = build_report(store, out_dir)                  # 4. summary / csv / html / xlsx（[[10 报告]]）
    publish_report(open_store(report), run_id, files, summary)   # 5. history/ 先写，latest/pointer.json 最后
    store.put_json("_STATUS.json", {"state": "complete", "role": "right"})
```

## 在 Databricks 上把它做成 Workflow（一键驱动）

前置只做一次：

1. **Volume**：在 Unity Catalog 里建一个 external location 指向专用 S3 桶，再建两个 Volume，
   比如 `main.dtrack.work`（→ `s3://dtrack-work/`）和 `main.dtrack.report`（→ `s3://dtrack-report/`）。
   本地 `--s3 s3://dtrack-work/dtrack/v1/runs/<run_id>` 写的对象，在集群上就是
   `/Volumes/main/dtrack/work/dtrack/v1/runs/<run_id>/`。
2. **Repo**：Workspace → Repos → Add repo，指向这个仓库；notebook 路径是
   `<repo>/databricks/consumer_notebook.py`。

然后建一个 Job（Workflows → Create Job）：

| 设置 | 值 |
| --- | --- |
| Task type | Notebook，路径 `<repo>/databricks/consumer_notebook.py`，Source = Git（拉固定分支或 tag） |
| Compute | Job cluster，DBR 14+，Single node 或 autoscale 1–4；不要用 all-purpose |
| Parameters（job parameters → notebook widgets） | `work` = `/Volumes/main/dtrack/work/dtrack/v1/runs`（runs 根），`run_id` = 空，`report` = `/Volumes/main/dtrack/report`，`gate` = `auto` |
| Max concurrent runs | 1 |
| Timeout | 比预期 run 长，比如 4 h |
| Trigger | **File arrival**：监听 `/Volumes/main/dtrack/work/dtrack/v1/runs`，新文件到达约一分钟内自动起 run；或 Schedule 每小时一次并把 `wait_timeout` 设 60 |
| Permissions | 运行身份（service principal）对两个 Volume READ/WRITE、对目标表 SELECT |

"一键驱动"的用法：本地跑 `stage1`，然后要么点 Run now（参数全部不改），要么什么都不点、等
file arrival trigger。`work` 指 runs 根，云端 `resolve_run` 自动挑最新且未完成的 run；要指定某一
次就填 `run_id`。Job 会自己等左侧计数、对账、排空、发布报告、退出。触发信号、文件树与并发规则
见 [[13 触发、run 标识与 S3 文件树]]。

不再有本地点火（`jobs/run-now`）：本地不持有 Databricks 凭据。

## 触发信号

不是 manifest，是 `stage1/<pair>.left.json`（本地 `stage1` 写的左侧计数）。云端拿它数右侧、
对账，自己写出 manifest，再继续比对。左侧后来的 `produce` 靠 `stage1/_CLOSED` 知道可以推了。

## 退出

| 情形 | 行为 |
| --- | --- |
| 正常 | `manifest/_CLOSED` 存在且 `results + dead == expected_units` → 出报告 → 返回 → 集群释放 |
| 左侧死了 | `_HEARTBEAT` 超过 `heartbeat_timeout` 未更新 → 抛异常非零退出；已比完的都在 `results/` |
| 一直没活 | `idle_timeout`（默认 15 分钟）→ 退出，防止空烧集群 |
| 人工 | Jobs UI 或 `jobs/runs/cancel`；重启同一个 `work` 从 `results/ ∪ dead/` 续跑，不重复 |

## 计算资源

Job cluster（不是 all-purpose），autoscale 1 到少量 worker；只在 job 存活期间存在。右侧计数和
统计都是一条 `spark.sql`，数据不离开仓库，只回一行。

## 本地替身

`python -m dtrack_cloud --work ... --right-csv-dir DIR` 跑同一个 `run()`，用
`CsvAnchorTarget` 代替 `SparkTarget`。e2e 就是这样验证协议的（[[11 e2e 运行]]）。
