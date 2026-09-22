---
tags: [dtrack, distributed, s3, run-id, trigger, databricks]
step: 0
updated: 2026-09-22
---

# 13 触发、run 标识与 S3 文件树

索引：[[00 索引]] · 相关：[[12 Databricks 作业]]、[[05 S3 提交协议]]、[[02 Stage 1 左侧计数]]

回答三个问题：云端怎么知道可以开工；同一个 pair 多次查、多个 job 并行时文件树怎么不打架；
Workflow 每次要什么输入才能读写到正确的文件夹。

## 1. 文件树：一个 run 一个文件夹，run_id 是唯一的钥匙

```
s3://dtrack-work/            ← Volume  /Volumes/main/dtrack/work
  dtrack/v1/runs/                                   ← 「runs 根」，Workflow 的 work 参数指这里
    20260922T120601Z-a3f9c1/                         ← run_id = UTC 时间戳 + 配置哈希
      _STATUS.json  _HEARTBEAT
      stage1/  manifest/  units/  _INDEX/  results/  dead/  reports/
    20260922T150000Z-a3f9c1/                         ← 同一配置再查一次：新时间戳，新文件夹
    20260922T151000Z-7b21de/                         ← 另一套配置（别的 pair 或别的日期范围）
s3://dtrack-report/          ← Volume  /Volumes/main/dtrack/report
  history/20260922T120601Z-a3f9c1/ summary.json index.html report.xlsx *_compare_*.csv
  history/20260922T150000Z-a3f9c1/ ...
  latest/pointer.json        ← 永远指向最近一次完成的 run
```

- **run_id 由本地 `stage1` 生成**（`make_run_id`：`%Y%m%dT%H%M%SZ` + `sha256(config)[:6]`），或用
  `--run-id` 指定。同一个 pair 查十次就是十个文件夹，彼此不覆盖；配置哈希那六位让你一眼看出
  "这是同一套配置的重跑"还是"另一套配置"。
- **pair 在 run 内部是文件名**：`stage1/orders.left.json`、`manifest/orders.json`、
  `units/orders--2026-02-01--amount--<digest>.json`。一个 run 可以装多个 pair，它们在同一棵树里
  按 pair 名分开，互不干扰（unit_id 里带 pair 名和摘要，[[09 比对规则]]）。
- **报告按 run 归档**，`history/<run_id>/` 不可变；`latest/pointer.json` 只在一次 run 完整发布后
  才覆写（[[10 报告]]）。本地 `fetch-report` 不给 `--run-id` 就跟指针走，给了就取那一次。

## 2. 云端怎么知道可以开工（两级信号，都是文件）

| 阶段 | 信号 | 谁写 | 云端怎么检测 |
| --- | --- | --- | --- |
| 找 run | `runs/<run_id>/` 出现且里面有 `stage1/` | 本地 `stage1` | `resolve_run`：给了 `run_id` 就进那个文件夹；没给就在 runs 根下挑**最新的、`_STATUS.json` 不是 complete 的** run |
| Stage 1 | `stage1/<pair>.left.json` | 本地 `stage1` | 每 `poll_interval` 秒列 `stage1/`，凡是还没有 `stage1/<pair>.json` 的 pair 就做右侧计数 + 对账（幂等，重复列到也不会重做） |
| Stage 1 收口 | `stage1/_LEFT_CLOSED` | 本地 `stage1` | 全部 pair 都有 verdict 后写 `stage1/_CLOSED`，本地 `produce` 等的是它 |
| Stage 2 B 类 | `manifest/<pair>.json`（云端自己写的） | 云端 | 一写完就预取右侧，不等本地 |
| Stage 2 fixed-N | `manifest/<pair>.keys.json` | 本地 `produce` | fixed 模式的 pair 等这个文件出现才预取 |
| Stage 2 A 类 | `_INDEX/<seq>-<unit>.json` | 本地 `produce` | `StartAfter=上次的 key` 增量列举，来一个比一个 |
| 结束 | `manifest/_CLOSED` 且 `results + dead == expected_units` | 本地写 `_CLOSED` | 满足即出报告、发布、退出 |

代码：`dtrack_cloud/store.py::resolve_run`、`app.py::run`、`stage1_right.py::pending_left_documents`、
`consumer.py::poll`。

## 3. Workflow 每次的输入

notebook 用 widgets 接参数；Job 把 job parameters 按同名传进来（`dbutils.widgets.get`），
点 "Run now with different parameters" 可以临时改。

| 参数 | 值 | 说明 |
| --- | --- | --- |
| `work` | `/Volumes/main/dtrack/work/dtrack/v1/runs`（runs 根）或某个 run 文件夹 | 指根时自动挑最新未完成的 run |
| `run_id` | 空 或 `20260922T120601Z-a3f9c1` | 给了就只处理这一个；多 run 并行时用它指定 |
| `report` | `/Volumes/main/dtrack/report` | 报告桶，固定 |
| `gate` | `auto` / `manual` / `ratio:0.02` | 放行策略 |
| `idle_timeout` / `wait_timeout` | 秒 | 没活多久退出；等不到 run 多久退出 |

三种启动方式，输入相同：

1. **手动 Run now**：`work` 指根，`run_id` 留空。本地跑完 `stage1` 后点一下，云端自己找到最新 run。
2. **Schedule**（每小时等）：同上，`wait_timeout` 设短，没活几秒退出。
3. **File arrival trigger**（推荐）：Databricks Jobs 支持监听 Unity Catalog Volume 或 external
   location 的子路径，新文件到达（约每分钟检查一次，递归子目录）就起一次 run。把触发路径设为
   `/Volumes/main/dtrack/work/dtrack/v1/runs`，本地 `stage1` 写下 `left.json` 的一分钟内 job 自动
   启动，`work` 指根、`run_id` 留空即可。配 `max_concurrent_runs = 1`，新文件在跑的时候到达会排队
   到下一次触发。

## 4. 并发与重复的几条规则

- **同一个 run 两个 job 同时跑**：不要。`max_concurrent_runs = 1` 挡住手动误点；file arrival 触发
  本身是串行的。
- **不同 run 同时跑**（两套配置各起一个 job）：可以，各自 `run_id` 指定，文件夹不同，报告归档不同；
  `latest/pointer.json` 是谁后发布指向谁。
- **run 中途 job 被杀**：重启同一个 `work`（根或 run 文件夹），`resolve_run` 仍会挑到它（`_STATUS`
  不是 complete），Consumer 从 `results/ ∪ dead/` 续跑，不重复（[[08 Consumer 调度]]）。
- **本地重跑 `stage1` 同一个 run_id**：会覆盖 `left.json`，但云端已有的 `stage1/<pair>.json` 不会
  重做。要重新对账就换 run_id。
- **旧 run 清理**：work 桶按前缀设 30 天生命周期；report 桶的 `history/` 长期保留。
