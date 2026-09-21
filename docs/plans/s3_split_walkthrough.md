# 讲透一遍：这个 job 到底是怎么跑的

> 实施说明：本文保留为工作流背景；当前交付不包含 SAS，实际命令与依赖边界以
> `docs/workflow.md` 为准。

配套文档：`plans/s3_split_pipeline.md`（设计与取舍）。这一篇不讲取舍，只讲
**实际发生了什么** —— 一个 table pair 从你敲下命令到业务看到结果，中间每一分钟
在干什么；出了问题你去哪儿看；一次跑五张表时队怎么排。

---

## 开场：先认清三个角色、两个桶

**三个角色**

| 角色 | 在哪运行 | 干什么 |
| --- | --- | --- |
| **Producer** | 你的本地 PC | 数左侧、拉左侧样本、往 S3 推 |
| **Consumer** | Databricks 上一个常驻 job | 数右侧、抓右侧样本、比对、出报告 |
| **S3** | 中间的邮局 | 两边互不认识，只通过文件交接 |

Producer 和 Consumer **从不直接对话**。Producer 不知道 Consumer 有没有起来，
Consumer 不知道 Producer 还要推多久。它们只认 S3 上的文件。这是整套设计最重要
的一句话 —— 任何一边挂了，另一边不会跟着挂。

**两个桶，职责完全不同**

```
s3://dtrack-work/        ← 工作区。机器读。乱、碎、大、30 天过期
s3://dtrack-report/      ← 成品区。人和 BI 读。干净、小、长期保留
```

为什么非要两个桶？三个理由，都很实际：

1. **权限不同。** 业务方只需要 report 桶的读权限。work 桶里可能有原始采样明细
   （`--upload-raw`），不能给业务看。
2. **生命周期不同。** work 桶 30 天自动清（里面全是中间产物，留着只花钱）；
   report 桶要留一年做趋势对比。
3. **访问模式不同。** work 桶一轮 run 会被 list 上千次；report 桶只在跑完时写一
   次、之后被 dashboard 读。混在一个桶里，生命周期规则和监控指标都会互相干扰。

---

# 第一课：一个 pair，从头到尾

## 我们的例子

```jsonc
"ecm_acct": {
  "left":  { "source": "oracle", "conn_macro": "pcds",
             "table": "ECM_ACCT_DAILY", "date_col": "RPT_DT",
             "date_type": "date" },
  "right": { "source": "databricks", "catalog": "main", "schema": "ecm",
             "table": "acct_daily", "date_col": "rpt_dt",
             "date_type": "string_dash" },
  "vintage": "month",
  "fromDate": "2026-06-01",
  "toDate":   "2026-07-31",
  "key": "ACCT_ID,SUB_ID",
  "sample": 0.005
}
```

一张 Oracle 表对一张 Databricks 表，两个月（61 天），30 个要比的字段，
0.5% 采样，按月分窗。

## 你敲的命令

```bash
dtrack s3-stage1 testing/project.db --config pairs.json \
       --s3 s3://dtrack-work/dtrack/v1 \
       --report-s3 s3://dtrack-report \
       --gate auto --kickstart
```

`--gate auto` 的意思是：Stage 1 数完、对账通过，就自己接着跑 Stage 2，不用你再
敲一次。

## 时间轴

下面每一行都是你在终端上真能看到的东西。

### `00:00` 生成 run_id，开工

```
[dtrack] run_id = 20260917T1402Z-a3f9c1
[dtrack] work   = s3://dtrack-work/dtrack/v1/runs/20260917T1402Z-a3f9c1/
[dtrack] report = s3://dtrack-report/
[dtrack] 1 pair: ecm_acct
```

`run_id` = 时间戳 + 配置的哈希。**配置改了，run_id 就变**，所以你永远不会把两
次不同参数的结果混在一起。

### `00:00 → 00:02` Databricks 体检（preflight）

这是第一件事，不是第三件事。为什么？因为它最可能失败，而且最便宜。如果
Databricks 连不上，我们要在**碰 Oracle 一根手指之前**就失败掉 —— 否则你会先花
两分钟数完 Oracle，然后才发现右边根本没法比。

```
[preflight] ecm_acct
  auth .................... OK  (service principal: dtrack-sp)
  warehouse ............... OK  (serverless, 1.2s cold start)
  main.ecm.acct_daily ..... OK  (61 partitions visible)
  columns ................. OK  (30/30 mapped)
  rpt_dt date_type ........ OK  (sample: '2026-06-01' matches string_dash)
  jobs run-now perm ....... OK  (job_id=812334)
[preflight] all checks passed -> _PREFLIGHT.json
```

注意 warehouse 用的是 **serverless SQL warehouse**，不是那个要开 4 分钟的大集
群。它 1 秒多就能用，按查询收费。整个 Stage 1 全靠它，所以 Stage 1 期间云上那
台大集群**还没起**，一分钱没花。

### `00:02 → 00:14` 数右侧（12 秒）

```
[stage1] ecm_acct: right count ...
  SELECT rpt_dt, COUNT(*) FROM main.ecm.acct_daily
   WHERE rpt_dt >= '2026-06-01' AND rpt_dt <= '2026-07-31'
   GROUP BY rpt_dt
  -> 60 partitions, 74,120,336 rows total   (11.8s)
```

一条查询，只扫日期这一列。右边 60 天有数据（少一天，先记住这个）。

**为什么先数右边？** 因为右边便宜又快，数完就知道"哪些天右边根本没有"。接下来
数 Oracle 的时候，可以把边界卡进去，少扫一批注定对不上的数据。

### `00:14 → 02:10` 数左侧（约 2 分钟）

```
[stage1] ecm_acct: left count ...
  SELECT RPT_DT, COUNT(*) FROM PCDS.ECM_ACCT_DAILY
   WHERE RPT_DT >= DATE '2026-06-01' AND RPT_DT <= DATE '2026-07-31'
   GROUP BY RPT_DT
  -> 61 partitions, 74,121,331 rows total   (1m56s)
```

Oracle 慢，但这也只是**单列的 GROUP BY**，不是下载数据。两分钟换来"接下来能少
下载多少"，很值。

这里必须强调一句：**这个数必须是精确的 `COUNT(*)`。** Hive 的
`SHOW PARTITIONS` 元数据、Oracle 的 `NUM_ROWS` 统计信息都不能用 —— 它们可能是
上周收集的，拿过期数字去做"相等判断"，结论就是错的。

### `02:10 → 02:11` 对账（第一个交付物就在这儿）

```
[stage1] ecm_acct: reconcile 61 partitions
  match ......... 58
  count_diff .....  2   2026-06-14 (-995)   2026-07-03 (-0? no: +12)
  left_only ......  1   2026-06-22  (left 1,203,441 / right absent)
  right_only .....  0
[stage1] gate=auto -> PASSED (excluded 3/61 = 4.9%)
[stage1] -> s3://.../runs/20260917T1402Z-a3f9c1/stage1/ecm_acct.json
[stage1] -> s3://.../runs/20260917T1402Z-a3f9c1/manifest/ecm_acct.json
```

**这三条已经是可以交给业务的发现了。** 不用等 Stage 2：

- `2026-06-14` 右边少了 995 条；
- `2026-07-03` 右边多了 12 条；
- `2026-06-22` 右边整天没有。

这就是"两阶段"的第一个好处 —— **最明显的问题最快浮出来**，而且这 3 天的数据我
们接下来**完全不下载**。61 天变 58 天，白省 5% 的下载和云端扫描。

请留意 `stage1/` 和 `manifest/` 写的是两份不同的东西，别搞混：

| 文件 | 装什么 | 给谁看 |
| --- | --- | --- |
| `stage1/ecm_acct.json` | **全部 61 天**的对账结论（含被排除的） | 人看，出报告 |
| `manifest/ecm_acct.json` | **只有 58 天**的执行计划 | Consumer 看，照着干 |

### `02:11` 点火（kickstart）

```
[kickstart] POST /api/2.2/jobs/run-now
   job_id=812334  idempotency_token=20260917T1402Z-a3f9c1
   params: { run_id, work_s3, report_s3 }
-> run_id 998877 QUEUED (cluster starting, ETA ~4min)
```

**时机是重点：manifest 刚写完就点火，在拉第一条数据之前。** 集群要开 4 分钟，
这 4 分钟正好和我们下载第一个窗口重叠 —— 冷启动不占用任何额外时间。

`idempotency_token` 用的是 `run_id`。意思是：哪怕网络抖动、你手一抖重跑了这条
命令，Databricks 也只会起**一个** job。这是整套设计里唯一需要的一把"锁"。

### `02:13 → 05:40` 左边下载六月窗口

```
[push] ecm_acct 2026-06 [28 partitions, holes: 06-14, 06-22]
  pull   : hash32 bucket < 21474836  (0.5% of 2^32)
           streaming fetchmany(50000) -> ecm_acct_left.csv
           1,203,556 rows in 3m21s
  stat   : duckdb col stats, 30 cols, 4 workers ... 38s
  upload : 30 units -> units/ecm_acct/2026-06/*.json  (0.4s)
           _INDEX/00000001..00000030
```

三件事要注意：

1. **一次扫描，拉全部 30 列。** 不是 30 条 SQL。这是整套方案省钱的根，别在拆分
   的时候搞坏了。
2. **只在上传的时候才按列切成 30 个小文件。** 切分在本地做，免费。好处是：云端
   拿到的是 30 个能独立处理的小活，一次下载喂饱 30 个槽位。
3. **窗口是"带洞"的。** 六月本该 30 天，我们只要 28 天，`06-14` 和 `06-22` 被
   挖掉了。SQL 里写的是"两段连续区间的并集"，不是 `BETWEEN 06-01 AND 06-30`。
   **这是全套代码最容易写错的一行** —— 写错了，被 Stage 1 排除的坏数据又被吃回
   来，比对结果莫名不等，还极难查。

顺序也有讲究：**先写 `units/...`（数据），再写 `_INDEX/...`（指针）。** S3 写单
个文件是原子的，Consumer 只看 `_INDEX/`。所以它永远不可能读到一个只写了一半的
文件。

### `06:10` 云端醒了，立刻有活干

```
[drain] run 20260917T1402Z-a3f9c1 | workers=4
[drain] manifest/ecm_acct.json loaded: 2 windows x 30 cols = 60 units
[drain] B-queue seeded with 2 right-side window fetches
[drain] A-queue: 30 left units present, waiting on right stats for 2026-06
[drain] -> fetch_right(ecm_acct, 2026-06)
```

**这里是"永不空转"的关键。** 集群一起来就有活，不需要等 Producer。

为什么？因为右边要抓哪些行，**完全不取决于左边的文件**。采样规则是
"`hash32(规范化后的主键) < 0.005 × 2^32`" —— 这个规则写在 manifest 里，两边各
自照着算，算出来必然是同一批记录。所以 Consumer 一读到 manifest，就能立刻开始
抓右侧，根本不用等左侧文件到没到。

于是 Consumer 手上永远有两种活：

| | 叫什么 | 要等什么 | 有多少 |
| --- | --- | --- | --- |
| **A** | 比对 | 左侧文件 + 右侧统计都齐了 | 看 Producer 推得多快 |
| **B** | 抓右侧、算统计 | 只要 manifest | **manifest 一到就全都能干** |

规矩一句话：**有 A 先干 A，A 空了就去干 B。** B 是个一开始就装满的大池子，所以
worker 不会闲着。

### `06:10 → 09:10` 右边抓六月

```
[drain] fetch_right(ecm_acct, 2026-06)
  WHERE rpt_dt BETWEEN '2026-06-01' AND '2026-06-13'
     OR rpt_dt BETWEEN '2026-06-15' AND '2026-06-21'
     OR rpt_dt BETWEEN '2026-06-23' AND '2026-06-30'
    AND hash32(...) < 21474836
  -> 1,203,499 rows, 30 col stats   (2m58s)
  -> delta: dtrack.right_stats (+30 rows)
```

看那个 `WHERE` —— 三段区间，两个洞。左边和右边用的是**同一份** partition 列
表，这样才是真的在比同一批数据。

抓右侧也是**一个窗口一次扫描**，不是每列一次。抓完一次，这个窗口的 30 列统计全
有了。

### `09:10` 六月的 30 个 unit 一瞬间比完

```
[drain] right stats ready for (ecm_acct, 2026-06) -> promote 30 units to A-queue
[drain] compare 30 units ... 4.2s
  ACCT_BAL     n_unique  48,221 vs 48,218   -> FAIL
  RISK_SCORE   hash_sum  mismatch           -> FAIL
  (28 others PASS)
  -> results/ecm_acct__2026-06__*.json
```

比对本身是**毫秒级**的 —— 两边各一行统计数字，相减而已。真正花时间的是前面两
次扫描。所以：**A 类活从来不是瓶颈，B 类的右侧扫描才是。** 这决定了你该开几个
worker（见第三课）。

### 同时：`06:20 → 09:50` 左边在拉七月

Producer 不等 Consumer。六月推完就接着拉七月，两边完全重叠：

```
[push] ecm_acct 2026-07 [30 partitions, holes: 07-03]
  pull  : 1,241,880 rows in 3m30s
  stat  : 41s
  upload: 30 units -> _INDEX/00000031..00000060
[push] ecm_acct: all windows pushed
[push] -> manifest/_CLOSED { pairs: ["ecm_acct"], expected_units: 60 }
```

`manifest/_CLOSED` 是 Producer 对 Consumer 说的唯一一句话："我推完了，一共 60
个，别再等新的了。"

在此之前，Producer 每 60 秒还会覆写一次 `_HEARTBEAT`。作用很实在：Consumer 能分
清"Producer 还在慢慢干，我继续等"和"Producer 死了，我该收摊了"。没有心跳，
Consumer 只能瞎等或者瞎超时。

### `09:15 → 12:20` 右边抓七月，然后比完

```
[drain] fetch_right(ecm_acct, 2026-07) ... 3m05s
[drain] compare 30 units ... 4.0s
  RISK_SCORE   hash_sum mismatch  -> FAIL
  (29 others PASS)
[drain] progress 60/60 units, _CLOSED seen -> finalize
```

### `12:25 → 12:50` 收尾：做分析、出报告、传到 report 桶

这一步是整个 job 的最后一公里，值得拆开讲。

**第 1 步：在 Databricks 上把数据落成表。** 不是落成文件 —— 落成 Unity Catalog
里的 Delta 表。因为 AI/BI dashboard、SQL Alert、Genie 都只认表。

```
[finalize] writing Unity Catalog tables
  main.dtrack.run_status    +1  row   (一次 run 一行：状态、耗时、发现数)
  main.dtrack.compare_row  +61  rows  (Stage 1：每个 partition 一行)
  main.dtrack.compare_col  +60  rows  (Stage 2：每个 column×window 一行)
```

三张表，三个粒度，这样你在 dashboard 上能从"这次 run 怎么样"一直点到"6 月 14 号
的 ACCT_BAL 差多少"。历史全留着，所以趋势图是免费的。

**第 2 步：生成三样给人看的东西。**

```
[finalize] rendering artifacts
  summary.json   1.2 KB   给机器/告警读
  row_diff.csv   3.1 KB   Stage 1 的全部 61 行
  col_diff.csv    18 KB   Stage 2 的全部 60 行
  index.html     240 KB   自包含单页，双击就能看（复用 html_export.py）
```

`index.html` 特意做成**自包含**的（CSS/JS/数据全内联，不引外部文件）。原因很
实际：它要能被塞进邮件附件、能在没网的会议室打开、能直接丢给不懂技术的人。

**第 3 步：传到 report 桶 —— 注意顺序。**

```
[finalize] uploading to s3://dtrack-report/
  1) history/20260917T1402Z-a3f9c1/summary.json
  2) history/20260917T1402Z-a3f9c1/row_diff.csv
  3) history/20260917T1402Z-a3f9c1/col_diff.csv
  4) history/20260917T1402Z-a3f9c1/index.html
  5) latest/pointer.json   <- 最后一步，只有一个小文件
[finalize] DONE in 12m48s | 4 findings (3 row, 1 col-pair... see summary)
```

**为什么 `latest/pointer.json` 必须最后写、而且只写这一个文件？**

S3 没有"多个文件一起提交"这种事。如果你直接往 `latest/` 里逐个覆盖那 4 个文
件，那么在中间某一刻，`latest/index.html` 是新的、`latest/col_diff.csv` 还是旧
的 —— dashboard 这时候刷新，看到的是拼接起来的假数据。

所以：**不可变的归档写进 `history/{run_id}/`，`latest/` 里只放一个指针文件。**

```jsonc
// s3://dtrack-report/latest/pointer.json
{ "run_id": "20260917T1402Z-a3f9c1",
  "finished_at": "2026-09-17T14:14:50Z",
  "status": "DONE",
  "findings": 4,
  "base": "history/20260917T1402Z-a3f9c1/" }
```

写这一个小文件是原子的。Dashboard 的逻辑变成："先读 `pointer.json`，拿到
`base`，再去 `base` 下面读别的"。这样它看到的永远是某一次 run 的**完整**结果，
不会半新半旧。

**第 4 步：通知。**

```
[finalize] alert -> #data-quality (4 findings, 2 need attention)
```

用 Databricks SQL Alert 盯 `main.dtrack.run_status` 表，或者 Consumer 直接发个
webhook。两种都行，区别是前者归运维管、后者归代码管。

### 这一趟的账

| 段 | 耗时 | 谁在干活 | 云上花钱吗 |
| --- | --- | --- | --- |
| preflight + 数两边 | 2m11s | 本地 + serverless warehouse | 几分钱 |
| 集群启动 | 4m（和下载重叠） | — | 算进 job 时长 |
| 左边下载两个窗口 | 6m51s | 本地 PC | 不花 |
| 右边扫两个窗口 | 6m03s（和上面重叠） | job cluster | **主要成本** |
| 比对 60 个 unit | 8s | job cluster | 可忽略 |
| 收尾出报告 | 25s | job cluster | 可忽略 |
| **端到端** | **12m48s** | | |

关键在那些"重叠"：左边下载和右边扫描是同时进行的，集群启动和 Stage 1 的尾巴是
同时进行的。串起来算是 20 多分钟，实际 12 分 48 秒。

---

# 第二课：跑到哪了？出了什么问题？

## 一个地方看全部：`_STATUS.json`

work 桶里有一个文件，谁在干活谁就每 15 秒覆写它一次：

```jsonc
// s3://dtrack-work/dtrack/v1/runs/20260917T1402Z-a3f9c1/_STATUS.json
{
  "run_id": "20260917T1402Z-a3f9c1",
  "updated_at": "2026-09-17T14:08:32Z",
  "producer": { "alive": true, "heartbeat_age_s": 12, "host": "DL-PC" },
  "consumer": { "alive": true, "job_run_id": 998877, "workers": 4,
                "idle_ratio": 0.03 },
  "pairs": {
    "ecm_acct": {
      "state": "DRAINING",
      "stage1": { "match": 58, "count_diff": 2, "left_only": 1,
                  "right_only": 0, "took_s": 131 },
      "stage2": { "units_total": 60, "pushed": 60,
                  "compared": 30, "dead": 0 },
      "current": "fetch_right(2026-07)",
      "errors": []
    }
  },
  "overall": { "state": "DRAINING", "pct": 0.50, "eta_s": 190 }
}
```

## 一个 pair 的状态机

每个 pair 在任意时刻**必然**处在下面某一个状态上，没有"不知道在哪"这种情况：

```
NEW
 └> PREFLIGHT_OK          Databricks 体检过了
     └> COUNTING_RIGHT    在数右侧
         └> COUNTING_LEFT 在数左侧
             └> RECONCILED        对账完了
                 ├> GATE_BLOCKED  差异太大，不让进 Stage 2（终态）
                 └> GATE_PASSED
                     └> PUSHING       本地在推 unit（x/y）
                         └> PUSH_DONE
                             └> DRAINING   云端在消化（x/y）
                                 ├> DONE     全成了
                                 ├> PARTIAL  有 dead unit，但出了报告
                                 └> FAILED   没救，见 errors
```

命令行直接看：

```bash
$ dtrack s3-status --s3 s3://dtrack-work/dtrack/v1 --run-id latest

run 20260917T1402Z-a3f9c1   DRAINING   50%   eta 3m10s
producer DL-PC  alive (hb 12s)     consumer job 998877  4 workers  idle 3%

pair        state      stage1                    stage2         now
ecm_acct    DRAINING   58/2/1/0  (131s)          30/60 cmp      fetch_right(2026-07)
```

## 「发现」和「故障」是两回事

这是整套监控设计里最重要的一条，讲课必须强调：

> **记录数不一样、列统计不一样 —— 这是产品的输出，不是系统出错了。**

搞混了会有两个后果：一是告警天天响，响到没人看；二是真正的故障（连不上、超时）
被埋在几百条"差异"里找不着。

所以错误码分两类：

**A 类：发现（`finding`）—— 不告警，进报告**

| 码 | 阶段 | 意思 |
| --- | --- | --- |
| `F_COUNT_DIFF` | stage1 | 某个 partition 两边记录数不等 |
| `F_LEFT_ONLY` | stage1 | 右边整个 partition 没有 |
| `F_RIGHT_ONLY` | stage1 | 左边整个 partition 没有 |
| `F_STAT_DIFF` | stage2 | 某列的统计值不等 |

**B 类：故障（`error`）—— 要告警，要有人修**

| 码 | 阶段 | 意思 | 找谁 |
| --- | --- | --- | --- |
| `E_AUTH` | preflight | service principal 过期/没权限 | 运维 |
| `E_NO_TABLE` | preflight | 右侧表不存在或被改名了 | 数据工程 |
| `E_COL_MISSING` | preflight | `col_map` 里的列右边没有 | 改配置 |
| `E_DATE_TYPE` | preflight | `date_type` 和实际格式不符 | 改配置 |
| `E_LEFT_CONN` | stage1 | Oracle/NAS 连不上 | 运维 |
| `E_LEFT_SLOW` | stage1 | 左侧计数超时 | 加边界/加索引 |
| `E_GATE_BLOCKED` | stage1 | 差异比例超阈值，人工决定 | 业务判断 |
| `E_PULL_FAIL` | stage2 | 左侧下载失败（重试 3 次后） | 看 traceback |
| `E_RIGHT_SCAN` | stage2 | Databricks 扫描失败 | 看 job log |
| `E_STAT_ENGINE` | stage2 | DuckDB 和 Spark 统计语义不一致 | **代码 bug，最要紧** |
| `E_PRODUCER_DEAD` | 任意 | 心跳超过 3 分钟没动 | 运维 |
| `E_UNITS_MISSING` | finalize | `_CLOSED` 说 60 个，只到了 57 个 | 看 `dead/` |

`E_STAT_ENGINE` 要特别拎出来说：如果本地 DuckDB 和云端 Spark 对同一批数据算出
不同的统计值，那所有结论都不能信了。所以它不是普通错误，是**必须立刻停下来查代
码**的那一类。检测办法：每轮 run 抽一个窗口，两边都算一遍同一侧的数据，对不上
就报这个码。

## 故障时你会看到什么

**preflight 挂了（最好的情况 —— 什么都没浪费）：**

```
[preflight] ecm_acct
  auth .................... OK
  warehouse ............... OK
  main.ecm.acct_daily ..... FAIL
      E_NO_TABLE: [TABLE_OR_VIEW_NOT_FOUND] `main`.`ecm`.`acct_daily`
      hint: 表被改名了？试试 SHOW TABLES IN main.ecm
[dtrack] ABORT before touching the left side. Oracle untouched, no cluster started.
```

**Producer 中途死了（本地断网）：**

```
[drain] heartbeat stale: 214s since last _HEARTBEAT (limit 180s)
[drain] E_PRODUCER_DEAD -> finalize PARTIAL with what we have
[drain] 30/60 units compared; 2026-07 never arrived
[finalize] PARTIAL -> report bucket (summary.json marks status=PARTIAL)
```

注意它**不是直接崩掉** —— 已经比完的 30 个 unit 照样出报告，标成 `PARTIAL`。你
本地恢复网络后重跑同一条命令，`run_id` 不变，已推过的 unit 靠 `_INDEX` 跳过，
接着从七月继续。

**云端某个 unit 反复失败：**

```
[drain] unit ecm_acct__2026-07__RISK_SCORE attempt 3/3 failed
        E_RIGHT_SCAN: [DELTA_FILE_NOT_FOUND] ... VACUUM during scan?
[drain] -> dead/ecm_acct__2026-07__RISK_SCORE.json (traceback inside)
[drain] progress 59/60 + 1 dead = 60 accounted -> finalize PARTIAL
```

一个 unit 死了不会卡住整个 run。它进 `dead/`，计入"已处理"，报告里明确写"这一
列没比出来"，而不是假装它通过了。

---

# 第三课：一次跑五张表，队怎么排

## 我们的五个 pair

| | pair | 左侧 | 右侧 | 个头 |
| --- | --- | --- | --- | --- |
| P1 | `ecm_acct` | oracle (pcds) | `main.ecm.acct_daily` | 大 |
| P2 | `ecm_txn` | oracle (pcds) | `main.ecm.txn_daily` | 很大 |
| P3 | `cust_mst` | hadoop (hive) | `main.cust.master` | 中 |
| P4 | `ref_prod` | csv | `main.ref.product` | 小 |
| P5 | `hist_bal` | sas | `main.hist.balance` | 中 |

五个左侧来源各不相同，右侧全是 Databricks —— 这是设定好的不对称。

## 规矩一：Stage 1 的并发，按「左侧来源」限流，不是给一个全局数字

这是最容易设计错的地方。如果你写 `--pair-workers 5`，五个 pair 一起上，
P1 和 P2 会同时向 Oracle 要两个会话 —— 加上别人的作业，DBA 会来找你。

正确的做法是**每个来源一个闸门**：

```jsonc
"source_limits": {
  "oracle": 2,      // PCDS 只许我们占 2 个会话
  "hadoop": 1,      // Hive 集群共用，客气点
  "csv":    4,      // 本地磁盘，随便
  "sas":    2,      // 本地 CPU，读文件吃内存
  "databricks": 8   // 右侧数数用的 warehouse，能扛
}
```

于是实际的排法是：P1、P2 抢 oracle 的 2 个位子（刚好各一个，并行）；P3 独占
hadoop；P4、P5 在本地跑。**五个 pair 的 Stage 1 实际是并行的，只是被各自的来源
限了速。**

右侧数数不受这个限制 —— 五条 `GROUP BY` 打同一个 serverless warehouse，它自己会
排队，12 秒内全回来。

## 规矩二：谁先过 barrier，谁负责点火

```
00:03  五个 pair preflight 全过
00:15  五个 pair 的右侧计数全回来
00:23  P4 (csv, 小) 对账完 -> GATE_PASSED -> ★ 点火！集群开始启动
00:35  P5 (sas) 过
01:00  P3 (hadoop) 过
02:15  P1 (oracle) 过
05:10  P2 (oracle, 很大) 过   <- 最后一个
```

**点火发生在 `00:23`，不是 `05:10`。** 不等所有 pair 过完 —— 第一个过关的就够
了。集群要开 4 分钟（`00:23 → 04:30`），这 4 分钟里 P1 和 P2 还在数 Oracle，
P4/P5/P3 已经在推 unit 了。集群一睁眼，S3 上已经堆着活。

这就是为什么 manifest 必须是**一个 pair 一个文件**（`manifest/P4.json`、
`manifest/P5.json`……），不能是一个写死的大文件。大文件意味着"等所有 pair 都过
barrier 才能写"，那就得等到 `05:10` 才点火，白丢 4 分半。

## 规矩三：两个阶段，用**相反**的排序策略

这条最反直觉，但道理很顺：

**Stage 1：小表优先。** 目的是**尽早点火**。谁最快过 barrier，集群就能最早开始
启动。所以按"预估计数耗时"升序排：P4 → P5 → P3 → P1 → P2。

**Stage 2：大表优先。** 目的是**让长尾最早开始**。P2 的下载要 20 分钟，如果把它
排在最后，整个 run 的结束时间就是"前面全部时间 + 20 分钟"。反过来先启动它，它
的 20 分钟就和别人的重叠了。

一句话记住：**排队等门票的时候小的先进（早点火），干活的时候大的先干（早收
工）。**

## 规矩四：Stage 2 出单要「轮转」，不能「一张表干完再下一张」

Producer 推 unit 的顺序**不是**这样：

```
✗ P1的1月 P1的2月 P1的3月 ... P2的1月 P2的2月 ...
```

而是这样：

```
✓ P1-06  P2-06  P3-06  P4-06  P5-06  P1-07  P2-07  P3-07 ...
```

为什么？因为云端抓右侧是按 (pair, window) 打到**不同的 Delta 表**上的。如果队列
里连着 30 个 unit 全是 P1 的，四个 worker 就全挤在 `main.ecm.acct_daily` 这一张
表上互相抢 IO 和缓存，有效并行度掉到 1。轮转能保证相邻的活落在不同表上。

（注意：轮转是**出单顺序**的轮转，和规矩三的"大表优先"不冲突 —— 大表优先决定
"哪个 pair 先开始拉"，轮转决定"多个 pair 都在拉的时候，unit 交替往 S3 放"。）

## 规矩五：云端给 A 类留一个保底位子

四个 worker，如果四个都去抓右侧大窗口（B 类，每个三分钟），那这三分钟里哪怕有
左侧文件到了、可以马上比对（A 类，几秒钟），也没人有空干。

解法很简单：**B 类最多占 W-1 个 worker，永远留至少 1 个给 A 类。**

```
workers = 4
  worker-0  ← 只接 A 类（保底位）
  worker-1  ← A 优先，A 空了接 B
  worker-2  ← 同上
  worker-3  ← 同上
```

这样"左侧文件一到就能马上出结果"这件事有了保证，业务能更早看到第一批发现。

还有一件必须做的事：在一个 job 里从多个线程提交 Spark 任务，默认是 FIFO ——
第一个宽扫描会把所有 executor 吃干，后面的线程排队。必须改成公平调度：

```python
spark.conf.set("spark.scheduler.mode", "FAIR")
sc.setLocalProperty("spark.scheduler.pool", f"unit-{i % 4}")
```

不改这一行，你开 4 个 worker 和开 1 个没区别。

## 规矩六：本地这头有个刹车

```
在 S3 上等着被处理的 unit 数 = |_INDEX| - |results| - |dead|
如果 > --wip（默认 200）：Producer 停 15 秒再看
```

这不是限速，是上限 —— 防止 Producer 一路狂推，在 S3 上堆出几万个没人处理的
unit。

**踩到这个刹车说明什么？说明云端是瓶颈。** 这时候正确的反应是**加 worker**（或
者开 autoscale），不是去压 Producer。反过来，如果 `idle_ratio` 一直很高，说明
worker 开多了，减掉省钱。

**该开几个 worker？** 记住第一课那个观察：比对是毫秒级的，右侧扫描是分钟级的。
所以 worker 数由右侧扫描撑着：

```
W ≈ 右侧一个窗口的扫描时间 / Producer 产出一个窗口的间隔
  ≈ 3 分钟 / 3.5 分钟 ≈ 1，五个 pair 并行 → 4~5
```

**起步开 4，然后只看两个数**：`--wip` 有没有被踩到（不够），`idle_ratio` 高不高
（多了）。别去纸上推。

## 五个 pair 的完整时间轴

```
时间    本地 PC                          Databricks
────────────────────────────────────────────────────────────────────
00:00   run_id 生成
00:03   5 个 pair preflight 全过          serverless warehouse
00:15   右侧计数全回来 (5 条查询)         serverless warehouse
00:15   左侧计数开始：
          csv P4 / sas P5 本地
          hadoop P3 一个位子
          oracle P1,P2 两个位子
00:23   P4 过 barrier ★点火               集群 QUEUED
00:23   P4 开始推 unit (小，30s 完)       集群 STARTING
00:35   P5 过 barrier，开始推
01:00   P3 过 barrier，开始推
02:15   P1 过 barrier，开始推（大表优先）
04:30                                     集群 RUNNING
04:30                                     读 manifest：P4,P5,P3,P1 共 4 份
04:30                                     A 队列已有 P4/P5 的 unit → 立刻开比
04:31                                     B 队列排上 4 个 pair 的右侧窗口
05:10   P2 过 barrier，开始推
05:11                                     读到 manifest/P2.json，B 队列追加
...     P1/P2 轮转推 unit                  A 优先，A 空补 B，保底位常驻
18:40   最后一个 unit 推完
18:41   写 manifest/_CLOSED
          { pairs:[P1..P5], expected_units: 640 }
...                                       继续排空
19:05                                     640/640 + _CLOSED → finalize
19:30                                     3 张 Delta 表 + 5 份 index.html
19:40                                     report 桶就绪，latest/pointer.json
                                          告警发出
```

端到端 19 分 40 秒，五张表。如果串着跑（一张表跑完再下一张），大约是 55 分钟。

---

# 第四课：业务最后看到的东西

## report 桶长什么样

```
s3://dtrack-report/
  latest/
    pointer.json                      ← dashboard 唯一的入口
  history/
    20260917T1402Z-a3f9c1/
      summary.json                    机器读：状态、发现数、耗时
      run_report.html                 全部 5 个 pair 的总览，一页
      ecm_acct/
        index.html                    这个 pair 的详情，自包含可离线
        row_diff.csv                  Stage 1：每个 partition 一行
        col_diff.csv                  Stage 2：每列每窗口一行
      ecm_txn/ ...
      cust_mst/ ...
      ref_prod/ ...
      hist_bal/ ...
    20260916T1402Z-7b21de/            ← 昨天的，不动
```

`summary.json` 就是整个 run 的一句话结论：

```jsonc
{
  "run_id": "20260917T1402Z-a3f9c1",
  "status": "DONE",
  "started_at": "2026-09-17T14:02:00Z",
  "finished_at": "2026-09-17T14:21:40Z",
  "elapsed_s": 1180,
  "pairs": 5,
  "findings": {
    "row_count_diff": 7,      // Stage 1 抓到的
    "partition_missing": 2,
    "col_stat_diff": 11       // Stage 2 抓到的
  },
  "errors": [],
  "cost_estimate_usd": 1.42,
  "base": "history/20260917T1402Z-a3f9c1/"
}
```

## 三个消费方，三条路

| 谁 | 怎么看 | 读哪儿 |
| --- | --- | --- |
| **业务/领导** | Databricks AI/BI dashboard | Unity Catalog 的三张 Delta 表 |
| **技术团队** | 点开 `index.html`，或下 csv | report 桶 `latest/pointer.json` → `base` |
| **告警系统** | 每天定时读一次 | `latest/pointer.json` 的 `findings` 和 `errors` |

**为什么 dashboard 走 Delta 表而不是 S3？** 因为 dashboard 要做的是"这个指标最
近 30 天的趋势""哪张表问题最多"这类跨 run 的查询。那是 SQL 的活，表天生就能干；
从 S3 上读 30 个 json 再拼起来是自找麻烦。

**那 report 桶是给谁的？** 给 Databricks 外面的人和系统 —— 邮件附件、静态站点、
另一个团队的工具链。他们没有你 workspace 的账号，但有一个 S3 只读权限。

## 一个 run 的结论怎么读

```
run 20260917T1402Z-a3f9c1   DONE   19m40s   $1.42

Stage 1 —— 记录数对账
  ecm_acct    58 match /  2 count_diff /  1 left_only
  ecm_txn     61 match /  0            /  0
  cust_mst    59 match /  2 count_diff /  0
  ref_prod    61 match /  0            /  0
  hist_bal    60 match /  3 count_diff /  1 right_only
  → 7 个 partition 记录数不等，2 个 partition 单边缺失

Stage 2 —— 采样列统计（只在上面 match 的 partition 上做）
  ecm_acct    2026-06  ACCT_BAL     n_unique  48,221 vs 48,218
              2026-06  RISK_SCORE   hash_sum  mismatch
              2026-07  RISK_SCORE   hash_sum  mismatch
  cust_mst    2026-07  CUST_SEG     n_missing  1,204 vs 0
  ...
  → 11 个 (列 × 窗口) 统计不等

下一步：对上面每一条，用报告里给出的确认 SQL 回源做全量校验
```

最后那句是关键：**采样只负责"发现问题存在"，不负责"下结论"。** 每一条发现，报
告里都带一条现成的全量确认 SQL，直接能交给技术团队在他们那边跑。目前为止，采样
标出来的差异，回全量确认的命中率是 100%。

---

# 一页速查

**文件在哪**

```
work 桶   s3://dtrack-work/dtrack/v1/runs/{run_id}/
          _PREFLIGHT.json    体检结果
          _STATUS.json       ★ 跑到哪了、出了什么问题，就看这个
          _HEARTBEAT         Producer 还活着吗
          stage1/{pair}.json 全部 partition 的对账结论（人看）
          manifest/{pair}.json  只含过关 partition 的执行计划（机器看）
          manifest/_CLOSED   Producer 说"我推完了，一共 N 个"
          _INDEX/{seq}-{unit_id}.json   就绪指针，Consumer 只扫这个
          units/ / raw/ / results/ / dead/

report 桶 s3://dtrack-report/
          latest/pointer.json           ★ dashboard 唯一入口
          history/{run_id}/...          不可变归档
```

**三条命令**

```bash
dtrack s3-stage1 ... --gate auto --kickstart   # 数两边、对账、点火、接着推
dtrack s3-status ... --run-id latest           # 跑到哪了
dtrack s3-push   ... --run-id <id> --stage2    # --gate manual 时人工放行
```

**六条规矩**

1. Stage 1 并发按**左侧来源**限流，不是给一个全局数字。
2. **第一个**过 barrier 的 pair 负责点火，不等全部过完。
3. Stage 1 小表优先（早点火），Stage 2 大表优先（早收工）—— 故意相反。
4. Stage 2 出单**跨 pair 轮转**，不要一张表干完再下一张。
5. 云端给 A 类（比对）留一个保底 worker，B 类（抓右侧）最多占 W-1。
6. 踩到 `--wip` 刹车 = 云端不够，加 worker；`idle_ratio` 高 = worker 多了，减。

**两个最容易写错的地方**

1. 带洞窗口的 `WHERE`。Stage 1 排除掉的 partition，右侧抓取时如果又吃回来，结
   果会莫名不等且极难查。必须有单测。
2. `latest/` 逐个覆盖文件。会让 dashboard 读到半新半旧。必须只原子覆写
   `pointer.json` 一个文件。

**一条铁律**

> 记录数不等、统计值不等 = **发现**，进报告，不告警。
> 连不上、超时、跑挂了 = **故障**，告警，要人修。
> 这两件事永远分开算，否则告警会响到没人看。
