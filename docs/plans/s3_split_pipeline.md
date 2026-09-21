# 拆分式流水线：本地采样 → S3 → Databricks 持续消费

> 实施说明：本文是原始设计依据；本分支已明确推迟 SAS，只实现 Oracle、Athena、
> Hadoop 与 CSV 左侧适配，其中 Oracle/Databricks 为基础依赖，Athena/Hadoop 为可选依赖。

目标：把今天 `compare-csv` 的单进程闭环（两侧都由同一台机器拉取 + DuckDB 算统
计 + 比对）拆成两个**异步、互不阻塞**的半边：

- **Producer（本地 PC）**：负责能触达的**左侧**（csv / sas / hadoop / oracle /
  athena …），沿用现有 hash-bucket 采样，按 `(pair, partition, column)` 粒度把
  结果 + 元数据推到 S3。
- **Consumer（Databricks 常驻作业）**：持续从 S3 读取清单与数据，自己抓取**右
  侧（永远是 Databricks）**、算统计、完成比对、落库/出报告。

两条硬约束贯穿全文：

1. **两阶段，Stage 1 是 barrier。** Stage 1 只比 partition 级的记录数
   （并顺带验证 Databricks 是否连得上）；只有 Stage 1 判定为"记录数相等"的
   partition 才有资格进 Stage 2。记录数本身就不等的数据，采样后的列统计必然不
   等，算了也是噪声。
2. **左右不对称。** 左侧是可插拔的多种来源，右侧**只能是 Databricks**。

本文回答：**(1) 有了 Stage 1 这个 barrier，怎么还能让 Databricks 的队列一直排
满；(2) S3 这一层的 job load / kickstart 怎么设计。**

基线：分支 `pkg`。Stage 1 对应现有 `_date_counts` / `_matched_dates` /
`_save_row_compare` / `compare-row`；Stage 2 对应 `_run_pair` 的
`_hash_bucket_where` → `_pull_side` → `_extract_and_load` → `_vintage_compare`。

---

## 0. 结论先行

barrier 看起来会让云端空转，但它不会 —— 靠三件事：

1. **Stage 1 根本不需要那个大集群。** 它在右侧只发一条
   `SELECT dt, COUNT(*) GROUP BY dt`，用 **serverless SQL warehouse**（秒级启
   动、按查询计费）就够。Stage 2 才用 job cluster。所以 Stage 1 期间"没有集群
   在空转"，因为集群还没起。
2. **barrier 是 per-pair，不是 global。** pairA 过了 Stage 1 就能进 Stage 2，
   此时 pairB 还在 Stage 1。manifest 因此必须是**增量**的
   （`manifest/{pair}.json` 逐个落地），不是一次写死的单文件。
3. **Stage 2 里 Consumer 永远有两类活。** 确定性 reject 采样
   （`hash32(normalized key) < rate × 2^32`）两侧完全独立 —— 右侧要抓哪些行只
   取决于 manifest 里的 `key_plan + threshold + matched_partitions`，**不依赖左
   侧文件**。于是：

| 工作类 | 依赖 | 供给量 |
| --- | --- | --- |
| **A. 比对（dependent）** | 左侧 unit 文件 + 该 partition 的右侧统计都就绪 | 受 Producer 限速 |
| **B. 右侧抓取+统计（independent）** | 只需要该 pair 的 manifest | **该 pair 过 barrier 的瞬间就全量可见** |

调度规则一句话：**优先做 A，A 空了就投机去做 B。** B 是一个"只要有一个 pair 过
了 barrier 就立刻装满"的 backlog，所以 worker 不会空转。

**kickstart 的时机由此确定：第一个 pair 通过 Stage 1 的那一刻。** 集群的
3–7 min 冷启动，与"剩余 pair 的 Stage 1 + 第一个 window 的左侧拉取"完全重叠。

> 例外：`--sample 500` 这种 **fixed-N IN-list** 模式下，右侧依赖左侧选出的 key
> 列表，B 类不再独立。见 §5.3。**拆分架构建议默认用 fraction 采样
> （`--sample 0.005`）**，因为它零协调。

---

## 1. 两阶段与 barrier

```
              ┌──────────────── Stage 1（barrier）─────────────────┐
              │ 0. Databricks preflight（连通性/权限/表/列）        │
              │ 1. 右侧 partition 记录数（SQL warehouse，一条查询） │
              │ 2. 左侧 partition 记录数（按 source 走最便宜的精确路径）│
              │ 3. 对账 → 每个 partition 一个 verdict              │
              │ 4. 写 stage1 报告 + manifest/{pair}.json           │
              └───────────────────────┬───────────────────────────┘
                                      │ 只有 verdict=match 的 partition 过关
              ┌───────────────────────▼───────────────────────────┐
              │ Stage 2（采样列统计）                              │
              │ 左：本地拉样本 → DuckDB 统计 → 切列 → S3           │
              │ 右：Databricks 抓样本 → 统计                       │
              │ 比对 → results/                                    │
              └───────────────────────────────────────────────────┘
```

Stage 1 的输出本身就是**第一个交付物**（"哪些 partition 的记录数不一样"是业务
要看的结论），不是给 Stage 2 用的中间态。所以它独立成报告、独立签字。

### 1.1 partition 的身份

现有代码里 partition 就是 `date_col` 的取值（`parse_date` 规范化后），按
`vintage` 再分桶。拆分后要区分三层，不要混：

| 层 | 含义 | 用途 |
| --- | --- | --- |
| **partition** | 左右双方都能识别的对账键 = 规范化日期 | **Stage 1 的对账粒度** |
| **window** | `bucket_date(partition, vintage)`，一次扫描的单位 | Stage 2 的扫描粒度 |
| **unit** | `(pair, window, column)` | Stage 2 的交付/比对粒度 |

**Stage 1 必须在 partition 粒度对账，不能在 window 粒度。** 否则一个月里某天
差 3 条，整月就被判为 count-diff 而整月出局，或者反过来被月总数的巧合掩盖。
Stage 2 的 window 只由**通过 barrier 的 partition 子集**构成 —— 现有
`_window_where` 的"把日期列表折成连续区间"逻辑（`_contiguous_runs`）正好支持
"带洞的 window"，直接复用。

文件型左侧（csv / sas）没有 SQL partition，只有文件。所以 Stage 1 额外产出一张
`partition_map`：

```jsonc
"partition_map": {                     // 仅文件型左侧
  "2026-06-01": ["ecm_20260601_01.sas7bdat", "ecm_20260601_02.sas7bdat"],
  "2026-06-02": ["ecm_20260602.sas7bdat"]
}
```

用途有二：(a) 记录数按文件累加到 partition 上；(b) Stage 2 拉某个 window 时只
读相关文件，不扫整目录。

### 1.2 Stage 1 的记录数必须是精确值

这是一个**等值判定**，近似统计会直接导致误判/漏判。所以：

- ✗ Hive `SHOW PARTITIONS` + `TBLPROPERTIES.numRows`、Oracle
  `USER_TAB_PARTITIONS.NUM_ROWS`：这些是统计信息，可能过期，**不能用作 gate**。
- ✓ 一律 `SELECT <date> , COUNT(*) GROUP BY <date>`。它是元数据级/单列扫描，
  相对于 Stage 2 的数据下载便宜得多。
- 例外：csv / sas 只能靠读文件拿精确行数 —— 这是文件型左侧 Stage 1 最贵的地
  方，见 §2.3。

`date_synonyms`（哨兵日期折叠，如 `3000-01-01 → 2026-07-01`）必须在对账**之
前**应用到两侧，现有 `apply_date_synonyms` 直接复用。

### 1.3 verdict 与 gate

每个 partition 四种结局：

| verdict | 含义 | 进 Stage 2？ |
| --- | --- | --- |
| `match` | 两侧记录数相等 | ✓ |
| `count_diff` | 都有，但记录数不等 | ✗（本身就是 finding） |
| `left_only` | 右侧缺这个 partition | ✗ |
| `right_only` | 左侧缺这个 partition | ✗ |

gate 策略（新子命令的 flag）：

- `--gate auto`（默认）：per-pair barrier 一过就自动进 Stage 2。
- `--gate manual`：Stage 1 全部 pair 跑完 → 写报告 → **停住**，等人看完后
  `dtrack s3-push --stage2 --run-id <id>` 续跑。适合第一次接一张新表、或
  `count_diff` 比例超过阈值时。
- `--gate ratio:0.3`：某 pair 的 `count_diff + single_side` 占比超过 30% 就把该
  pair 挡在 barrier 外（数据本身有系统性问题，跑 Stage 2 是浪费），其余 pair 照
  常放行。

这三条都写进 `stage1.json` 的 `gate_decision`，Consumer 只看 manifest 里出现了
哪些 pair，不重复判断。

---

## 2. 左右不对称

### 2.1 来源矩阵

| source | 作左侧 | 作右侧 | Stage 1 计数 | Stage 2 采样下推 |
| --- | --- | --- | --- | --- |
| `databricks` | （可以但没必要） | **✓ 唯一** | SQL warehouse | Spark/SQL |
| `oracle` | ✓ | ✗ | `GROUP BY` | ✓ `_oracle_portable_hash16` |
| `hadoop`(Hive) | ✓ | ✗ | `GROUP BY` | ✓（注意 Hive 2.0.0 缺 `NULLIF`） |
| `aws`(Athena) | ✓ | ✗ | `GROUP BY` | ✓ |
| `csv`(DuckDB) | ✓ | ✗ | 读文件 | ✓ 本地 DuckDB |
| `sas`（新增） | ✓ | ✗ | 读文件 | ✗ 无下推，本地过滤 |

右侧收敛到一种，带来两个实质简化：

1. **Consumer 只需要 `platforms/databricks.py`**，不用打包 oracledb /
   pyathena / pyodbc / ldap3。云端依赖表因此极小、集群 init 脚本极快。
2. **hash 表达式只需要保证"每个左侧 × databricks"的两两一致**，不需要 N×N 全
   矩阵。现有跨引擎 hash parity 的验证面从 `C(5,2)=10` 降到 4 对。

### 2.2 LeftSource 契约

把左侧收进一个窄接口，新增来源只实现这 4 个方法：

```python
# >>> 左侧来源契约：Stage 1 两个方法，Stage 2 两个方法 <<< #
class LeftSource(Protocol):
    def preflight(self) -> Health: ...                    # 连通性/权限/表存在
    def partition_counts(self, from_dt, to_dt) -> dict[str, int]: ...   # Stage 1
    def discover_columns(self) -> dict[str, str]: ...     # {col: TYPE}
    def pull_window(self, partitions, cols, sample_pred, out_csv,
                    arraysize) -> int: ...                # Stage 2，流式写 CSV
```

现有 `platforms/{oracle,athena,hadoop,csv_duckdb}.py` 的 `_query_*` / `_iter_*`
已经等价于后两个方法，只是没有统一签名；这次拆分顺手把签名收口，是低成本的。

### 2.3 SAS（新增，最不规则的一个）

`sas7bdat` 没有 SQL 引擎，所以：

- **读取**：`pyreadstat.read_file_multiprocessing`（比 `pandas.read_sas` 快数
  倍，且能只读需要的列），或 DuckDB 的 `sas` 扫描（若版本支持）。落地为
  parquet 缓存后，后续全部走 DuckDB。
- **Stage 1 计数很贵**：行数只能靠读。两级优化：
  (a) `pyreadstat.read_file_metadata` 能拿到 `number_rows` —— 如果一个文件就是
  一个 partition（常见的 `ecm_20260601.sas7bdat` 命名），**元数据就够了，零读
  取**，这是最理想的情况；
  (b) 一个文件含多个日期时，只读 `date_col` 一列做 `value_counts`，仍比全读便
  宜一两个数量级。
- **Stage 2 无下推**：hash 采样谓词没法下推给 SAS，只能本地算。流程变成
  `sas7bdat → parquet（一次）→ DuckDB 里施加 hash32 谓词 → 样本 CSV`。
  parquet 缓存按 `(file, mtime, size)` 做键，跨 run 复用。
- **dtype 映射**：SAS 的数值全是 double、日期是自 1960-01-01 起的天/秒数。
  必须在 `constants.py` 里加 `sas_date` / `sas_datetime` 两个 `date_type`，
  否则日期对不上。这是 SAS 接入最容易出错的一处。

`sas/` 目录目前是空的，属于全新实现；建议排在阶段 F（见 §11），不要和拆分本身
的正确性验证混在一起。

---

## 3. S3 布局与提交协议

```
s3://<bucket>/dtrack/v1/
  runs/{run_id}/
    _PREFLIGHT.json                                # Databricks 健康检查结果
    stage1/{pair}.json                             # ★ 每个 partition 的 verdict
    stage1/_CLOSED                                 # Stage 1 全部 pair 完成
    manifest/{pair}.json                           # ★ 增量：过了 barrier 才出现
    manifest/_CLOSED                               # {pairs: [...], expected_units: N}
    _HEARTBEAT                                     # Producer 每 60s 覆写
    units/{pair}/{window}/{column}.json            # payload B（统计）
    raw/{pair}/{window}/part-0000.parquet          # payload A（可选，宽表）
    _INDEX/{seq:08d}-{unit_id}.json                # ★ 单调递增的"就绪指针"
    results/{unit_id}.json                         # Consumer 输出
    dead/{unit_id}.json                            # 超过 max_attempts
```

**提交顺序是协议的全部：先写数据对象，再写指针对象。** S3 单对象 PUT 原子，且
自 2020-12 起 LIST 强一致，所以 Consumer 只扫 `_INDEX/` 与 `manifest/` 就不可能
读到半截文件。

`_INDEX` 用零填充递增序号命名：Consumer 记住上次的最大 seq，
`list_objects_v2(StartAfter=...)` 就是**增量列举**，与 run 总量无关。

`manifest/` 与 `stage1/` 是两份不同的东西，别合并：`stage1/{pair}.json` 是**全
部** partition 的对账结论（含被排除的，要出报告）；`manifest/{pair}.json` 是
Stage 2 的**执行计划**（只含过关的 partition）。

### 3.1 stage1/{pair}.json

```jsonc
{
  "run_id": "20260917T1400Z-a3f9c1",
  "pair": "ecm_sas_vs_dbx",
  "left":  {"source": "sas", "path": "//nas/ecm/", "date_col": "RPT_DT",
            "date_type": "sas_date", "count_method": "file_metadata"},
  "right": {"source": "databricks", "catalog": "main", "schema": "ecm",
            "table": "ecm_daily", "date_col": "rpt_dt",
            "date_type": "string_dash", "count_method": "group_by"},
  "date_synonyms": {"3000-01-01": "2026-07-01"},
  "bounds": {"from": "2026-06-01", "to": "2026-07-31"},
  "partitions": {
    "2026-06-01": {"left": 1234567, "right": 1234567, "verdict": "match"},
    "2026-06-02": {"left": 1234567, "right": 1234560, "verdict": "count_diff",
                   "delta": -7},
    "2026-06-03": {"left": 998,     "right": null,    "verdict": "left_only"}
  },
  "summary": {"match": 58, "count_diff": 2, "left_only": 1, "right_only": 0},
  "partition_map": {"2026-06-01": ["ecm_20260601.sas7bdat"]},
  "gate_decision": {"policy": "ratio:0.3", "passed": true,
                    "excluded_ratio": 0.049},
  "timing": {"preflight_s": 1.2, "right_count_s": 8.4, "left_count_s": 41.0}
}
```

### 3.2 manifest/{pair}.json（Stage 2 的执行计划）

```jsonc
{
  "run_id": "...", "pair": "ecm_sas_vs_dbx",
  "sample": {"mode": "fraction", "rate": 0.005, "threshold": 21474836},
  "vintage": "month",
  "key_cols": ["ACCT_ID", "SUB_ID"],
  "key_plan": [{"pos": 0, "left_norm": "...", "right_norm": "..."}],
  "col_map": {"ACCT_BAL": "acct_bal"},
  "columns": ["ACCT_BAL", "..."],                // 已应用 col_filter
  "windows": [
    {"bucket": "2026-06", "partitions": ["2026-06-01", "2026-06-04"],
     "n_left": 2469134},                          // 注意：带洞
    {"bucket": "2026-07", "partitions": ["..."]}
  ],
  "right": {"catalog": "main", "schema": "ecm", "table": "ecm_daily",
            "date_col": "rpt_dt", "date_type": "string_dash"},
  "stats_preset": "fine", "strict": true,
  "expected_units": 60                            // = windows × columns
}
```

`manifest/_CLOSED` 汇总 `expected_units` 总数，Consumer 靠它判断"还会不会有新
pair 进来"。**Producer 必须在这一步之前保证 `manifest/{pair}.json` 已全部落
地**，否则 Consumer 会提前收摊。

### 3.3 Producer 的断点续跑

- Stage 1：`stage1/{pair}.json` 已存在 → 跳过该 pair（记录数缓存也落
  `project.db._row_counts`，与现有 `_matched_dates` 的复用路径一致）。
- Stage 2：列 `_INDEX/`，已存在的 `unit_id` 跳过（等价于现有
  `_loaded_buckets` 的跳过语义）。
- `--force` 换新 `run_id`，天然隔离。

### 3.4 Payload 选哪种

| 方案 | 内容 | 体积/unit | 取舍 |
| --- | --- | --- | --- |
| **B（默认）** | 本地 DuckDB 算好的 `_col_stats` 行：`n_total / n_missing / n_unique / min / max / hash_sum` | ~1 KB | 极小、S3 成本可忽略、队列永不被 I/O 卡住；要求跨引擎 stat 语义一致 |
| **A** | 原始采样行（per window 宽表） | MB 级 | 云端两侧同源计算；但 on-prem 明细出域有合规问题 |

**建议：默认 B；`--upload-raw` 时额外传一份 window 级宽表 parquet**，只为差异
列的云端下钻，避免回源重拉。合规不允许时关掉，下钻仍走现有"回源单列确认查询"。

---

## 4. Producer 设计（本地 PC）

```
dtrack s3-stage1 {project.db} --config pairs.json --s3 s3://bucket/dtrack/v1 \
                 [--gate auto|manual|ratio:0.3] [--kickstart] [--pair-workers 4]

dtrack s3-push   {project.db} --config pairs.json --s3 ... --run-id <id> \
                 [--sample 0.005] [--vintage month] [--upload-raw] \
                 [--wip 200] [--workers 4]
```

拆成两个子命令（而不是一个 `--stage` flag），因为它们的失败语义、重试策略、权
限需求都不同：Stage 1 只需要 SQL warehouse 读权限，Stage 2 才需要 jobs 触发权
限；Stage 1 失败要停，Stage 2 失败可以部分交付。`--gate auto` 时
`s3-stage1` 会自动串到 `s3-push`。

### 4.1 Stage 1 的执行顺序（顺序本身是设计）

```
1. Databricks preflight  ← 最先，因为最可能失败且最便宜
2. 右侧 partition counts （所有 pair 并行，同一个 warehouse）
3. 左侧 partition counts （按 source 分组并发，受各源会话数限制）
4. 对账 + gate + 写 stage1/{pair}.json + manifest/{pair}.json
```

**preflight 放第一位**是关键：若 Databricks 连不上，要在动左侧一根手指之前就失
败，而不是把 Oracle 扫完才发现右边没法比。preflight 检查项（全部写进
`_PREFLIGHT.json`）：

| 检查 | 方式 | 失败即 abort |
| --- | --- | --- |
| 认证 | OAuth M2M 拿 token / `current_user()` | ✓ |
| warehouse 可用 | `SELECT 1` | ✓ |
| catalog/schema/table 存在 | `DESCRIBE TABLE EXTENDED` | ✓ |
| 列存在且类型可映射 | 与 `col_map` 比对 | ✓ |
| `date_col` 存在且 `date_type` 对得上 | `MIN/MAX(date_col)` 取样 | ✓ |
| jobs 触发权限 | `GET /api/2.2/jobs/get?job_id=` | ⚠ 只警告（Stage 1 不需要） |
| Stage 2 的 job cluster 配置合法 | dry-run `run-now` 不做，只校验 job 定义 | ⚠ |

现有 `dtrack doctor` 只查环境变量、不发真连接；preflight 是**活体检查**，应该
把 doctor 的输出并进来一起打印，但判定独立。

**右侧先于左侧**：右侧永远是一条便宜查询，几秒钟就能把"哪些 partition 右边根本
没有"确定下来；左侧随后计数时就能加上 `date IN (右侧已有的 partition)` 的边界
（对 Oracle/Hive 是有效的分区裁剪），少扫一批注定 `left_only` 的数据。

### 4.2 Stage 2 的内部流水线（三级，全部重叠）

```
[plan] ─> [pull] ×W_pull ─> [stat] ×W_stat ─> [upload] ×W_up ─> S3
manifest   左侧扫描(慢,I/O)   DuckDB(CPU)      切列+PUT(小,网络)
```

今天 `_run_pair` 是严格串行（逐 bucket `pull → extract → compare`），拆分后要
改成有界队列串起来的三段。`W_pull` 受源库并发会话数限制（Oracle 建议 2–4）是真
瓶颈；`upload` 的 unit 是 KB 级，30 列 × 4 线程 ≈ 0.3 s，可忽略。

**扫描按 window，交付按 unit**：一个 window 仍然只发**一条** SQL 拉全部 value
列（manuscript 里"286 次全表扫描 → 1 次"就是靠这个），只在**上传阶段**才按列
切成 N 个 unit 文件。切分在本地做，免费。收益：1 次源库扫描喂饱 ~30 个云端槽
位，Producer 天然跑在 Consumer 前面。

### 4.3 出单顺序：round-robin，不要 depth-first

今天是 `for pair: for bucket in sorted(buckets)`。拆分后**必须跨 pair 轮转**：

```
(pairA, 2026-01) (pairB, 2026-01) (pairC, 2026-01) (pairA, 2026-02) ...
```

理由：Consumer 的右侧抓取是按 (pair, window) 打到不同 Databricks 表的。若
Producer 深度优先，就绪队列长期只含同一张右表的 unit，并发全挤在一张表上，有效
并行度掉到 1。轮转让相邻 unit 落在不同右表上。

### 4.4 背压（high-water mark）

不限速，只设上限：

```
ready_unclaimed = |_INDEX| - |results| - |dead|      # 两次 list 计数，缓存 30s
while ready_unclaimed > --wip: sleep 15s
```

`--wip` 默认 200。触发背压说明 Consumer 是瓶颈 → 加 Consumer 并发，而不是压
Producer。

---

## 5. Consumer 设计（Databricks 常驻作业）

```
dtrack s3-drain --s3 s3://bucket/dtrack/v1 --run-id <id|latest> \
                --workers 8 --idle-timeout 900 --prefetch-depth 3
```

### 5.1 架构选型：单 driver 协调器 + 线程池

| 方案 | 评价 |
| --- | --- |
| 每个 unit 一次 Databricks job run | ✗ 每 run 30–60 s 开销 + 集群抖动 |
| Delta `work_queue` 表 + 多 worker `MERGE` 抢占 | ✗ 并发 MERGE 会 `ConcurrentAppendException`，要加分区隔离和重试，复杂度不值 |
| **单个常驻 job，driver 上一个协调器 + N 个工作线程** | ✓ **推荐** |

只有**一个** job run 在跑 → 抢占不需要分布式锁，内存队列即可。Delta 只做**持久
化 checkpoint**（`run_state` 表，每完成一个 unit 写一行）供崩溃续跑，正常路径不
走它。多 run 并行靠 `run_id` 隔离，一个 run 一个 job run
（`max_concurrent_runs=1`）。

线程池里同时提交多个 Spark action 时必须：

```
spark.conf.set("spark.scheduler.mode", "FAIR")
sc.setLocalProperty("spark.scheduler.pool", f"unit-{i % 8}")
```

否则默认 FIFO 下第一个宽扫描独占全部 executor，后面线程排队 = 又空转了。

### 5.2 调度循环（§0 的落地）

```python
# >>> 永不空转：A 类优先，A 空则投机做 B 类；manifest 是增量的 <<< #
while True:
    poll_manifests()                   # 新过 barrier 的 pair -> 追加 B 类 backlog
    poll_index()                       # 增量 list _INDEX/, StartAfter=last_seq
    a = queue_a.pop()                  # 左侧已到 & 该 window 右侧统计已算好
    if a: run(compare, a); continue
    b = next_prefetch()                # 按 manifest windows 顺序，未算过的右侧
    if b: run(fetch_right_and_stat, b); continue
    if manifests_closed() and all_done(): break
    if stale_heartbeat(): abort_with_partial_report()
    backoff_sleep()                    # 1 -> 2 -> 4 ... 上限 30s
```

- `queue_a` 入队条件：左侧 unit 文件到达 **AND** 该 `(pair, window)` 的右侧统计
  已在本地 Delta `right_stats` 里。右侧统计是 **window 级**的，一次抓取喂饱该
  window 全部列。
- 右侧抓取的 `WHERE` 必须用 manifest 里**带洞的** partition 列表（复用
  `_contiguous_runs` 折出的区间并集），不能简单用 `bucket` 的月首月末 —— 否则
  就把 Stage 1 排除掉的 partition 又吃回来了。**这是整套设计里最容易写错的一
  行。**
- `--prefetch-depth` 限制 B 类跑在 Producer 前面多少个 window（默认 3），避免
  把整轮云端扫描在前 10 分钟做完然后真空转，也避免 Producer 中途失败时白烧钱。

### 5.3 fixed-N 模式的降级路径

`--sample 500` 时右侧依赖左侧 key 列表，B 类不再独立。此时 Producer 在
Stage 1 之后插一个 **Phase 1.5**：对每个过关 window 跑一次 `_window_keys`（只取
key 列，窄查询），把全部 window 的 key 列表打包成
`manifest/{pair}.keys.json` 一次性上传。这样 B 类又全量可见了。Phase 1.5 的总耗
时 ≈ window 数 × 一条窄查询，通常短于第一个 window 的数据拉取，不上关键路径。

### 5.4 退出条件

正常退出：`manifest/_CLOSED` 存在 **且** `|results| + |dead| == expected_units`。
两条兜底：

- `_HEARTBEAT` 超过 `3 × interval` 未更新 → 判定 Producer 死亡，出部分报告，非
  零码退出。
- `--idle-timeout`（默认 15 min）内既无新 unit、又无 B 类可做 → 退出，防止
  Producer 挂在半路且心跳也没了时烧集群。

### 5.5 重试与死信

每个 unit 带 `attempts`；失败 → 指数退避回队尾；`attempts > 3` → 写
`dead/{unit_id}.json`（含 traceback），计入完成数，不阻塞退出。结果以 `unit_id`
为键，天然幂等。

### 5.6 输出

- `results/{unit_id}.json`：与现有 `_VINTAGE_COMPARE_FIELDS` 同构的一行；
- 收尾汇总成 `{pair}_compare_col.csv`（与今天字节同构）+ Delta
  `dtrack.compare_col` 表 → 接 AI/BI dashboard 与 alert；
- **Stage 1 的报告也要一起出**：`{pair}_compare_row.csv` / `_row_comparison`，
  因为"哪些 partition 记录数不一样"是独立交付物；
- 可选回写 `project.db`（SQLite）到 S3，让本地 `dtrack compare-row/compare-col
  --html` 仍能出原来的 HTML/Excel。

---

## 6. Kickstart：S3 job 怎么拉起来

### 6.1 三种触发方式

| 方式 | 延迟 | 问题 |
| --- | --- | --- |
| Cron（每 15 min，跑完即退） | 差 | 每次冷启动 3–7 min，一轮 run 反复启停 |
| S3 Event → EventBridge/SQS → `jobs/run-now` | 好 | 几百个 unit 触发几百次 run，必须 debounce；冷启动仍在关键路径 |
| **Producer 主动 kickstart + 自排空长作业** | **最好** | 推荐 |

### 6.2 推荐时序（barrier 决定触发点）

```
t=0      s3-stage1: Databricks preflight（SQL warehouse，秒级）
              ├─ 失败 -> 立即 abort，左侧一根手指都没动
t=0+10s  右侧 partition counts（所有 pair 并行，一条查询/pair）
t=0+1m   左侧 partition counts（按 source 并发）
t=0+2m   pairA 对账完 -> stage1/pairA.json + manifest/pairA.json
t=0+2m+1s ★ POST /api/2.2/jobs/run-now
              { job_id, idempotency_token: run_id,
                job_parameters: { run_id, s3_root } }
t=2m..6m 集群启动        ║ 并行：pairB/pairC 的 Stage 1 + pairA 第一个 window 拉取
t=~3m    Consumer 起来即读 manifest/pairA.json -> B 类 backlog 装满，开始预取右侧
t=~7m    第一批左侧 unit 落地 -> A 类开始
...
t=T      Producer 写 manifest/_CLOSED{expected_units}
t=T+ε    Consumer 排空 -> 汇总报告 -> 退出 -> 集群释放
```

四个要点：

1. **kickstart 在"第一个 pair 通过 barrier"时发，不在 run 开始时发**，也不等全
   部 pair 过完。这样既不会在 preflight 失败时白起集群，也让集群启动与剩余
   Stage 1 完全重叠。
2. **`idempotency_token = run_id`**：Producer 重试/网络抖动重复调用都不会起第二
   个 job run。这是整套设计里唯一需要的"锁"。
3. **Stage 1 用 serverless SQL warehouse，Stage 2 用 job cluster**
   （`autoscale min=1 max=W`、`max_concurrent_runs=1`）。Jobs Compute 的 DBU 单
   价比 All-Purpose 低一半以上；把 Stage 1 从集群上摘出去，barrier 期间的云端成
   本接近零。
4. **兜底 cron**：每小时一次 `run-now`，挡重靠 `max_concurrent_runs=1`。用于
   Producer 的 kickstart 调用失败（本地网络/权限）时仍能把 run 捞起来，最多晚
   1 小时，但不会永久卡死。
5. `--gate manual` 时**不发** kickstart；人工放行的 `s3-push --stage2` 才发。

### 6.3 Producer 需要的最小 Databricks 权限

- Stage 1：一个 SQL warehouse 的 `CAN_USE` + 目标表的 `SELECT`；
- Stage 2：额外一个指定 job 的 `CAN_MANAGE_RUN`。

不需要 workspace 写权限、不需要 databricks-connect、不需要长期 PAT
（用 OAuth M2M service principal，`DATABRICKS_CLIENT_ID/SECRET`）。

---

## 7. 容量：怎么判断"排满"是可达的

设 Producer 产出速率 λ（unit/min），单 worker 服务速率 μ，worker 数 W。

- **W·μ > λ** → Consumer 稳态空闲，靠 B 类预取填补；预取做完就真空转。正确反应
  是**减 W**（省钱），不是加队列。
- **W·μ < λ** → Consumer 是瓶颈，Producer 会撞 `--wip`。正确反应是加 W
  （autoscale 自动做）。
- **理想 W = ceil(λ/μ)**，再 +1 抗长尾。

按 manuscript 的量级估：30 列 × 1 个月 × 0.5% 采样，全程 ~9 min。`vintage=day`
时 30 window × 30 列 = 900 unit，λ ≈ 100 unit/min。单 unit 的比对（两侧统计行相
减）是毫秒级，μ 极大 → **A 类根本不是瓶颈，B 类的右侧扫描才是**。所以实际
`W ≈ ceil(t_scan_right / t_pull_left)`，通常 2–4 够。

**起步 W=4，看 `--wip` 有没有被触发来决定加减。** job 里打点
`idle_seconds / claimed_seconds`，直接读比例。

Stage 1 本身的耗时预算（因为它是 barrier，直接加在端到端时间上）：

| 左侧 | 典型 Stage 1 耗时 | 优化手段 |
| --- | --- | --- |
| oracle / hadoop / athena | 秒~数分钟（单列 GROUP BY） | 加右侧 partition 边界裁剪 |
| csv | 秒级（DuckDB 扫单列） | — |
| **sas** | **可能是最贵的一环** | 文件=partition 时用 `read_file_metadata`，零读取 |

---

## 8. 安全与成本

- **凭据**：本地 PC 用 STS 短期凭据（IAM Role + `AssumeRole`），策略前缀限定到
  `dtrack/v1/runs/*`，只给 `PutObject/ListBucket`；Databricks 侧走 Unity
  Catalog External Location / Instance Profile，只给 `Get/List/Put`。两边都不放
  长期 access key。
- **加密**：bucket 默认 SSE-KMS。启用 `--upload-raw` 必须过 on-prem 数据出域评
  审 —— 明细行 vs 统计值是两个不同的合规等级。
- **生命周期**：`runs/` 前缀 30 天过期；`raw/` 7 天过期。
- **小文件**：payload B 是 KB 级 JSON，900 个也只有 ~1 MB；payload A 必须
  window 级 coalesce 成单文件，**禁止 per-column parquet**。
- **列举成本**：`_INDEX/` + `StartAfter` 增量列举，一轮 poll ≈ 1 次
  `ListObjectsV2`。2s 一轮、1 小时的 run ≈ 1800 次请求 ≈ $0.01。
- **barrier 的成本红利**：Stage 1 就把 `count_diff` 的 partition 挡掉了，这部分
  数据的左侧下载 + 右侧扫描 + 云端计算全部省下。`count_diff` 占比越高，省得越
  多 —— 这是两阶段设计除了正确性之外的第二个理由。

---

## 9. 代码落点

新增 `dtrack/cloud/`（ccsv 同步镜像）：

| 文件 | 内容 |
| --- | --- |
| `preflight.py` | Databricks 活体检查 → `_PREFLIGHT.json`；并入 `doctor` 输出 |
| `stage1.py` | partition 计数对账 + verdict + gate；复用 `_date_counts` / `apply_date_synonyms` / `compare_row_counts` |
| `manifest.py` | `stage1/*.json` 与 `manifest/*.json` 的 schema/读写/校验；`unit_id` 规范化 |
| `s3io.py` | boto3 封装：原子提交（data → index）、增量列举、心跳、`_CLOSED` |
| `producer.py` | Stage 2 的 `plan → pull → stat → upload` 三级流水线；round-robin；背压 |
| `consumer.py` | 协调器 + A/B 双队列 + 线程池 + drain loop + 退出条件 |
| `right_side.py` | Databricks 侧 window 抓取与统计（带洞 partition 区间！） |
| `kickstart.py` | `jobs/run-now` + idempotency token + OAuth M2M |

新增 `dtrack/platforms/sas.py` + `LeftSource` 协议收口（`platforms/base.py`）。

`cli.py` 加三个子命令 `s3-stage1` / `s3-push` / `s3-drain`；`justfile` 加 recipe。

**现有代码需要的改动**：
- `_run_pair` 的串行 bucket 循环抽成可复用的 `iter_windows(pairs)` 生成器（支持
  round-robin）；
- `_matched_dates` 拆成"取计数"与"判 verdict"两半 —— 现在它把两件事和一句
  `print` 揉在一起，Stage 1 需要结构化的 verdict 而不是一个 matched 列表；
- `_pull_side` / `_extract_and_load` 保持不变（这是拆分能低成本落地的前提）。

---

## 10. 分阶段交付

| 阶段 | 内容 | 验收 |
| --- | --- | --- |
| **A** | `preflight.py` + `stage1.py` + `manifest.py`，`--gate manual` 落地 | `testing/pairs.json` 上产出 `stage1/*.json`，verdict 分布与今天 `dtrack compare-row` 逐条一致；Databricks 断连时在碰左侧之前失败 |
| **B** | `s3io.py` + 提交协议 + 增量 manifest | minio/moto 上验证"先 data 后 index"的原子性、`StartAfter` 增量列举、`_CLOSED` 语义 |
| **C** | Producer Stage 2 三级流水线 + round-robin + 背压 | 本地跑 `csv` 源 pair，S3 上 `_INDEX` 完整、unit 数 == `expected_units` |
| **D** | Consumer 单机版（`--right-source csv`，不上 Databricks） | 排空后产出的 `{pair}_compare_col.csv` 与今天 `just compare-csv` **字节相同** ← **正确性锚点** |
| **E** | 右侧换真 Databricks + FAIR pool + Delta checkpoint 续跑 | `tests/containers` 起 Trino/Hive 做代理验证；`kill -9` Consumer 后重启，无重复无遗漏；带洞 partition 的 `WHERE` 有专门的单测 |
| **F** | kickstart + 心跳/超时/死信 + 指标打点 | 端到端一轮：`idle_seconds / total < 5%`；杀掉 Producer → Consumer 在 3×heartbeat 内带部分报告退出 |
| **G** | `platforms/sas.py` + `sas_date` / `sas_datetime` + parquet 缓存 | 一个 `sas ↔ databricks` 的 pair 端到端跑通；`read_file_metadata` 路径的 Stage 1 零读取验证 |

阶段 D 的"字节相同"是硬门槛：在它通过之前不要接 Databricks，否则分不清是拆分
逻辑错了还是跨引擎语义差异。阶段 G（SAS）刻意排在最后 —— 它是全新 source，别和
拆分本身的正确性验证纠缠。

---

## 11. 已知风险

1. **barrier 直接加在端到端时间上。** 若左侧 Stage 1 计数很慢（Oracle 大表单列
   GROUP BY、SAS 需要读文件），barrier 就成了新的瓶颈。缓解：右侧先算并用来裁剪
   左侧、per-pair 放行、SAS 走文件元数据。**必须打点 `timing` 字段**，否则事后
   无从优化。
2. **带洞 partition 的 `WHERE` 是最容易错的一处。** Stage 1 排除掉的 partition
   如果在右侧抓取时又被吃回来，比对结果会莫名不等且极难查。必须有针对
   `_contiguous_runs` + `_window_where` 的单测，覆盖"月中挖洞""首尾挖洞""整个
   window 只剩一天"三种形状。
3. **本地 DuckDB 与 Spark 的 stat 语义差异**（payload B 的前提）。`n_unique` 必
   须是精确 `COUNT(DISTINCT)`，`hash_sum` 用已验证的 hash16 表达式，`min/max`
   只在 STRICT 下规范化。右侧收敛到 Databricks 后验证面从 10 对降到 4 对，但每
   对都要有 container 级测试。
4. **SAS 的日期基准（1960-01-01）与 double 数值精度**。日期映射错会让整个
   Stage 1 对账全线 `left_only`；double 精度会让 `min/max/hash_sum` 与
   Databricks 的 decimal 对不上。接 SAS 时先只跑 Stage 1 验证 partition 对得
   上，再开 Stage 2。
5. **Producer 是单点。** 本地 PC 断网 = 整轮停摆。心跳 + 断点续跑能让重启后接着
   跑，但没有 HA。要 HA 就得把 Producer 搬到能触达源库的服务器上，那就不是"本地
   PC"方案了。
6. **右侧预取可能白做**：Producer 中途失败时已预取的 B 类结果作废。用
   `--prefetch-depth` 压住浪费面，并把右侧统计写进 Delta（键
   `run_id + pair + window + partition_set_hash`），下一轮同参数 run 直接命中复
   用 —— 注意键里必须含 partition 集合的 hash，否则 Stage 1 判定变了会错误复用。
