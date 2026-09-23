# 计划覆盖对照（2026-09-22，两侧拆分后）

对照 `s3_split_pipeline.md` 与 `s3_split_walkthrough.md`，逐项标注当前 `dtrack-distributed`
包的落地状态。证据来自 `examples/e2e/execution.txt` 的真实运行（`just e2e`）。逐步骤的代码讲解见
`docs/notes/00 索引.md`。

状态：✅ 已实现并经 e2e 验证 · 🟡 部分实现 · ❌ 未实现

## Stage 1 / barrier

| 计划条目 | 状态 | 说明 |
| --- | --- | --- |
| preflight 最先执行，失败则不碰左侧 | 🟡 | 拆分后左右各自 preflight；右侧失败写 `_PREFLIGHT.json`，`col-check` 轮询时看到即中止（左侧计数已发生） |
| 右侧先计数，再左侧计数 | ❌ | 拆分后左侧先写 `row_check/<pair>.left.json`，右侧随后计数对账（ADR 0006） |
| 右侧结果用于裁剪左侧扫描（`date IN (...)`） | ❌ | 左侧只用 fromDate/toDate 边界 |
| 精确 `COUNT(*) GROUP BY` | ✅ | `SqlSource.partition_counts` |
| `date_synonyms` 折叠 | ✅ | `apply_synonyms` |
| 四种 verdict + `stage1/{pair}.json` + `summary` | ✅ | |
| `--gate auto / manual / ratio:x` | ✅ | e2e 步骤 10 验证 ratio:0.02 挡住 orders |
| `--gate manual` 后人工放行（`s3-push --stage2`） | ❌ | manual 只出报告，没有独立放行命令，需改 gate 重跑 stage1 |
| 增量 `manifest/{pair}.json` + `row_check/_CLOSED` | ✅ | |
| `timing` 字段 | ✅ | preflight_s / left_count_s / right_count_s |
| 文件型左侧 `partition_map` | ❌ | csv 只支持单文件 |
| Stage 1 断点续跑（已存在的 stage1/{pair}.json 跳过） | ❌ | 同 run_id 重跑会覆盖 |
| `--log-level` + UTC 时间戳日志 | ✅ | stage1 / producer / consumer / report 四个 logger |
| fixed-N 的 Phase 1.5（key 列表进 manifest） | ✅ | `key_tuples` 写在 window 里，不是独立 keys.json |
| 按左侧来源限流（`source_limits`）、小表优先 | ❌ | 只有全局 `--workers` |
| preflight 检查列/`date_type`/jobs 权限，并入 doctor | 🟡 | 只做 `SELECT … WHERE 1=0` + `DESCRIBE TABLE EXTENDED` |

## S3 协议

| 计划条目 | 状态 | 说明 |
| --- | --- | --- |
| 先写 data 再写 `_INDEX` 指针 | ✅ | 单测 + moto 集成测试 |
| `_INDEX/{seq:08d}-{unit_id}` + `StartAfter` 增量列举 | ✅ | |
| `_HEARTBEAT` / `manifest/_CLOSED{expected_units}` | ✅ | 心跳按 window 提交，不是定时 60s |
| `results/` / `dead/` | ✅ | |
| `_STATUS.json` 状态机、进度、eta、`s3-status` 命令 | 🟡 | 只有 `state` 一个字段 |
| `--upload-raw`（payload A） | ❌ | ADR 0004 明确默认只传统计 |
| SSE-KMS | ✅ | `--kms-key-id` |
| report 桶 `history/{run_id}/` + `latest/pointer.json` + summary.json + index.html | ✅ | `report` 子命令：summary.json、`{pair}_compare_row/col.csv`、`index.html`（移植原 html_export 样式）、`report.xlsx`；`--report-s3` 先写 history/ 最后写 pointer |

## left col-check

| 计划条目 | 状态 | 说明 |
| --- | --- | --- |
| pull → stat → upload 三级重叠 | ✅ | 两个线程池 + 有界队列 |
| 一次扫描拉全窗口，上传时按列切 unit | ✅ | |
| 跨 pair round-robin | ✅ | e2e 日志可见 customers/orders 交替 |
| `--wip` 背压 | ✅ | |
| 断点续跑（跳过已在 `_INDEX` 的 unit） | ✅ | e2e 步骤 9 `published_units: 0` |
| 采样谓词下推到源库（`hash32 < threshold` in SQL） | ❌ | SQL 源拉整窗口后在 pandas 里采样 |
| 流式 `fetchmany` 落盘 | ❌ | `fetchall` |
| Stage 2 大表优先 | ❌ | |

## right col-check

| 计划条目 | 状态 | 说明 |
| --- | --- | --- |
| A 类优先，A 空则做 B（manifest 一到即预取右侧） | ✅ | e2e 步骤 5：left col-check 未启动就完成 3 个窗口的右侧预取 |
| 带洞 partition 的右侧 `WHERE` | ✅ | `contiguous_runs` + `partition_where`，单测 + e2e manifest 核对 |
| `--prefetch-depth` | ✅ | |
| 重试 → `dead/`（含 traceback） | ✅ | |
| 心跳过期 / idle 超时退出 | 🟡 | 直接抛异常退出，没有 PARTIAL 报告和退出码分级 |
| 退出条件 `_CLOSED && results+dead == expected` | ✅ | |
| 本地 checkpoint / Delta checkpoint | ✅ | e2e 用本地；Delta 未验证 |
| 多 worker 线程池 + FAIR pool + 保底 A 槽位 | ❌ | 单线程串行 |
| 右侧统计写 Delta `right_stats` 跨 run 复用 | ❌ | 只在内存 |
| Databricks 侧算统计（Spark） | ✅ | `SparkTarget.window_statistics` 单条 `spark.sql` 在仓内算采样谓词 + 全部列统计，只回一行；本地 pyspark 上与 pandas 逐字段一致（`tests/spark`），真集群待 `tests/live` |
| Delta 表 `run_status / compare_row / compare_col`、告警 | ❌ | HTML/Excel 已由 `report` 提供 |
| F_* / E_* 错误码分类 | ❌ | |

## Kickstart 与部署

| 计划条目 | 状态 | 说明 |
| --- | --- | --- |
| 第一个过 barrier 的 pair 触发 `jobs/run-now`，`idempotency_token=run_id` | ❌ | 拆分后删除：本地不持有 Databricks 凭据，云端 job 由人或 schedule 启动后轮询 `row_check/*.left.json` |
| Databricks 常驻 job 的入口/打包 | ✅ | `src/dtrack_right/notebook.py` + `dtrack_right/`，Repos 同步即可 import，无需 wheel；手动或 scheduled workflow 启动一次，自行退出 |
| 兜底 cron | ❌ | 部署侧 |

## 左侧来源

| 来源 | 状态 |
| --- | --- |
| oracle / athena / hadoop（impyla） / csv | ✅ 同一 `SqlSource` 基类；Oracle 在容器上验证过 preflight / 精确计数 / 带洞拉取（`tests/containers`） |
| 多 Oracle server、多 Databricks workspace/catalog | ✅ `connections` + `conn_macro`，凭据走 `*_env`；左侧文档里只带 catalog/schema/table，不带端点 |
| 右侧 Databricks Volume（`read_files` 当表读 / 上传方预算统计） | 🟡 `SparkTarget` 支持 `kind: volume_raw` 走 `read_files`；`volume_stats` 未做（ADR 0005） |
| 本地零 Databricks 凭据、两套代码分开运行 | ✅ ADR 0006；`tests/test_dependencies.py` 锁定；本地只剩 row-check / col-check / fetch-report / doctor / debug，约 1.7k 行 |
| legacy 的 key 规范化（前导零保留、只折叠纯小数尾零、NFC+lower、`<<NULL>>`）与 md5 前 8 hex 桶 | ✅ 两侧同规则，parity 测试 + `test_key_normalisation_*`；Spark 侧 `_norm_sql` 为其 SQL 孪生 |
| legacy 的 hash16（md5 前 4 hex）列哈希 | 🟡 有意改为 sha256 前 16 hex，两侧都是自家代码不再受 Oracle 表达式限制；左侧下推到 Oracle 时需 `STANDARD_HASH(..., 'SHA256')` |
| 最终报告下载到本地 | ✅ `fetch-report`，按 `latest/pointer.json` 或 `--run-id` |
| 工作区可为 Volume 路径（集群无 AWS key） | ✅ `VolumeStore`，与 `S3Store` 同契约 |
| sas | ❌ 明确推迟 |

## 报告与交付

| 计划条目 | 状态 | 说明 |
| --- | --- | --- |
| `summary.json` / `{pair}_compare_row.csv` / `{pair}_compare_col.csv` | ✅ | `report` 子命令 |
| 自包含 `index.html`（复用 html_export 样式） | ✅ | 移植自 main 分支 `dtrack/html_export.py`，headless Chrome 核对过渲染 |
| Excel | ✅ | `report.xlsx`，沿用 legacy `excel.py` 的 `{pair}_row` / `{pair}_col` 布局 |
| report 桶 `history/{run_id}/` 先写、`latest/pointer.json` 最后写 | ✅ | `publish_report`，单测锁定顺序 |
| `cost_estimate_usd`、per-pair `index.html`、`run_report.html` 总览 | ❌ | 现在是单页总览 + 每 pair CSV |

## 可以改进的地方（按收益排序）

0. **真集群验证 `SparkTarget`**。已在本地 pyspark 会话上逐字段验证（`tests/spark`，`--run-spark`）：采样谓词、计数、`hash_sum`、`n_unique`、min/max、mean/std、strict 全部与 pandas 一致；`OracleSource` 也在 Oracle 容器上验证（`tests/containers`）。首次运行暴露并修掉了两处：整数列 min/max 的 `100003.0`，以及 `7.50` 与 `7.5` 的哈希差异（现在数值列按规范小数文本哈希）。剩下的是在真 Databricks 集群上跑一次 `tests/live`。

1. **左侧采样谓词下推**。`SqlSource.pull_window`（`dtrack_left/sources.py`） 现在 `SELECT` 整个窗口再在 pandas 里按
   `hash32(key) < threshold` 过滤，源库和网络承担的是 100% 的数据量而不是 0.5%。右侧的
   `SparkTarget` 已经下推；把 `_norm_sql` 的思路移植到 Oracle / Hive 方言即可。
2. **流式落盘**。`SqlSource.query` 用 `fetchall`，大窗口会 OOM（原 dtrack 在 Simba 上真踩过）。
   改成 `fetchmany(arraysize)` 逐批 `csv.writer.writerows`，与 `--arraysize` 参数一起补上。
3. **right col-check 并发**。`ColCheck.run` 单线程串行，右侧一个窗口扫描期间无法比对已到的 unit。
   加 `--workers N` 线程池、B 类最多占 `N-1`、Spark FAIR pool，这是 walkthrough 规矩五。
4. **心跳定时化**。left col-check 只在每个窗口提交后 `heartbeat()`，一次左侧拉取超过 180s 就会被
   right col-check 误判为死亡。用后台线程每 60s 覆写 `_HEARTBEAT`，并把 `heartbeat_timeout` 设为
   `3 × interval`。
5. **PARTIAL 收尾而不是抛异常**。心跳过期 / idle 超时时 right col-check 直接 `raise`，已比完的
   `results/` 不会汇总。应改为写 `_STATUS.json{state: partial}`、调用 `report` 出 PARTIAL 报告、
   非零退出。`report` 已经能识别 `results + dead < expected` 并标 PARTIAL，只差 right col-check 调用它。
6. **manual gate 的放行命令**。`--gate manual` 之后没有 `release --run-id --pair` 一类的命令，
   只能改成 auto 重跑 stage1（重新计数）。放行只需读 `stage1/{pair}.json` 再 `build_manifest`。
7. **Stage 1 续跑与右侧裁剪**。`run_row_check` 应跳过已存在的 `stage1/{pair}.json`；左侧计数应带
   `date IN (右侧已有的 partition)` 边界，对 Oracle / Hive 是有效的分区裁剪。
8. **按来源限流**。`--workers` 是全局数字，五个 Oracle pair 会同时占五个会话。把 `source_limits`
   读进 config，按 `left.source` 分组各开一个 `ThreadPoolExecutor`。
9. **preflight 加深**。现在只做 `WHERE 1=0` + `DESCRIBE TABLE EXTENDED`。应校验 `col_map` 的右侧
   列存在、`date_col` 的 `MIN/MAX` 与 `date_type` 对得上、`--job-id` 的 run 权限，并把 `doctor`
   的输出并进 `_PREFLIGHT.json`。
10. **`_STATUS.json` 状态机 + `status` 命令**。三方（stage1 / producer / consumer）各自覆写同一
    个文件，只有一个 `state` 字段。改为 per-pair 状态、`units_total/pushed/compared/dead`、
    `heartbeat_age_s`，再加 `dtrack-distributed status --s3 ... --run-id latest`。
11. **F_* / E_* 错误码**。`dead/` 里只有 `error` 字符串；报告里"发现"和"故障"混在一起。给
    `compare_statistics` 的 diff 打 `F_STAT_DIFF`，给 `_failure` 按异常类型打 `E_RIGHT_SCAN` /
    `E_PULL_FAIL`，`summary.json.errors` 才能直接接告警。
12. **Stage 2 按 pair 增量开始**。拆分后 `col-check` 等 `row_check/_CLOSED`（全部 pair 对账完）才开始推；
    计划要求 per-pair 过 barrier 就推。改为轮询 `manifest/` 逐个接入 `iter_windows_round_robin`。
13. **Delta 落表**。`run_status / compare_row / compare_col` 三张表是 dashboard 的前提；
    `DeltaCheckpoint` 已经证明了写 Delta 的路径，扩成三张表工作量不大。
14. **小处**：`unit_id` 的 slug 会把 `2026-01-01` 折成 `2026-01-01`、列名大小写丢失（靠 digest 保
    唯一）；`right col-check.poll` 每轮对 `manifest/` 全量 `list_json`（N 个 pair 就 N 次 GET）；
    `ready_unclaimed` 每个 unit 提交前 list 三个前缀，大 run 下应缓存 30s（计划 §4.4）。

## 结论

两阶段 barrier、S3 提交协议、left col-check/right col-check 的调度骨架与断点语义已经完整落地并能在本地
闭环。`report` 子命令补上了 summary/CSV/HTML/Excel 与 report 桶。尚未实现的集中在三块：(1) 性能
层——采样谓词下推、流式拉取、多 worker；(2) 交付层——Delta 表、状态机与错误码；(3) 运维层——
Databricks job 打包、manual 放行、Stage 1 续跑与按来源限流。
