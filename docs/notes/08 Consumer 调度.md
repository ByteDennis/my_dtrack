---
tags: [dtrack, distributed, stage2, consumer, scheduler, right]
step: 8
updated: 2026-09-22
---

# 08 Consumer 调度

上一步：[[07 采样与统计]] · 下一步：[[09 比对规则]] · 索引：[[00 索引]]

云端 job 对账完（[[03 Stage 1 右侧对账]]）就进入排空循环。代码在 `dtrack_cloud/scheduler.py` 和
`dtrack_cloud/consumer.py`，由 `dtrack_cloud/app.py` 串起来（[[12 Databricks 作业]]）。

## 永不空转：A 类优先，A 空则做 B

| 类 | 叫什么 | 要等什么 | 供给 |
| --- | --- | --- | --- |
| A | 比对 | 左侧 unit 到了 **且** 该 window 的右侧统计算好了 | 受 Producer 限速 |
| B | 抓右侧 + 算统计（一条 spark.sql） | 只要 manifest | pair 一过 barrier 就全量可见 |

```python
class DrainScheduler:
    def add_manifest(self, manifest):       # 新 pair -> 每个 window 进 B backlog
    def add_unit(self, pointer):            # 新指针 -> 右侧已就绪就进 ready，否则挂 pending
    def mark_right_ready(self, pair, window, stats):   # B 做完 -> 该 window 的 pending 全部升到 ready
    def next_task(self):
        if self.ready: return DrainTask("compare", ...)                          # A 优先
        key = 左侧已到的 window 优先，否则 backlog 队首；投机预取不超过 prefetch_depth
        return DrainTask("prefetch", ...)
```

e2e 里云端在本地 `produce` 开始前就把两个 orders 窗口的右侧统计算好了：

```
manifest orders loaded: 2 windows x 4 columns = 8 units -> B-queue seeded
B fetch_right orders 2026-01-01: 29 partition(s)
B right stats ready orders 2026-01-01 (0.094s) -> 0 unit(s) promoted to A-queue
...
poll: 7 new object(s), cursor=_INDEX/00000006-orders--2026-01-01--region--3f3069d1c3b4.json
A compare orders 2026-01-01 AMOUNT -> MATCH
```

## 主循环

```python
# dtrack_cloud/consumer.py
def run(self):
    while True:
        if self.poll(): last_progress = now                 # 增量读 manifest/ 与 _INDEX/(StartAfter=cursor)
        task = None if not self.scheduler.ready and self.complete() else self.scheduler.next_task()
        if task: self.run_task(task); continue
        if self.complete(): return self.results             # _CLOSED 存在 且 results+dead >= expected_units
        if self.on_idle and self.on_idle(): continue        # 空闲时回头看有没有新的 left.json 要对账
        if heartbeat 过期: raise RuntimeError
        if idle 超时:      raise TimeoutError
        time.sleep(self.poll_interval)
```

`on_idle` 就是 `run_stage1_right`：所以对账和排空是同一个循环，pair 晚到也能接上。

## fixed-N 要等 key 文件

```python
def poll(self):
    for key, manifest in self.store.list_json("manifest/"):
        if manifest["sample"]["mode"] == "fixed":
            if not self.store.exists(f"manifest/{pair}.keys.json"): continue     # 本地还没选 key，先不预取
            for window in manifest["windows"]: window["key_tuples"] = keys[window["bucket"]]
        self.scheduler.add_manifest(manifest)
```

`tests/cloud/test_cloud.py::test_consumer_waits_for_fixed_sample_keys_before_prefetching` 锁定。

## 右侧抓取（B 类）

```python
def run_task(self, task):
    if task.kind == "prefetch":
        stats = self._target(manifest).window_statistics(manifest, task.payload)   # SparkTarget 或 CsvAnchorTarget
        self.scheduler.mark_right_ready(pair, window, {left: stats[right] for left, right in col_map.items()})
```

右侧统计是 **window 级**的：一条查询喂饱该 window 的全部列。

## 重试与死信

失败进 `attempts`，指数回队；超过 `max_attempts` 写 `dead/{unit_id}.json`（含 traceback），
prefetch 失败则该 window 全部列进 dead。死信计入完成数，不阻塞退出。

## 断点续跑

启动时 `self.seen = results/ ∪ dead/` 里的 unit_id，`poll()` 直接跳过；且一旦 `complete()` 就不再
投机预取。e2e 第二次云端 job 返回 `results: 0`、没有任何右侧扫描。不再需要单独的 checkpoint 文件。

## 已知差距

单线程；心跳过期直接抛异常而不出 PARTIAL 报告。见 [[plan_coverage|plan_coverage]] 改进项 3、5。
