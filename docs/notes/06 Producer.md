---
tags: [dtrack, distributed, stage2, producer, left]
step: 6
updated: 2026-09-22
---

# 06 Producer

上一步：[[05 S3 提交协议]] · 下一步：[[07 采样与统计]] · 索引：[[00 索引]]

```bash
dtrack-distributed produce --s3 s3://work/.../runs/<run_id> [--pull-workers 2 --stat-workers 2 --wip 200 --wait-timeout 1800]
```

本地的 Stage 2：等云端放行 → （fixed-N 先选 key）→ 拉左侧 → 采样 → 算统计 → 按列切 unit 提交。

## 先等右侧关门

```python
# workflows/producer.py
def wait_for_manifests(store, *, timeout=1800, poll_interval=3, pair_names=None):
    while True:
        if store.exists("stage1/_CLOSED"):
            return [m for key, m in store.list_json("manifest/") if key != "manifest/_CLOSED" and not key.endswith(".keys.json")]
        state = store.get_json("_STATUS.json")                       # 变化时打印云端进度
        if store.exists("_PREFLIGHT.json") and not ok: raise RuntimeError("Right side preflight failed")
        if elapsed > timeout: raise TimeoutError("Right side did not close Stage 1 before timeout")
        time.sleep(poll_interval)
```

e2e 日志里就是一行等待、一行放行：

```
INFO dtrack.producer: waiting for the Right side: status={'role': 'left', 'run_id': ..., 'state': 'left_closed'}
INFO dtrack.producer: stage1/_CLOSED seen: 2 manifest(s) released
```

## fixed-N：本地选 key，共享给云端

```python
def publish_fixed_keys(store, manifest, work_dir):
    if manifest["sample"]["mode"] != "fixed" or store.exists(f"manifest/{pair}.keys.json"): return
    for window in manifest["windows"]:
        path = source.pull_window(window["partitions"], [], output, key_columns=manifest["key_cols"])   # 只拉 key 列
        sampled = sample_csv(path, key_columns=..., mode="fixed", count=..., families=...)
        windows[window["bucket"]] = pd.read_csv(sampled)[key_cols].drop_duplicates().values.tolist()
    store.put_json(f"manifest/{pair}.keys.json", {"pair": pair, "windows": windows})
```

fraction 模式不需要这一步，两侧各自按 hash 选。

## 出单顺序：跨 pair 轮转

```python
def iter_windows_round_robin(manifests):
    for windows in itertools.zip_longest(*(m["windows"] for m in manifests)):
        for manifest, window in zip(manifests, windows):
            if window is not None: yield manifest, window
# (customers, 2026-01) (orders, 2026-01) (orders, 2026-02) ...
```

## 三级重叠流水线（auto queue 的本地那一半）

```python
class ProducerPipeline:
    def run(self, manifests):
        existing = {row["unit_id"] for _, row in self.store.list_json("_INDEX/")}     # 断点续跑
        with ThreadPoolExecutor(self.pull_workers) as pulls, ThreadPoolExecutor(self.stat_workers) as stats:
            while pull_jobs or stat_jobs or ready or not exhausted:
                while not exhausted and len(pull_jobs) < self.pull_workers:      # 装满 pull 池（源库并发）
                    manifest, window = next(plans); pull_jobs[pulls.submit(self.pull_window, manifest, window)] = ...
                while ready and len(stat_jobs) < self.stat_workers:               # 装满 stat 池（本地 CPU）
                    stat_jobs[stats.submit(self.compute_stats, ...)] = ...
                complete, _ = wait(active, return_when=FIRST_COMPLETED)           # 谁先完成处理谁
                for job in complete:
                    if job in pull_jobs: ready.append(...); continue
                    for column in manifest["columns"]:
                        uid = unit_id(pair, bucket, column)
                        if uid in existing: continue
                        while self.store.ready_unclaimed() > self.wip: time.sleep(...)   # 背压上限
                        self.store.commit_unit(sequence, uid, rows[column], {...}); sequence += 1
                    self.store.heartbeat()
```

- **扫描按 window，交付按 unit**：一个 window 一条 SQL 拉全部列，上传时才按列切成 KB 级 JSON。
- **`--wip`** 不是限速是上限：S3 上未处理的 unit 超过 200 就停 1 秒再看。踩到说明云端是瓶颈。
- **续跑**：已在 `_INDEX/` 的 unit 跳过；e2e 第二次 `produce` 输出 `published_units: 0`。

## 收摊

```python
if indexed >= expected: self.store.close_manifests(list(published), expected)     # manifest/_CLOSED
```

## 带洞的 WHERE

```python
# engine/sql.py
def partition_where(date_col, partitions, *, dialect, date_type=None):
    for start, end in contiguous_runs(partitions):          # [('2026-01-01','2026-01-14'), ('2026-01-16','2026-01-30')]
        clauses.append(f"{col} = {lo}" if start == end else f"{col} BETWEEN {lo} AND {hi}")
```

写成 `BETWEEN 月初 AND 月末` 就把 Stage 1 排除的坏数据又吃回来了。云端的 `SparkTarget` 用同一份
partition 列表和同一个函数（parity 测试）。

## 已知差距

左侧仍是拉整窗口再本地采样、`fetchall`、心跳不定时、等 `_CLOSED` 而不是按 pair 增量开始。见
[[plan_coverage|plan_coverage]]。
