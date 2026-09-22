---
tags: [dtrack, distributed, stage1, left]
step: 2
updated: 2026-09-22
---

# 02 Stage 1 左侧计数

上一步：[[01 配置与连接档案]] · 下一步：[[03 Stage 1 右侧对账]] · 索引：[[00 索引]]

```bash
dtrack-distributed stage1 --config config.json --s3 s3://work/dtrack/v1/runs/<run_id> [--run-id <id>]
```

本地这一步只做三件事：左侧 preflight、左侧精确计数、把计数和剥掉凭据的 pair 配置交给 S3。
右侧计数、对账、gate、manifest 都在云端（[[03 Stage 1 右侧对账]]）。

## run_id

```python
def make_run_id(config, *, now=None):
    digest = hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()[:6]
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{digest}"      # 20260922T115149Z-a3f9c1
```

时间戳 + 配置哈希：配置改了 run_id 就变。e2e 用 `--run-id` 指定，便于命名日志。

## 左侧 preflight 与计数

```python
# workflows/stage1.py
def run_stage1_left(config, store, *, run_id=None, pair_names=None, source_factory=source_from_config, pair_workers=4):
    store.put_json("_STATUS.json", {"state": "left_counting", "run_id": run_id, "role": "left"})
    sources = {name: source_factory(pair["left"]) for name, pair in pairs.items()}
    health = {name: source.preflight() for name, source in sources.items()}      # SELECT * ... WHERE 1=0
    if failed: raise RuntimeError(f"left preflight failed: {', '.join(failed)}")
    with ThreadPoolExecutor(pair_workers) as pool:                                # 各 pair 并行计数
        counts = {name: pool.submit(count, name).result() ...}
```

生成的 SQL（Oracle，`date_type: date`）：

```sql
SELECT "BUSINESS_DATE" AS dtrack_partition, COUNT(*) AS dtrack_count
FROM "FINANCE"."ORDERS"
WHERE ("BUSINESS_DATE" >= DATE '2026-01-01' AND "BUSINESS_DATE" <= DATE '2026-02-28')
GROUP BY "BUSINESS_DATE"
```

必须是精确 `COUNT(*)`，`NUM_ROWS` 这类统计信息可能过期。CSV 左侧是 pandas `value_counts`。

## 交给 S3 的文档

```python
document = {
    "run_id": run_id, "pair": name,
    "left": public["left"], "right": public["right"],       # 无凭据、无端点
    "bounds": {"from": ..., "to": ...},
    "date_synonyms": {...},
    "counts": {"2026-01-01": 30, "2026-01-02": 30, ...},   # 左侧每天精确行数
    "pair_config": public,                                  # 云端 build_manifest 需要的一切
    "preflight": health[name],
    "timing": {"left_count_s": took},
}
store.put_json(f"stage1/{name}.left.json", document)
store.put_json("stage1/_LEFT_CLOSED", {"pairs": sorted(pairs), "run_id": run_id})
store.heartbeat()
```

`stage1/<pair>.left.json` 就是云端等的**第一个信号**：它一出现，云端就去数右侧。
`_LEFT_CLOSED` 告诉云端"左侧的 pair 就这些"，云端全部对账完才写 `stage1/_CLOSED`。

## 日志（e2e `logs/06_stage1_left.log`）

```
INFO dtrack.stage1: run_id=20260922T115149Z-e2e work=s3://... pairs=['orders', 'customers']
INFO dtrack.stage1: left preflight orders: OK
INFO dtrack.stage1: left count orders: 58 partitions, 1740 rows (0.045s)
INFO dtrack.stage1: -> stage1/orders.left.json
INFO dtrack.stage1: left side closed: 2 pair(s) in 0.31s; waiting on the Right side for verdicts
```

## 与计划的差距

计划让右侧先数、再用右侧的 partition 边界裁剪左侧扫描。拆分后本地不能问右侧，所以顺序反过来，
左侧只用 fromDate/toDate 边界。见 [[plan_coverage|plan_coverage]]。
