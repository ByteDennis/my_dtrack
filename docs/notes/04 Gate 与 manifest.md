---
tags: [dtrack, distributed, stage1, barrier, manifest]
step: 4
updated: 2026-09-22
---

# 04 Gate 与 manifest

上一步：[[03 Stage 1 右侧对账]] · 下一步：[[05 S3 提交协议]] · 索引：[[00 索引]]

这两步都在云端（`dtrack_cloud/gates.py`、`dtrack_cloud/manifest.py`），代码与本地
`domain/` 逐字节相同，`tests/cloud/test_parity.py` 保证两份不会漂移。

## gate：这个 pair 放不放行

```python
def evaluate_gate(partitions, policy="auto"):
    matched = sum(row["verdict"] == "match" for row in partitions.values())
    excluded_ratio = (total - matched) / total
    if policy == "manual":          return {..., "passed": False, "reason": "manual_review"}
    if policy == "auto":            return {..., "passed": matched > 0, "reason": "auto"}
    if policy.startswith("ratio:"): return {..., "passed": matched > 0 and excluded_ratio <= threshold}
```

| notebook 的 `gate` widget | 行为 | e2e 结果 |
| --- | --- | --- |
| `auto` | 按天过滤，有 match 就放行，从不阻塞 | orders / customers 都放行 |
| `manual` | 只出 stage1 报告，不写 manifest | manifests written=0 |
| `ratio:0.02` | 排除比例 > 2% 的 pair 整个挡下 | orders 5.08% 被挡，customers 放行 |

gate 是 **per-pair** 的：一个坏表不影响别的表。

## manifest：Stage 2 的执行计划

```python
def build_manifest(run_id, pair_name, pair, stage1):          # pair = left.json 里的 pair_config
    columns = [c for c in pair["col_map"] if c.lower() != date_col]
    for partition, row in sorted(stage1["partitions"].items()):
        if row["verdict"] == "match":                                       # ★ 只有过关的 partition
            grouped[bucket_date(partition, vintage)].append(partition)
    windows = [{"bucket": b, "partitions": ps, "n_left": ...} for b, ps in sorted(grouped.items())]
    return {"protocol_version": 1, "run_id", "pair", "sample": sample_plan(pair.get("sample")),
            "vintage", "key_cols", "right_key_cols", "key_families", "columns", "col_map",
            "windows", "left", "right", "strict", "expected_units": len(windows) * len(columns)}
```

e2e 里 orders 的 `windows`：

```json
[{"bucket": "2026-01-01", "partitions": ["2026-01-01", ..., "2026-01-14", "2026-01-16", ..., "2026-01-30"], "n_left": 870},
 {"bucket": "2026-02-01", "partitions": ["2026-02-01", ..., "2026-02-09", "2026-02-11", ..., "2026-02-28"], "n_left": 810}]
```

`01-15`、`01-31`、`02-10` 三个洞被挖掉了。`validate_manifest` 强制 partitions 有序唯一、
`expected_units == windows × columns`。`stage1/{pair}.json`（全部 partition）和
`manifest/{pair}.json`（只含过关的）是两份东西。

## 采样计划

```python
def sample_plan(raw):
    if raw in (None, "", False): return {"mode": "all"}
    if isinstance(raw, float):   return {"mode": "fraction", "rate": raw, "threshold": round(raw * 2**32)}
    return {"mode": "fixed", "count": int(raw)}
```

`fraction` 零协调：两侧各自算 `hash32(key) < threshold`。`fixed` 需要一侧先选 key。拆分后由**本地**
在 `produce` 开头选（[[06 Producer]] 的 `publish_fixed_keys`），写 `manifest/<pair>.keys.json`：

```json
{"pair": "customers", "windows": {"2026-01-01": [["5007"], ["5019"], ...]}}
```

云端的 Consumer 对 fixed 模式的 pair 会等这个文件出现再预取（[[08 Consumer 调度]]）。

## 谁点火

拆分后没有 `jobs/run-now` 点火：本地不持有 Databricks 凭据。云端 job 由人或 schedule 启动一次，
自己轮询 `stage1/*.left.json`（[[12 Databricks 作业]]）。
