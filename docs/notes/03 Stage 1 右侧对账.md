---
tags: [dtrack, distributed, stage1, barrier, right]
step: 3
updated: 2026-09-22
---

# 03 Stage 1 右侧对账

上一步：[[02 Stage 1 左侧计数]] · 下一步：[[04 Gate 与 manifest]] · 索引：[[00 索引]]

云端 job（[[12 Databricks 作业]]）启动后一直轮询 `stage1/`。每出现一个还没有 verdict 的
`<pair>.left.json`，就走一遍下面的流程。没有人工介入。

## 找出待处理的 pair

```python
# databricks/dtrack_cloud/stage1_right.py
def pending_left_documents(store):
    for key, document in store.list_json("stage1/"):
        if not key.endswith(".left.json"): continue
        if store.exists(f"stage1/{document['pair']}.json"): continue    # 已有 verdict 就跳过（幂等）
        pending.append(document)
```

## 右侧 preflight 与计数（`spark.sql`）

```python
# databricks/dtrack_cloud/targets.py
class SparkTarget:
    def preflight(self):
        columns = {row[0]: str(row[1]).lower() for row in self._rows(f"DESCRIBE {self.relation()}")}
        if self.config["date_col"] not in columns: raise ValueError(...)
        self.column_types = columns                     # 后面判断 numeric / categorical 用
    def partition_counts(self, from_date=None, to_date=None):
        rows = self._rows(f"SELECT `{date_col}`, COUNT(*) FROM {relation} WHERE {bounds} GROUP BY `{date_col}`")
        return {canonical_date(row[0]): int(row[1]) for row in rows}
```

`relation()` 支持三种右侧形态（ADR 0005）：`catalog.schema.table`、`query` 子查询、
`kind: volume_raw` 时的 `read_files('/Volumes/...', format => 'parquet')`。Spark 标识符用反引号
（`quote_ident(..., "spark")`）。

## 对账：每个 partition 一个 verdict

```python
# databricks/dtrack_cloud/gates.py（与本地 domain/gates.py 逐字节相同，parity 测试锁定）
def reconcile_partitions(left_counts, right_counts, date_synonyms=None):
    left, right = apply_synonyms(left_counts, ...), apply_synonyms(right_counts, ...)
    for partition in sorted(set(left) | set(right)):
        verdict = ("right_only" if l is None else "left_only" if r is None
                   else "match" if int(l) == int(r) else "count_diff")
```

| verdict | 意思 | 进 Stage 2 |
| --- | --- | --- |
| `match` | 两侧记录数相等 | ✓ |
| `count_diff` | 都有但不等 | ✗，本身就是 finding |
| `left_only` | 右侧缺这天 | ✗ |
| `right_only` | 左侧缺这天 | ✗ |

`date_synonyms`（哨兵日期折叠）在对账前应用到两侧，随 left.json 一起传来。

## 落地物

```python
def reconcile_pair(store, left_doc, target, *, gate="auto"):
    health = target.preflight(); right_counts = target.partition_counts(...)
    partitions = reconcile_partitions(left_doc["counts"], right_counts, left_doc.get("date_synonyms"))
    document = {"run_id", "pair", "left", "right", "bounds", "partitions", "summary",
                "gate_decision": evaluate_gate(partitions, gate), "preflight": health,
                "timing": {"preflight_s", "left_count_s", "right_count_s"}}
    store.put_json(f"stage1/{pair}.json", document)                                  # 全部 partition，人看
    store.put_bytes(f"reports/{pair}_compare_row.csv", stage1_csv_bytes(document))   # pair,partition,left,right,delta,verdict
    if decision["passed"]:
        store.put_json(f"manifest/{pair}.json", build_manifest(run_id, pair, left_doc["pair_config"], document))
```

全部 pair 处理完且 `stage1/_LEFT_CLOSED` 存在时写 `stage1/_CLOSED`，本地 `produce` 就是等这个。

## 日志（e2e `logs/05_cloud_job.log`）

```
INFO dtrack.cloud.stage1: right count orders: 58 partitions, 1737 rows (0.043s)
INFO dtrack.cloud.stage1: reconcile orders: {'match': 56, 'count_diff': 1, 'left_only': 1, 'right_only': 1}
INFO dtrack.cloud.stage1:   finding orders 2026-01-15 left=30 right=27
INFO dtrack.cloud.stage1:   finding orders 2026-01-31 left=None right=30
INFO dtrack.cloud.stage1:   finding orders 2026-02-10 left=30 right=None
INFO dtrack.cloud.stage1: gate orders: policy=auto passed=True excluded_ratio=0.0508
INFO dtrack.cloud.stage1: manifest orders: 2 windows x 4 columns = 8 units -> manifest/orders.json
INFO dtrack.cloud.stage1: stage1 closed: 2/2 pairs released
```

这三条 finding 已经是第一个交付物，不用等 Stage 2。
