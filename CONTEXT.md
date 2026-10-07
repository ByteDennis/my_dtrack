# Distributed Reconciliation

This context establishes whether two representations of the same business dataset agree while keeping data findings distinct from execution failures.

## Language

**Pair**:
Two dataset representations expected to agree and evaluated together.
_Avoid_: Job, table comparison

**Left side**:
The source representation read by the Producer.
_Avoid_: Primary, local table

**Right side**:
The Databricks representation used for distributed reconciliation.
_Avoid_: Target, cloud copy

**Partition**:
The canonical date-valued reconciliation identity on which exact row-count equality is decided.
_Avoid_: Bucket, Window

**Date type**:
The configured family of a side's partition-column encoding: date, string, number, or SAS. A probe resolves string and SAS families to one concrete format before reconciliation.
_Avoid_: Timestamp type, date format name

**Canonical day**:
The ISO calendar date to which side-specific date encodings are normalized before partition comparison or date-key hashing.
_Avoid_: Raw date, timestamp

**Window**:
A collection of admitted Partitions scanned together for sampled column statistics.
_Avoid_: Partition, batch

**Unit**:
The smallest independently committed and completed Stage 2 result, identified by Pair, Window, and column.
_Avoid_: File, task

**Finding**:
A verified disagreement between the two sides.
_Avoid_: Error, failure

**Failure**:
An inability to establish a result because execution, connectivity, permissions, or protocol handling failed.
_Avoid_: Finding, mismatch

**Barrier**:
The per-Pair rule that admits only count-matching Partitions to Stage 2.
_Avoid_: Global gate, pause

**Ready pointer**:
The commit record that makes a fully written Unit visible to the Consumer.
_Avoid_: Unit, lock file

**Work store**:
The short-lived S3 namespace used for protocol state and intermediate artifacts.
_Avoid_: Report store, archive

**Report store**:
The long-lived destination for finalized reconciliation evidence.
_Avoid_: Work store, queue
