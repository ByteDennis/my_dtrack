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
The smallest independently published and completed Stage 2 comparison, identified by Pair, Window, and column. Its publication includes the Left statistics and makes the Unit ready for the Right side.
_Avoid_: File, task

**Pair plan**:
The immutable Stage 2 contract for one Pair: admitted Windows, mapped columns, sampling, normalization, and expected Unit count.
_Avoid_: Manifest, status

**Unit state snapshot**:
A derived, human-readable view of every planned Unit's current lifecycle state. Protocol objects remain authoritative.
_Avoid_: Queue, manifest

**Finding**:
A verified disagreement between the two sides.
_Avoid_: Error, failure

**Failure**:
An inability to establish a result because execution, connectivity, permissions, or protocol handling failed.
_Avoid_: Finding, mismatch

**Barrier**:
The per-Pair rule that admits only count-matching Partitions to Stage 2.
_Avoid_: Global gate, pause

**Run status**:
The replaceable observation of current run ownership and progress, interpreted in the timezone frozen when the run starts.
_Avoid_: Pair plan, audit

**Audit event**:
Immutable evidence of one representative submitted SQL operation. Pair identity belongs to its path rather than its visible SQL header.
_Avoid_: Status, log line

**Work store**:
The short-lived S3 namespace used for protocol state and intermediate artifacts.
_Avoid_: Report store, archive

**Report store**:
The long-lived destination for finalized reconciliation evidence.
_Avoid_: Work store, queue
