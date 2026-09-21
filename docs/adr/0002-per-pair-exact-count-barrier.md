# ADR 0002: Use a per-Pair exact-count Barrier

## Status

Accepted

## Context

Column statistics cannot explain a row-population mismatch, and one Pair must not prevent unrelated
Pairs from progressing.

## Decision

Stage 1 uses exact `COUNT(*)` grouped by canonical Partition. Each Pair independently admits only
Partitions with equal counts on both sides. Approximate distinct counts are never emitted.

## Consequences

Count mismatches remain visible findings in Stage 1 and cannot contaminate Stage 2 statistics.
Holes in admitted dates are represented as explicit Partition lists, never collapsed to ranges.
