# ADR 0004: Publish statistics instead of raw extracts by default

## Status

Accepted

## Context

Raw source rows increase transfer cost and data exposure while the normal reconciliation only needs
per-column statistics.

## Decision

The Producer publishes exact statistics by Unit. Raw extracts remain local scratch files and are not
part of the default S3 protocol.

## Consequences

The protocol is smaller and safer. A future raw-data diagnostic mode must be explicit, access
controlled, and separately retained.
