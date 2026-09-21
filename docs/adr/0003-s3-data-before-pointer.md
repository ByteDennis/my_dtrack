# ADR 0003: Commit S3 Units data before their Ready pointers

## Status

Accepted

## Context

S3 object creation is atomic per key but there is no transaction spanning a payload and its queue
record.

## Decision

The Producer writes `units/<id>.json` first and `_INDEX/<sequence>-<id>.json` second. Consumers only
discover Units through `_INDEX/` and resume using lexicographic `StartAfter`.

## Consequences

A visible pointer always names an existing payload. Orphan payloads are harmless and can be removed
after the run; an orphan pointer is a protocol violation covered by tests.
