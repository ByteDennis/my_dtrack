# ADR 0001: Deliver the distributed workflow as a standalone package

## Status

Accepted

## Context

The implementation was derived from a larger repository whose CLI, UI, legacy backends, and tests
are not runtime requirements of the distributed workflow. Keeping those files on this branch
would obscure ownership and make every release inherit unrelated changes.

## Decision

The branch root is a standalone `dtrack-distributed` Python package. Existing `ccsv` and `dtrack`
code are references, not vendored dependencies. Shared behavior needed at runtime is implemented
inside this package and locked by focused tests.

## Consequences

The branch is independently installable and reviewable. Changes from the reference implementation
must be ported deliberately, and its compatibility tests must be extended when behavior is adopted.
