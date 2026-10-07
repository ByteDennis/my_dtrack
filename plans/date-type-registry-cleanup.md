# Date Type Registry Cleanup

## Scope

- `src/dtrack_left/common/dates.py`
- `src/dtrack_right/common/dates.py`
- side-specific `common/date_platforms.py` adapters and backend preflight probes
- date-type regression tests and user-facing CLI/guide documentation

## Behavior lock

- Preserve calendar-day filtering, half-open native-date ranges, Python/SQL normalization parity, side-specific date types, and portable manifest serialization.
- Replace the old spelling matrix with four user-facing names: `date`, `string`, `number`, and `sas`.

## Cleanup plan

1. Add tests for four canonical types (`date`, `string`, `number`, `sas`), internal detected formats, class registration, and old-name rejection.
2. Replace duplicate built-in registrations with `NativeDateType`, `StringDateType`, `NumericDateType`, and `SasDateType`; reject every old alias instead of silently retaining it.
3. Make `register_date_type(DateType(...))` the primary extension API; do not retain built-in aliases that recreate the old spelling matrix.
4. Keep one semantic date-type interface (`predicate`, `day`, `literal`, `normalize`, `normalize_sql`) and move dialect branching into registered `DateSqlPlatform` adapters. Left carries every source adapter; Right carries Spark only.
5. Resolve `string`/`sas` once during preflight, propagate the concrete spec through row/manifest payloads, update the guides, then run targeted and full verification.

## Fallback inventory

- Unknown string names currently become a generic string date type. Classification: masking fallback slop because a typo silently changes query semantics. Replace with an explicit validation error.
- Existing built-in aliases would preserve the confusing public matrix. Remove them and return an error listing the four supported names.
- Generic SQL normalizer fallback is grounded fail-safe behavior for custom types; retain it with existing cross-dialect tests.

## Non-goals

- No new dependency.
- No change to row/column protocol artifacts or sampling/hash algorithms.
- No automatic guessing from an empty or mixed-format probe sample.
