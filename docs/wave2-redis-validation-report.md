# Wave 2 Redis Validation Report

This report captures the first deeper validation layer for the `Wave 2` `Redis` adapter.

## Validation Summary

- restart recovery: passed
- short-soak validation: passed

## Restart Recovery Result

Profile:

- baseline run before any keys
- first write window: `3` keys
- Redis container restart
- second write window: `3` keys

Observed result:

- `baseline_processed = 0`
- `first_processed = 3`
- `second_processed = 3`
- `delivery_count = 6`
- `proof_count = 6`
- `dlq_count = 0`

Interpretation:

- the adapter resumed correctly after a source restart
- no deliveries were lost
- no DLQ entries were produced

## Short Soak Result

Profile:

- `5` cycles
- `3` keys per cycle
- `15` expected events total

Observed result:

- `baseline_processed = 0`
- `total_processed = 15`
- `total_failed = 0`
- `delivery_count = 15`
- `proof_count = 15`
- `dlq_count = 0`

Interpretation:

- the `Redis` baseline handled bounded sustained load cleanly
- proof export matched processed deliveries
- no DLQ growth occurred

## Status Impact

After this validation, the `Wave 2` `Redis` adapter is no longer just a baseline plus live harness. It now has:

- snapshot / resume baseline
- local full-cycle proof export
- Docker-backed live validation
- restart recovery validation
- bounded short-soak validation
