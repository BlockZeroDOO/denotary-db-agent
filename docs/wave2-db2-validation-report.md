# Wave 2 IBM Db2 Validation Report

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

This report captures the first deeper local validation layer for the `Wave 2` `IBM Db2` adapter.

## Validation Summary

- cold restart recovery: passed
- short-soak validation: passed

## Cold Restart Recovery Result

Profile:

- baseline run before any rows
- first write window: `3` rows
- first agent process closes
- second agent process reopens against the same `state_db`
- second write window: `3` rows

Observed result:

- `baseline_processed = 0`
- `first_processed = 3`
- `second_processed = 3`
- `delivery_count = 6`
- `proof_count = 6`
- `dlq_count = 0`

Interpretation:

- checkpoint continuity works across a cold agent restart
- no already exported rows were replayed incorrectly
- no DLQ entries were produced

## Short Soak Result

Profile:

- `5` cycles
- `3` rows per cycle
- `15` expected events total

Observed result:

- `baseline_processed = 0`
- `total_processed = 15`
- `total_failed = 0`
- `delivery_count = 15`
- `proof_count = 15`
- `dlq_count = 0`

Interpretation:

- the `Db2` baseline handled bounded sustained local writes cleanly
- proof export matched processed deliveries
- no DLQ growth occurred

## Status Impact

After this validation, the `Wave 2` `IBM Db2` adapter now has:

- snapshot / resume baseline
- local full-cycle proof export
- local Docker live validation
- cold restart recovery validation
- bounded short-soak validation
