# Wave 2 ScyllaDB Validation Report

Validation date:

- `2026-04-20`

Execution model:

- local Docker-backed `ScyllaDB`
- adapter: `scylladb`
- checkpoint mode: watermark polling
- proof export: enabled

## Restart Result

- status: `passed`
- `baseline_processed = 0`
- `first_processed = 3`
- `second_processed = 3`
- `delivery_count = 6`
- `proof_count = 6`
- `dlq_count = 0`

## Short-Soak Result

- status: `passed`
- `cycles = 5`
- `events_per_cycle = 3`
- `total_processed = 15`
- `total_failed = 0`
- `delivery_count = 15`
- `proof_count = 15`
- `dlq_count = 0`

## Current Readiness Impact

This validation confirms that the current `Wave 2` `ScyllaDB` adapter now covers:

- snapshot baseline
- local full-cycle proof export
- Docker-backed live validation
- cold restart recovery
- bounded short-soak validation

Open layers remain:

- mainnet `denotary` validation
- bounded budget validation
- degraded-service validation
