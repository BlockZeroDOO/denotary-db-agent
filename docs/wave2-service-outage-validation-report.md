# Wave 2 Local Service-Outage Validation Report

This report captures the current local outage-recovery validation state for the `Wave 2` adapters that already have repeatable local harnesses.

Validated adapters:

- `SQLite`
- `Redis`

Validated outage scenarios:

- `ingress.prepare`
- `watcher.register`
- `receipt.get`
- `audit.get_chain`

## Verified Recovery Pattern

For every passing scenario, the observed recovery contract is:

- baseline `run_once()` processes `0` events
- first attempt returns:
  - `processed = 0`
  - `failed = 1`
- second attempt returns:
  - `processed = 1`
  - `failed = 0`
- proof export succeeds after the temporary outage is removed

## Current Result Summary

`SQLite`

- `ingress_prepare_outage`: passed
- `watcher_register_outage`: passed
- `receipt_fetch_outage`: passed
- `audit_fetch_outage`: passed

`Redis`

- `ingress_prepare_outage`: passed
- `watcher_register_outage`: passed
- `receipt_fetch_outage`: passed
- `audit_fetch_outage`: passed

## Interpretation

This closes the first local service-degradation recovery layer for:

- file-backed edge / embedded `SQLite`
- operational-state `Redis`

Together with restart and short-soak validation, these adapters now have a stronger `Wave 2` readiness story than simple baseline proof export alone.
