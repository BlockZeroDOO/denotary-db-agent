# Wave 2 Local Service-Outage Validation Report

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

This report captures the current local outage-recovery validation state for the `Wave 2` adapters that already have repeatable local harnesses.

Validated adapters:

- `SQLite`
- `Redis`
- `IBM Db2`
- `Apache Cassandra`
- `ScyllaDB`
- `Elasticsearch`

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

`IBM Db2`

- `ingress_prepare_outage`: passed
- `watcher_register_outage`: passed
- `receipt_fetch_outage`: passed
- `audit_fetch_outage`: passed

`Apache Cassandra`

- `ingress_prepare_outage`: passed
- `watcher_register_outage`: passed
- `receipt_fetch_outage`: passed
- `audit_fetch_outage`: passed

`ScyllaDB`

- `ingress_prepare_outage`: passed
- `watcher_register_outage`: passed
- `receipt_fetch_outage`: passed
- `audit_fetch_outage`: passed

`Elasticsearch`

- `ingress_prepare_outage`: passed
- `watcher_register_outage`: passed
- `receipt_fetch_outage`: passed
- `audit_fetch_outage`: passed

Artifact:

- [summary.json](../data/wave2-service-outage-validation-latest/summary.json)

## Interpretation

This closes the first local service-degradation recovery layer for:

- file-backed edge / embedded `SQLite`
- operational-state `Redis`

Together with restart, short-soak, and long-soak validation, the active `Wave 2` adapters now have a substantially stronger readiness story than simple baseline proof export alone.
