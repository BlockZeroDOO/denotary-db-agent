# Wave 2 Long-Soak Validation Report

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

This report captures the current bounded long-soak result for the active `Wave 2` adapters.

Profile:

- `10` cycles
- `5` events per cycle
- `50` expected events per adapter

## Result

All active `Wave 2` adapters passed:

- `SQLite`: `50 processed`, `0 failed`, `50 proofs`, `0 dlq`
- `Redis`: `50 processed`, `0 failed`, `50 proofs`, `0 dlq`
- `ScyllaDB`: `50 processed`, `0 failed`, `50 proofs`, `0 dlq`
- `IBM Db2`: `50 processed`, `0 failed`, `50 proofs`, `0 dlq`
- `Apache Cassandra`: `50 processed`, `0 failed`, `50 proofs`, `0 dlq`
- `Elasticsearch`: `50 processed`, `0 failed`, `50 proofs`, `0 dlq`

Artifact:

- [summary.json](../data/wave2-long-soak-validation-latest/summary.json)

## Interpretation

This report confirms the current bounded long-soak profile for the active `Wave 2` set:

- the adapters now have bounded long-soak confirmation, not only restart and short-soak coverage
- the local sustained-load story is now materially closer to the stronger `Wave 1` baseline
