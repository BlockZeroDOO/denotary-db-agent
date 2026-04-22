# Wave 2 IBM Db2 Runbook

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

This runbook describes the recommended baseline for deploying `denotary-db-agent` against `IBM Db2`.

The target model is:

- `IBM Db2` remains the enterprise SQL source
- `denotary-db-agent` polls explicitly tracked tables through the `Db2` adapter
- notarization proofs are exported locally and anchored through the normal `deNotary` service stack
- checkpoint state survives process restarts in the local `state_db`

## Recommended Fit

Use the `Db2` adapter when:

- the business workflow depends on Db2-resident operational records
- explicit table selection is acceptable
- bounded snapshot polling is acceptable
- the goal is to prove the current table state exposed through Db2

Good examples:

- ERP ledgers
- regulated operational tables
- reconciliation datasets
- partner-facing extracts sourced from Db2

## Reference Config

Starter config:

- [../deploy/config/db2-agent.example.json](../deploy/config/db2-agent.example.json)

Reference docs:

- [db2-config-reference.md](db2-config-reference.md)
- [storage-config-reference.md](storage-config-reference.md)
- [denotary-service-config-reference.md](denotary-service-config-reference.md)
- [denotary-env-file-runbook.md](denotary-env-file-runbook.md)

## Preconditions

Before starting the agent, confirm:

1. the Db2 host, port, and database are reachable
2. the tracked schemas and tables are explicitly defined
3. the chosen watermark field is present and stable
4. the service user can write the configured `storage.state_db` and `proof_dir`
5. the `deNotary` service stack and signer are already reachable

## Recommended Source Settings

For the current baseline, use:

- `adapter = "db2"`
- `capture_mode = "watermark"`
- explicit schema/table coverage
- bounded `row_limit`

## First-Time Startup

Recommended order:

1. `doctor`
2. `bootstrap`
3. `inspect`
4. one controlled `run --once`
5. verify proof export
6. start daemon mode

## Validation Status

The current `Db2` implementation already confirms:

- live ping
- tracked-table introspection
- local full-cycle proof export
- env-gated live integration harness
- local Docker-backed restart validation
- local Docker-backed short-soak validation
