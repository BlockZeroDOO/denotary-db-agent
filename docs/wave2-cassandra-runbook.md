# Wave 2 Apache Cassandra Runbook

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

This runbook describes the recommended baseline for deploying `denotary-db-agent` against `Apache Cassandra`.

The target model is:

- `Cassandra` remains the distributed wide-column source
- `denotary-db-agent` polls explicitly tracked tables through the `Cassandra` adapter
- notarization proofs are exported locally and anchored through the normal `deNotary` service stack
- checkpoint state survives process restarts in the local `state_db`

## Recommended Fit

Use the `Cassandra` adapter when:

- the business workflow depends on Cassandra-visible operational records
- explicit table selection is acceptable
- bounded snapshot polling is acceptable
- the goal is to prove the currently visible table state rather than reconstruct every low-level mutation

Good examples:

- distributed event ledgers
- IoT or telemetry state tables
- operational mirrors exposed to downstream systems
- scale-out datasets where snapshot proof is sufficient

## Reference Config

Starter config:

- [../deploy/config/cassandra-agent.example.json](../deploy/config/cassandra-agent.example.json)

Reference docs:

- [cassandra-config-reference.md](cassandra-config-reference.md)
- [storage-config-reference.md](storage-config-reference.md)
- [denotary-service-config-reference.md](denotary-service-config-reference.md)
- [denotary-env-file-runbook.md](denotary-env-file-runbook.md)

## Preconditions

Before starting the agent, confirm:

1. the Cassandra contact points are reachable
2. the tracked keyspaces and tables are explicitly defined
3. the chosen watermark field is present and queryable
4. the service user can write the configured `storage.state_db` and `proof_dir`
5. the `deNotary` service stack and signer are already reachable

## Recommended Source Settings

For the current baseline, use:

- `adapter = "cassandra"`
- `capture_mode = "watermark"`
- explicit keyspace/table coverage
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

The current `Cassandra` implementation already confirms:

- live cluster ping
- tracked-table introspection
- local full-cycle proof export
- env-gated live integration harness
- local Docker-backed restart validation
- local Docker-backed short-soak validation
