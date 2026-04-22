# Wave 2 ScyllaDB Runbook

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

This runbook describes the recommended baseline for deploying `denotary-db-agent` against `ScyllaDB`.

The target model is:

- `ScyllaDB` remains the distributed wide-column source
- `denotary-db-agent` polls explicitly tracked tables through the `scylladb` adapter
- notarization proofs are exported locally and anchored through the normal `deNotary` service stack
- checkpoint state survives process restarts in the local `state_db`

## Recommended Fit

Use the `ScyllaDB` adapter when:

- the business workflow depends on Scylla-visible operational records
- explicit table selection is acceptable
- bounded snapshot polling is acceptable
- the goal is to prove the currently visible table state rather than reconstruct every low-level mutation

Good examples:

- high-throughput operational ledgers
- distributed state tables
- telemetry or event-oriented application mirrors
- scale-out workloads where snapshot proof is sufficient

## Reference Config

Starter config:

- [../deploy/config/scylladb-agent.example.json](../deploy/config/scylladb-agent.example.json)

Reference docs:

- [scylladb-config-reference.md](scylladb-config-reference.md)
- [storage-config-reference.md](storage-config-reference.md)
- [denotary-service-config-reference.md](denotary-service-config-reference.md)
- [denotary-env-file-runbook.md](denotary-env-file-runbook.md)

## Preconditions

Before starting the agent, confirm:

1. the Scylla contact points are reachable
2. the tracked keyspaces and tables are explicitly defined
3. the chosen watermark field is present and queryable
4. the service user can write the configured `storage.state_db` and `proof_dir`
5. the `deNotary` service stack and signer are already reachable

## Recommended Source Settings

For the current baseline, use:

- `adapter = "scylladb"`
- `capture_mode = "watermark"`
- explicit keyspace/table coverage
- bounded `row_limit`

## Validation Status

The current `ScyllaDB` implementation confirms:

- adapter registration
- Cassandra-compatible cluster validation
- tracked-table introspection baseline
- deterministic checkpoint resume
- local full-cycle proof export
- Docker-backed live integration harness
- restart recovery validation
- short-soak validation
- real `denotary` mainnet happy-path validation

Reference:

- [wave2-scylladb-validation.md](wave2-scylladb-validation.md)
- [wave2-scylladb-validation-report.md](wave2-scylladb-validation-report.md)

Current gap:

- mainnet `denotary` validation still remains future work
