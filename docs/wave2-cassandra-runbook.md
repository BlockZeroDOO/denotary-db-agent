# Wave 2 Apache Cassandra Runbook

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

This runbook describes the current release-oriented baseline for deploying
`denotary-db-agent` against `Apache Cassandra`.

The supported source model today is:

- `Cassandra` remains the distributed wide-column source
- `denotary-db-agent` polls explicitly tracked tables through the `cassandra`
  adapter
- notarization proofs are exported locally and anchored through the normal
  `deNotary` service stack
- checkpoint state survives process restarts in the local `state_db`

Native Cassandra CDC is not part of the current baseline.

## Security Baseline

For enterprise production use:

- keep `Ingress`, `Watcher`, `Receipt`, and `Audit` in the same trusted
  deployment boundary as the agent
- use a dedicated hot permission such as `dnanchor`
- do not use `owner` or `active` as the runtime signer permission
- keep the hot key in `env_file` or a secret mount
- treat `state_db`, `proof_dir`, and saved evidence snapshots as sensitive
  local artifacts

Reference:

- [security-baseline.md](security-baseline.md)
- [denotary-env-file-runbook.md](denotary-env-file-runbook.md)

## Recommended Fit

Use the `Cassandra` adapter when:

- the business workflow depends on Cassandra-visible operational records
- explicit table selection is acceptable
- bounded snapshot polling is acceptable
- the goal is to prove the currently visible table state rather than reconstruct
  every low-level mutation

Good examples:

- distributed event ledgers
- IoT or telemetry state tables
- operational mirrors exposed to downstream systems
- scale-out datasets where snapshot proof is sufficient

## Reference Config

Starter config:

- [deploy/config/cassandra-agent.example.json](../deploy/config/cassandra-agent.example.json)

Reference docs:

- [cassandra-config-reference.md](cassandra-config-reference.md)
- [security-baseline.md](security-baseline.md)
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

## Recommended Layout

Example Linux layout:

- Cassandra contact points: `10.20.30.11:9042`, `10.20.30.12:9042`
- agent state: `/var/lib/denotary-db-agent/cassandra-agent-state.sqlite3`
- proof export directory: `/var/lib/denotary-db-agent/proofs`
- env-file secret: `/etc/denotary-db-agent/agent.secrets.env`

## Recommended Source Settings

For the current baseline, use:

- `adapter = "cassandra"`
- `capture_mode = "watermark"`
- explicit keyspace/table coverage
- explicit watermark and commit timestamp columns
- bounded `row_limit`

Typical source section:

```json
{
  "id": "cassandra-core-ledger",
  "adapter": "cassandra",
  "enabled": true,
  "source_instance": "events-eu-1",
  "database_name": "ledger",
  "include": {
    "ledger": ["invoices", "payments"]
  },
  "checkpoint_policy": "after_ack",
  "backfill_mode": "full",
  "connection": {
    "hosts": ["10.20.30.11", "10.20.30.12"],
    "port": 9042,
    "username": "cassandra",
    "password": "replace-me"
  },
  "options": {
    "capture_mode": "watermark",
    "watermark_column": "updated_at",
    "commit_timestamp_column": "updated_at",
    "primary_key_columns": ["id"],
    "row_limit": 250
  }
}
```

## First-Time Startup

Recommended order:

1. `doctor`
2. `bootstrap`
3. `inspect`
4. one controlled `run --once`
5. verify proof export
6. start daemon mode

Example:

```bash
python -m denotary_db_agent --config /etc/denotary-db-agent/cassandra-agent.json doctor --source cassandra-core-ledger --strict
python -m denotary_db_agent --config /etc/denotary-db-agent/cassandra-agent.json bootstrap --source cassandra-core-ledger
python -m denotary_db_agent --config /etc/denotary-db-agent/cassandra-agent.json inspect --source cassandra-core-ledger
python -m denotary_db_agent --config /etc/denotary-db-agent/cassandra-agent.json run --once
python -m denotary_db_agent --config /etc/denotary-db-agent/cassandra-agent.json run --interval-sec 15
```

## What `inspect` Should Show

Healthy baseline should include:

- `adapter = "cassandra"`
- `capture_mode = "watermark"`
- tracked tables under `tracked_tables`
- configured `watermark_column`
- configured or discovered primary key columns
- `cdc.runtime.transport = "polling"`

## Validation Status

The current `Cassandra` validation already confirms:

- env-gated live baseline validation
- local Docker-backed validation
- local full-cycle proof export
- restart recovery validation
- short-soak validation
- bounded long-soak validation
- local service-outage recovery validation
- real `denotary` mainnet happy-path validation
- bounded mainnet budget validation
- real mainnet degraded-service recovery validation

Reference:

- [wave2-cassandra-validation.md](wave2-cassandra-validation.md)
- [wave2-cassandra-validation-report.md](wave2-cassandra-validation-report.md)
- [wave2-mainnet-budget-validation-report.md](wave2-mainnet-budget-validation-report.md)
- [wave2-mainnet-service-outage-validation-report.md](wave2-mainnet-service-outage-validation-report.md)
- [wave2-readiness-matrix.md](wave2-readiness-matrix.md)

## Operational Notes

- keep tracked tables explicit and small enough for bounded polling
- treat the current adapter as proof of visible operational state, not as
  native mutation-stream capture
- keep watermark and primary key columns stable across the tracked tables
- validate cluster auth, replication, and consistency settings separately from
  the adapter baseline before production rollout

## Current Limits

The current baseline does not provide:

- native Cassandra CDC integration
- delete tombstone reconstruction after a row disappears between polls
- wildcard table discovery
- a production-scale replacement for native mutation streaming

For the current release posture, prefer:

- explicit keyspace/table coverage
- bounded polling
- regular `doctor`, `inspect`, and `report` checks
- release or rollout evidence based on restart, soak, and degraded-service runs
