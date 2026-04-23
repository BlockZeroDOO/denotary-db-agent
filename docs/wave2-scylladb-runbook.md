# Wave 2 ScyllaDB Runbook

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

This runbook describes the current release-oriented baseline for deploying
`denotary-db-agent` against `ScyllaDB`.

The supported source model today is:

- `ScyllaDB` remains the distributed wide-column source
- `denotary-db-agent` polls explicitly tracked tables through the `scylladb`
  adapter
- notarization proofs are exported locally and anchored through the normal
  `deNotary` service stack
- checkpoint state survives process restarts in the local `state_db`

The current `scylladb` adapter reuses the Cassandra-compatible watermark
baseline. Native Scylla-specific CDC is not part of the current baseline.

## Recommended Fit

Use the `ScyllaDB` adapter when:

- the business workflow depends on Scylla-visible operational records
- explicit table selection is acceptable
- bounded snapshot polling is acceptable
- the goal is to prove the currently visible table state rather than reconstruct
  every low-level mutation

Good examples:

- high-throughput operational ledgers
- distributed state tables
- telemetry or event-oriented application mirrors
- scale-out workloads where snapshot proof is sufficient

## Reference Config

Starter config:

- [deploy/config/scylladb-agent.example.json](../deploy/config/scylladb-agent.example.json)

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

## Recommended Layout

Example Linux layout:

- Scylla contact points: `10.10.20.11:9042`, `10.10.20.12:9042`
- agent state: `/var/lib/denotary-db-agent/scylladb-agent-state.sqlite3`
- proof export directory: `/var/lib/denotary-db-agent/proofs`
- env-file secret: `/etc/denotary-db-agent/agent.secrets.env`

## Recommended Source Settings

For the current baseline, use:

- `adapter = "scylladb"`
- `capture_mode = "watermark"`
- explicit keyspace/table coverage
- explicit watermark and commit timestamp columns
- bounded `row_limit`

Typical source section:

```json
{
  "id": "scylladb-core-ledger",
  "adapter": "scylladb",
  "enabled": true,
  "source_instance": "cluster-eu-1",
  "database_name": "ledger",
  "include": {
    "ledger": ["invoices", "payments"]
  },
  "checkpoint_policy": "after_ack",
  "backfill_mode": "full",
  "connection": {
    "hosts": ["10.10.20.11", "10.10.20.12"],
    "port": 9042,
    "username": "scylla",
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
python -m denotary_db_agent --config /etc/denotary-db-agent/scylladb-agent.json doctor --source scylladb-core-ledger --strict
python -m denotary_db_agent --config /etc/denotary-db-agent/scylladb-agent.json bootstrap --source scylladb-core-ledger
python -m denotary_db_agent --config /etc/denotary-db-agent/scylladb-agent.json inspect --source scylladb-core-ledger
python -m denotary_db_agent --config /etc/denotary-db-agent/scylladb-agent.json run --once
python -m denotary_db_agent --config /etc/denotary-db-agent/scylladb-agent.json run --interval-sec 15
```

## What `inspect` Should Show

Healthy baseline should include:

- `adapter = "scylladb"`
- `capture_mode = "watermark"`
- tracked tables under `tracked_tables`
- configured `watermark_column`
- configured or discovered primary key columns
- `cdc.runtime.transport = "polling"`

## Validation Status

The current `ScyllaDB` validation already confirms:

- Docker-backed live baseline validation
- local full-cycle proof export
- restart recovery validation
- short-soak validation
- bounded long-soak validation
- local service-outage recovery validation
- real `denotary` mainnet happy-path validation
- bounded mainnet budget validation
- real mainnet degraded-service recovery validation

Reference:

- [wave2-scylladb-validation.md](wave2-scylladb-validation.md)
- [wave2-scylladb-validation-report.md](wave2-scylladb-validation-report.md)
- [wave2-readiness-matrix.md](wave2-readiness-matrix.md)

## Operational Notes

- keep tracked tables explicit and small enough for bounded polling
- treat the current adapter as proof of visible operational state, not as
  native mutation-stream capture
- keep watermark and primary key columns stable across the tracked tables
- validate cluster auth, replication, and consistency settings separately from
  the adapter baseline before production rollout

## Current Limits

The current baseline does not yet provide:

- native Scylla-specific CDC
- delete tombstone reconstruction after a row disappears between polls
- wildcard table discovery
- a production-scale replacement for native mutation streaming

For the current release posture, prefer:

- explicit keyspace/table coverage
- bounded polling
- regular `doctor`, `inspect`, and `report` checks
- release or rollout evidence based on restart, soak, and degraded-service runs
