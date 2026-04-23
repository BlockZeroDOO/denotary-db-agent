# Wave 2 IBM Db2 Runbook

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

This runbook describes the current release-oriented baseline for deploying
`denotary-db-agent` against `IBM Db2`.

The supported source model today is:

- `IBM Db2` remains the enterprise SQL source
- `denotary-db-agent` polls explicitly tracked tables through the `db2` adapter
- notarization proofs are exported locally and anchored through the normal
  `deNotary` service stack
- checkpoint state survives process restarts in the local `state_db`

Native Db2 CDC is not part of the current baseline.

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

- [deploy/config/db2-agent.example.json](../deploy/config/db2-agent.example.json)

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

## Recommended Layout

Example Linux layout:

- Db2 endpoint: `db2.example.net:50000`
- agent state: `/var/lib/denotary-db-agent/db2-agent-state.sqlite3`
- proof export directory: `/var/lib/denotary-db-agent/proofs`
- env-file secret: `/etc/denotary-db-agent/agent.secrets.env`

## Recommended Source Settings

For the current baseline, use:

- `adapter = "db2"`
- `capture_mode = "watermark"`
- explicit schema/table coverage
- explicit watermark and commit timestamp columns
- bounded `row_limit`

Typical source section:

```json
{
  "id": "db2-core-ledger",
  "adapter": "db2",
  "enabled": true,
  "source_instance": "erp-eu-1",
  "database_name": "LEDGER",
  "include": {
    "DB2INST1": ["INVOICES", "PAYMENTS"]
  },
  "checkpoint_policy": "after_ack",
  "backfill_mode": "full",
  "connection": {
    "host": "db2.example.net",
    "port": 50000,
    "username": "db2inst1",
    "password": "replace-me",
    "database": "LEDGER"
  },
  "options": {
    "capture_mode": "watermark",
    "watermark_column": "UPDATED_AT",
    "commit_timestamp_column": "UPDATED_AT",
    "primary_key_columns": ["ID"],
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
python -m denotary_db_agent --config /etc/denotary-db-agent/db2-agent.json doctor --source db2-core-ledger --strict
python -m denotary_db_agent --config /etc/denotary-db-agent/db2-agent.json bootstrap --source db2-core-ledger
python -m denotary_db_agent --config /etc/denotary-db-agent/db2-agent.json inspect --source db2-core-ledger
python -m denotary_db_agent --config /etc/denotary-db-agent/db2-agent.json run --once
python -m denotary_db_agent --config /etc/denotary-db-agent/db2-agent.json run --interval-sec 15
```

## What `inspect` Should Show

Healthy baseline should include:

- `adapter = "db2"`
- `capture_mode = "watermark"`
- tracked tables under `tracked_tables`
- configured `watermark_column`
- configured or discovered primary key columns
- `cdc.runtime.transport = "polling"`

## Validation Status

The current `Db2` validation already confirms:

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

- [wave2-db2-validation.md](wave2-db2-validation.md)
- [wave2-mainnet-budget-validation-report.md](wave2-mainnet-budget-validation-report.md)
- [wave2-mainnet-service-outage-validation-report.md](wave2-mainnet-service-outage-validation-report.md)
- [wave2-readiness-matrix.md](wave2-readiness-matrix.md)

## Operational Notes

- prefer explicit schema and table coverage over broad discovery assumptions
- keep watermark columns monotonic and indexed where practical
- validate timestamp precision and ordering on the real Db2 deployment before
  promotion
- keep driver and client packaging stable across environments

## Current Limits

The current baseline does not yet provide:

- native Db2 CDC integration
- delete tombstone reconstruction after a row disappears between polls
- wildcard table discovery
- a production-scale replacement for native log-based mutation capture

For the current release posture, prefer:

- explicit schema/table coverage
- bounded polling
- regular `doctor`, `inspect`, and `report` checks
- release or rollout evidence based on restart, soak, and degraded-service runs
