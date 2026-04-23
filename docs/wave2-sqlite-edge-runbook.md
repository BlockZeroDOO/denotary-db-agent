# Wave 2 SQLite Edge Runbook

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

This runbook describes the current release-oriented baseline for deploying
`denotary-db-agent` against a file-backed `SQLite` database in edge, embedded,
desktop, or local-first environments.

The supported source model today is:

- `SQLite` remains the local system-of-record for the edge workload
- `denotary-db-agent` polls tracked tables on a bounded interval
- notarization proofs are exported locally and anchored through the normal
  `deNotary` service stack
- the local checkpoint store survives process restarts

Native SQLite CDC is not part of the current baseline.

## Recommended Fit

Use the `SQLite` adapter when:

- the application already writes to a local `.sqlite3` file
- the workload is append-heavy or periodically updated
- continuous CDC is not required
- bounded polling and deterministic proof export are acceptable

Good examples:

- edge gateways
- branch-office or kiosk apps
- local-first desktop software
- mobile or field-device sync buffers
- embedded compliance logs

## Reference Config

Starter config:

- [deploy/config/sqlite-edge-agent.example.json](../deploy/config/sqlite-edge-agent.example.json)

Reference docs:

- [sqlite-config-reference.md](sqlite-config-reference.md)
- [storage-config-reference.md](storage-config-reference.md)
- [denotary-service-config-reference.md](denotary-service-config-reference.md)
- [denotary-env-file-runbook.md](denotary-env-file-runbook.md)

## Preconditions

Before starting the agent, confirm:

1. the `SQLite` file already exists
2. tracked tables already exist inside the file
3. each tracked table has a stable primary key
4. each tracked table has a monotonic watermark column such as `updated_at`
5. the service user can read the `SQLite` file
6. the service user can write the configured `storage.state_db` and `proof_dir`
7. the `deNotary` service stack and signer are already reachable

## Recommended Layout

Example Linux layout:

- application database: `/var/lib/edge-app/ledger.sqlite3`
- agent state: `/var/lib/denotary-db-agent/sqlite-edge-state.sqlite3`
- proof export directory: `/var/lib/denotary-db-agent/proofs`
- env-file secret: `/etc/denotary-db-agent/agent.secrets.env`

Keep the agent checkpoint database separate from the application database.

## Recommended Source Settings

For the current baseline, use:

- `adapter = "sqlite"`
- `capture_mode = "watermark"`
- explicit `include`
- explicit `primary_key_columns`
- explicit `watermark_column`

Typical source section:

```json
{
  "id": "sqlite-edge-ledger",
  "adapter": "sqlite",
  "enabled": true,
  "source_instance": "edge-device-1",
  "database_name": "ledger",
  "include": {
    "main": ["invoices", "payments"]
  },
  "checkpoint_policy": "after_ack",
  "backfill_mode": "full",
  "connection": {
    "path": "/var/lib/edge-app/ledger.sqlite3"
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
python -m denotary_db_agent --config /etc/denotary-db-agent/sqlite-edge-agent.json doctor --source sqlite-edge-ledger --strict
python -m denotary_db_agent --config /etc/denotary-db-agent/sqlite-edge-agent.json bootstrap --source sqlite-edge-ledger
python -m denotary_db_agent --config /etc/denotary-db-agent/sqlite-edge-agent.json inspect --source sqlite-edge-ledger
python -m denotary_db_agent --config /etc/denotary-db-agent/sqlite-edge-agent.json run --once
python -m denotary_db_agent --config /etc/denotary-db-agent/sqlite-edge-agent.json run --interval-sec 30
```

## What `inspect` Should Show

Healthy baseline should include:

- `adapter = "sqlite"`
- `capture_mode = "watermark"`
- tracked tables under `tracked_tables`
- configured `watermark_column`
- configured or discovered primary key columns
- `cdc.runtime.transport = "polling"`

## Validation Status

The current `SQLite` validation already confirms:

- file-backed baseline validation
- local full-cycle proof export
- cold restart recovery validation
- short-soak validation
- bounded long-soak validation
- local service-outage recovery validation
- real `denotary` mainnet happy-path validation
- bounded mainnet budget validation
- real mainnet degraded-service recovery validation

Reference:

- [wave2-sqlite-validation.md](wave2-sqlite-validation.md)
- [wave2-sqlite-validation-report.md](wave2-sqlite-validation-report.md)
- [wave2-readiness-matrix.md](wave2-readiness-matrix.md)

## Operational Notes

- prefer polling intervals that match the write pattern of the edge app
- avoid tables whose watermark can move backward
- keep proof retention bounded on storage-constrained hosts
- archive proof bundles during uplink windows if the device is intermittently
  connected
- keep the application database and agent checkpoint database as separate files

## Current Limits

The current baseline does not yet provide:

- native SQLite CDC
- file-watch driven wakeups
- wildcard table discovery
- automatic schema migration handling for edge files

For the current release posture, prefer:

- explicit tracked tables
- explicit PK and watermark configuration
- bounded polling
- regular `doctor`, `inspect`, and `report` checks
- release or rollout evidence based on restart, soak, and degraded-service runs
