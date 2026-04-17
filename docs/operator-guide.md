# deNotary DB Agent Operator Guide

## Commands

### Validate

```bash
denotary-db-agent --config examples/agent.example.json validate
```

Checks:

- config shape
- adapter availability
- live PostgreSQL connectivity for the configured source
- tracked table introspection and key column discovery

### Run One Pass

```bash
denotary-db-agent --config examples/agent.example.json run --once
```

In the current scaffold this is intended for:

- PostgreSQL watermark polling
- snapshot/bootstrap testing
- local integration with `Ingress API` and `Finality Watcher`

### Status

```bash
denotary-db-agent --config examples/agent.example.json status
```

Returns:

- source ids
- configured adapters
- current checkpoints
- delivery count
- DLQ count

### Replay / Reset Checkpoint

```bash
denotary-db-agent --config examples/agent.example.json replay --source pg-core-ledger
```

or:

```bash
denotary-db-agent --config examples/agent.example.json checkpoint --source pg-core-ledger --reset
```

## Current Adapter Targets

- PostgreSQL: logical decoding / WAL plan
- MySQL: row-based binlog plan
- MariaDB: MariaDB binlog profile
- SQL Server: CDC / Change Tracking plan
- Oracle: redo / LogMiner plan
- MongoDB: change streams plan

## PostgreSQL Baseline

The current PostgreSQL adapter is the first live implementation and works as:

- read included tables from a live PostgreSQL database
- order rows by a configured watermark column plus primary key
- emit deterministic `snapshot` events
- persist per-table checkpoints in the local SQLite state store
- resume from the last delivered watermark/primary-key position

Expected source options:

- `watermark_column`
- `commit_timestamp_column`
- optional `primary_key_columns` overrides keyed by `schema.table`
- optional `row_limit`

Recommended first-run behavior:

- set `backfill_mode` to `full`
- use a monotonically increasing timestamp column like `updated_at`
- ensure all tracked tables have a primary key

## PostgreSQL Live Harness

Use the included live harness to validate the adapter against a real PostgreSQL container:

PowerShell:

```powershell
./scripts/run-live-postgres-integration.ps1
```

Shell:

```bash
./scripts/run-live-postgres-integration.sh
```

The harness validates:

- live database connectivity
- table introspection
- initial snapshot/backfill delivery
- checkpoint persistence
- incremental resume after new rows are inserted

## Permissions Planning

Per-database operator docs still need to be expanded in later waves, but the expected direction is:

- read-only access to the target objects
- CDC-specific privileges where required
- metadata/catalog visibility for included schemas and tables

## State Files

The local SQLite state file stores:

- source checkpoints
- delivery attempts
- DLQ records

Back up this file if replay/recovery history matters operationally.
