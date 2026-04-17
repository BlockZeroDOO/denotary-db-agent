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
- chain RPC connectivity when `chain_rpc_url` is configured
- Receipt Service and Audit API health when their URLs are configured
- tracked table introspection and key column discovery

### Run One Pass

```bash
denotary-db-agent --config examples/agent.example.json run --once
```

In the current scaffold this is intended for:

- PostgreSQL watermark polling
- snapshot/bootstrap testing
- local integration with `Ingress API`, `Finality Watcher`, `Receipt Service`, and `Audit API`
- built-in `verifbill` signing/broadcast using the configured hot permission key

### Run As Daemon

```bash
denotary-db-agent --config examples/agent.example.json run --interval-sec 5
```

This keeps the PostgreSQL plugin running continuously and re-checking sources on the given interval.

For PostgreSQL sources in `capture_mode = "trigger"` the daemon also waits on database `LISTEN/NOTIFY`
signals, so inserts/updates/deletes can wake the agent before the fallback interval expires.

### Status

```bash
denotary-db-agent --config examples/agent.example.json status
```

Returns:

- source ids
- configured adapters
- current checkpoints
- delivery count
- proof bundle count
- DLQ count

### Proof

```bash
denotary-db-agent --config examples/agent.example.json proof --request-id <request_id>
```

Returns stored proof metadata for a finalized request, including the exported proof bundle path.

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
- prepare and sign `verifbill::submit` inside the agent
- advance watcher state from registration to included/finalized
- fetch finalized receipt and audit proof chain
- export a local proof bundle JSON file
- persist per-table checkpoints in the local SQLite state store
- resume from the last delivered watermark/primary-key position
- in trigger mode, capture `insert/update/delete` events via plugin-managed triggers
- wake daemon loops through PostgreSQL `LISTEN/NOTIFY`
- optionally delete processed rows from `denotary_cdc.events` after checkpoint advancement

Expected source options:

- `watermark_column`
- `commit_timestamp_column`
- optional `primary_key_columns` overrides keyed by `schema.table`
- optional `row_limit`
- optional `cleanup_processed_events` for trigger mode, default `true`

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
- trigger CDC cleanup after processed events are checkpointed

## Enterprise Signer Permission

Recommended config:

- `submitter`: enterprise payer account
- `submitter_permission`: `dnanchor`

Recommended security model:

- no `owner` key on the DB Agent host
- no `active` key on the DB Agent host
- no `eosio.token::transfer` permission on the hot broadcaster key

Helper scripts that print `updateauth` / `linkauth` commands:

- [scripts/print-verifbill-permission-commands.ps1](../scripts/print-verifbill-permission-commands.ps1)
- [scripts/print-verifbill-permission-commands.sh](../scripts/print-verifbill-permission-commands.sh)

## Permissions Planning

Per-database operator docs still need to be expanded in later waves, but the expected direction is:

- read-only access to the target objects
- CDC-specific privileges where required
- metadata/catalog visibility for included schemas and tables

## State Files

The local SQLite state file stores:

- source checkpoints
- delivery attempts
- proof bundle metadata
- DLQ records

Back up this file if replay/recovery history matters operationally.
