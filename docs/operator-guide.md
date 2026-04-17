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
- PostgreSQL logical decoding polling from a logical slot
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

### Health

```bash
denotary-db-agent --config examples/agent.example.json health
```

Returns:

- local source runtime state
- paused/resumed state
- best-effort health for configured chain / receipt / audit services
- configured ingress / watcher endpoints

### Bootstrap

```bash
denotary-db-agent --config examples/agent.example.json bootstrap --source pg-core-ledger
```

For PostgreSQL sources this:

- validates live connectivity
- discovers tracked tables
- for `capture_mode = "trigger"`, creates or refreshes `denotary_cdc` schema objects and table triggers
- for `capture_mode = "logical"`, ensures logical prerequisites such as `wal_level=logical`, `REPLICA IDENTITY FULL`, and logical slot setup

### Inspect

```bash
denotary-db-agent --config examples/agent.example.json inspect --source pg-core-ledger
```

For PostgreSQL this returns:

- capture mode
- tracked tables
- primary key and watermark settings
- trigger CDC schema status when `capture_mode = "trigger"`
- logical slot status when `capture_mode = "logical"`
- installed trigger count or slot state, depending on mode

### Refresh

```bash
denotary-db-agent --config examples/agent.example.json refresh --source pg-core-ledger
```

Use this when:

- tracked tables gained or lost columns
- `ALTER TABLE` changed the selected column set you want notarized
- primary key or watermark assumptions changed
- you want to force a runtime artifact reinstall before restarting the daemon

The agent also performs this refresh automatically when the stored runtime signature no longer
matches the live PostgreSQL table shape. `inspect` now reports the live `selected_columns`
per tracked table, so operators can confirm what the current runtime will hash and send.

### Pause / Resume

```bash
denotary-db-agent --config examples/agent.example.json pause --source pg-core-ledger
denotary-db-agent --config examples/agent.example.json resume --source pg-core-ledger
```

This is useful when:

- you want to stop one noisy source
- you are doing table maintenance or schema changes
- you want to keep checkpoints and state but temporarily suppress new deliveries

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

- PostgreSQL: watermark polling, trigger CDC, and logical decoding
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
- in logical mode, read `insert/update/delete` from a PostgreSQL logical replication slot
- in logical mode, advance the logical slot only after a successful delivery checkpoint
- in logical mode, keep a transaction-safe cursor so multi-row transactions are replayed correctly even with a small `row_limit`

Expected source options:

- `watermark_column`
- `commit_timestamp_column`
- optional `primary_key_columns` overrides keyed by `schema.table`
- optional `row_limit`
- optional `cleanup_processed_events` for trigger mode, default `true`
- optional `slot_name` for logical mode
- optional `output_plugin`, default `test_decoding`
- optional `auto_create_slot`, default `true`
- optional `replica_identity_full`, default `true`

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
- logical decoding capture through a live replication slot
- preservation of multi-row logical transactions when `row_limit = 1`

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
