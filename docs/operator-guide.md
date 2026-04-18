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
For PostgreSQL sources in `capture_mode = "logical"` the daemon checks the logical slot for
pending changes before the fallback interval expires, so the loop can wake sooner when WAL data
is already queued for delivery.

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
- per-source `severity`
- best-effort health for configured chain / receipt / audit services
- configured ingress / watcher endpoints
- source-level warnings for logical slot drift and WAL lag thresholds when PostgreSQL logical mode is used

### Doctor

```bash
denotary-db-agent --config examples/agent.example.json doctor --source pg-core-ledger
denotary-db-agent --config examples/agent.example.json doctor --source pg-core-ledger --save-snapshot
denotary-db-agent --config examples/agent.example.json doctor --source pg-core-ledger --strict
```

Returns one compact live preflight report for deploy readiness with:

- config path checks
- reachability of configured deNotary services
- chain RPC readiness
- signer readiness for `submitter@submitter_permission`
- billing account existence
- per-source connectivity and tracked-table visibility

Use this before:

- first live rollout
- key rotation
- moving the agent to a new host
- enabling a new PostgreSQL source

With `--save-snapshot`, the same preflight JSON is written under the local runtime directory.
Use this when operators want rollout evidence or want to attach the exact readiness report to a change ticket.

With `--strict`, the command exits with status `1` only when overall severity is `critical` or `error`.
This is intended for:

- CI/CD pre-deploy gates
- `systemd` `ExecStartPre`
- Windows service pre-start wrappers

### Metrics

```bash
denotary-db-agent --config examples/agent.example.json metrics --source pg-core-ledger
```

Returns a compact export-friendly JSON view with:

- per-source severity
- delivery / proof / DLQ counts
- pending logical-slot state
- retained WAL / flush lag bytes
- effective stream runtime mode
- reconnect / backoff / fallback / probation flags

Use this when an external scheduler, log shipper, or monitoring sidecar wants a small stable payload instead of full `inspect` or `diagnostics`.

### Report

```bash
denotary-db-agent --config examples/agent.example.json report --source pg-core-ledger --save-snapshot
```

Returns and optionally saves one compact rollout evidence bundle with:

- `doctor`
- `metrics`
- `diagnostics`
- `status`

Use this when operators want one file per rollout or change window instead of several separate JSON artifacts.

All saved `diagnostics`, `doctor`, and `report` snapshots are also indexed in the local manifest:

- `data/diagnostics/evidence-manifest.json`

The manifest itself is capped by:

- `storage.evidence_manifest_retention`

### Artifacts

```bash
denotary-db-agent --config examples/agent.example.json artifacts --source pg-core-ledger
denotary-db-agent --config examples/agent.example.json artifacts --source pg-core-ledger --kind report
```

Returns entries from the local evidence manifest with optional filters by:

- `source`
- `kind`

Use this when operators want to find the latest rollout evidence files without opening the manifest JSON manually.

### Diagnostics

```bash
denotary-db-agent --config examples/agent.example.json diagnostics --source pg-core-ledger
denotary-db-agent --config examples/agent.example.json diagnostics --source pg-core-ledger --save-snapshot
denotary-db-agent --config examples/agent.example.json diagnostics --source pg-core-ledger --save-snapshot --snapshot-retention 10
```

Returns a compact operator-focused report with:

- source severity and warnings
- configured vs effective stream runtime mode
- reconnect / fallback / probation state
- recent stream error history
- logical slot progress markers and lag counters

With `--save-snapshot`, the command also writes the same JSON payload to a timestamped file under the
local runtime directory. Use `--output <path>` when you want a deterministic file path for automation.
By default the command keeps only the newest `20` matching diagnostics snapshots per source; override
that with `--snapshot-retention <N>` when operators need a shorter or longer local history.

### Bootstrap

```bash
denotary-db-agent --config examples/agent.example.json bootstrap --source pg-core-ledger
```

For PostgreSQL sources this:

- validates live connectivity
- discovers tracked tables
- for `capture_mode = "trigger"`, creates or refreshes `denotary_cdc` schema objects and table triggers
- for `capture_mode = "logical"`, ensures logical prerequisites such as `wal_level=logical`, `REPLICA IDENTITY FULL`, and logical slot setup
- when `output_plugin = "pgoutput"`, also creates or refreshes the configured publication

### Inspect

```bash
denotary-db-agent --config examples/agent.example.json inspect --source pg-core-ledger
```

For PostgreSQL this returns:

- capture mode
- tracked tables
- selected columns per tracked table
- primary key and watermark settings
- trigger CDC schema status when `capture_mode = "trigger"`
- logical slot status when `capture_mode = "logical"`
- `pgoutput` publication state and publication tables when `output_plugin = "pgoutput"`
- whether `pgoutput` publication tables are currently in sync with tracked tables
- `replica_identity_expected` for tracked logical tables
- `replica_identity_by_table` for tracked logical tables
- whether tracked logical tables are currently in sync with the expected REPLICA IDENTITY mode
- `pending_changes` for logical slots
- `current_wal_lsn`, `retained_wal_bytes`, and `flush_lag_bytes` for logical slots
- `stream_session_active`, `stream_start_lsn`, and `stream_acknowledged_lsn` for PostgreSQL stream mode
- `stream_connect_count`, `stream_reconnect_count`, `stream_last_connect_at`, and `stream_last_reconnect_at`
- `stream_last_reconnect_reason`
- `stream_last_error`, `stream_last_error_kind`, and `stream_last_error_at`
- `stream_error_history`
- `stream_failure_streak`, `stream_backoff_active`, and `stream_backoff_until`
- `stream_fallback_active`, `stream_fallback_remaining_sec`, `stream_fallback_until`, and `stream_fallback_reason`
- `effective_runtime_mode` so operators can see when PostgreSQL is temporarily running in `peek` fallback
- `stream_probation_active`, `stream_probation_remaining_sec`, `stream_probation_until`, and `stream_probation_reason`
- installed trigger count or logical/publication state, depending on mode

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
For `pgoutput`, `refresh` also repairs publication drift when publication membership no longer
matches the tracked table set. When `replica_identity_full = true`, `refresh` also repairs
REPLICA IDENTITY drift for tracked logical tables.

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
- in logical mode, support both `test_decoding` and `pgoutput`
- in logical mode, advance the logical slot only after a successful delivery checkpoint
- in logical mode, keep a transaction-safe cursor so multi-row transactions are replayed correctly even with a small `row_limit`
- in logical mode, surface slot backlog and WAL lag in `inspect`
- in logical mode, wake daemon loops when the logical slot already has pending changes
- in `pgoutput` mode, manage the publication lifecycle inside the agent
- in `pgoutput` mode, use bounded replication-protocol streaming by default
- in `pgoutput` mode, keep explicit SQL peeking as a fallback runtime mode
- bounded streaming sends standby-status feedback using the last already-acknowledged LSN
- when stream mode is active, post-delivery checkpoint advancement updates the active session ack directly

Expected source options:

- `watermark_column`
- `commit_timestamp_column`
- optional `primary_key_columns` overrides keyed by `schema.table`
- optional `row_limit`
- optional `cleanup_processed_events` for trigger mode, default `true`
- optional `slot_name` for logical mode
- optional `output_plugin`, default `test_decoding`
- optional `logical_runtime_mode`, default `stream` for `pgoutput`
- optional `publication_name` for `output_plugin = "pgoutput"`
- optional `auto_create_slot`, default `true`
- optional `auto_create_publication`, default `true`
- optional `replica_identity_full`, default `true`
- optional `logical_wait_poll_sec`, default `0.5`
- optional `logical_stream_timeout_sec`, default `2.0`
- optional `logical_stream_idle_timeout_sec`, default `0.5`
- optional `logical_stream_reconnect_base_delay_sec`, default `0.5`
- optional `logical_stream_reconnect_max_delay_sec`, default `10.0`
- optional `logical_stream_error_history_size`, default `5`
- optional `logical_stream_fallback_failure_threshold`, default `3`
- optional `logical_stream_fallback_sec`, default `30.0`
- optional `logical_stream_probation_sec`, default `15.0`
- optional `logical_warn_retained_wal_bytes`, default `268435456`
- optional `logical_warn_flush_lag_bytes`, default `67108864`

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
- `pgoutput` publication bootstrap / inspect with live PostgreSQL state
- `pgoutput` live insert/update/delete capture through both bounded streaming and SQL peeking

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

## Runtime Retention

Use `storage` retention settings when you want the agent to prune old local artifacts automatically:

- `proof_retention`
- `delivery_retention`
- `dlq_retention`

Behavior:

- values of `0` disable pruning
- positive values keep only the newest N rows per source
- when old proof rows are pruned, the corresponding exported proof JSON files are also deleted from disk

Recommended starting point for long-running PostgreSQL installations:

```json
"storage": {
  "state_db": "./data/agent-state.sqlite3",
  "proof_dir": "./data/proofs",
  "proof_retention": 1000,
  "delivery_retention": 5000,
  "dlq_retention": 1000,
  "diagnostics_snapshot_interval_sec": 900,
  "diagnostics_snapshot_retention": 20
}
```

With `diagnostics_snapshot_interval_sec > 0`, daemon mode writes an all-sources diagnostics snapshot on its own schedule.
This is useful when operators want periodic health evidence even if nobody runs `diagnostics --save-snapshot` manually.
