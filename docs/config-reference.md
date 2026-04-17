# deNotary DB Agent Config Reference

This document is the canonical reference for `denotary-db-agent` configuration.

Use it together with:

- [examples/agent.example.json](../examples/agent.example.json)
- [docs/operator-guide.md](operator-guide.md)
- [README.md](../README.md)

## Top-Level Shape

The config file is a single JSON object:

```json
{
  "agent_name": "denotary-db-agent",
  "log_level": "INFO",
  "denotary": {},
  "storage": {},
  "sources": []
}
```

## Top-Level Fields

### `agent_name`

- Type: `string`
- Required: no
- Default: `"denotary-db-agent"`
- Purpose: logical name for logs, diagnostics, and snapshots

### `log_level`

- Type: `string`
- Required: no
- Default: `"INFO"`
- Typical values:
  - `DEBUG`
  - `INFO`
  - `WARNING`
  - `ERROR`

### `denotary`

- Type: `object`
- Required: yes
- Purpose: deNotary backend and on-chain signing settings

### `storage`

- Type: `object`
- Required: yes
- Purpose: local SQLite state and proof/snapshot storage paths

### `sources`

- Type: `array`
- Required: yes
- Must be non-empty: yes
- Purpose: one or more database sources handled by the agent

## `denotary`

Example:

```json
"denotary": {
  "ingress_url": "http://127.0.0.1:8080",
  "watcher_url": "http://127.0.0.1:8081",
  "watcher_auth_token": "integration-shared-token",
  "receipt_url": "http://127.0.0.1:8082",
  "audit_url": "http://127.0.0.1:8083",
  "chain_rpc_url": "https://history.denotary.io",
  "submitter": "enterpriseac1",
  "submitter_permission": "dnanchor",
  "submitter_private_key": "REPLACE_WITH_DNANCHOR_WIF",
  "schema_id": 1,
  "policy_id": 1,
  "billing_account": "verifbill",
  "wait_for_finality": true,
  "finality_timeout_sec": 180,
  "finality_poll_interval_sec": 3.0
}
```

### `ingress_url`

- Type: `string`
- Required: yes
- Purpose: base URL of the deNotary Ingress API
- Used for:
  - `/v1/single/prepare`
  - `/v1/batch/prepare`

### `watcher_url`

- Type: `string`
- Required: yes
- Purpose: base URL of Finality Watcher

### `watcher_auth_token`

- Type: `string`
- Required: no
- Default: `""`
- Purpose: bearer/shared token used by the watcher client when required

### `receipt_url`

- Type: `string`
- Required: no
- Default: `""`
- Purpose: base URL of Receipt Service

### `audit_url`

- Type: `string`
- Required: no
- Default: `""`
- Purpose: base URL of Audit API

### `chain_rpc_url`

- Type: `string`
- Required: no
- Default: `""`
- Purpose: chain RPC endpoint for broadcast and transaction tracking

### `submitter`

- Type: `string`
- Required: yes
- Purpose: enterprise payer account used to sign `verifbill::submit` and `verifbill::submitroot`

### `submitter_permission`

- Type: `string`
- Required: no
- Default: `"dnanchor"`
- Purpose: permission name used by the hot signing key
- Recommendation:
  - use a dedicated custom permission such as `dnanchor`
  - do not use `owner`
  - avoid using `active` on the agent host

### `submitter_private_key`

- Type: `string`
- Required: no in schema, but required for actual built-in signing
- Default: `""`
- Purpose: WIF private key for `submitter@submitter_permission`
- Recommendation:
  - store outside source control
  - inject at deploy time

### `schema_id`

- Type: `integer`
- Required: yes
- Purpose: `verif` schema id used by enterprise notarization

### `policy_id`

- Type: `integer`
- Required: yes
- Purpose: `verif` policy id used by enterprise notarization

### `billing_account`

- Type: `string`
- Required: no
- Default: `"verifbill"`
- Purpose: billing contract account used for enterprise submissions

### `wait_for_finality`

- Type: `boolean`
- Required: no
- Default: `false`
- Purpose: whether the agent should wait for finality before closing the delivery flow

### `finality_timeout_sec`

- Type: `integer`
- Required: no
- Default: `120`
- Purpose: maximum time to wait for finality

### `finality_poll_interval_sec`

- Type: `number`
- Required: no
- Default: `2.0`
- Purpose: polling interval while waiting for finality

## `storage`

Example:

```json
"storage": {
  "state_db": "./data/agent-state.sqlite3",
  "proof_dir": "./data/proofs",
  "proof_retention": 1000,
  "delivery_retention": 5000,
  "dlq_retention": 1000
}
```

### `state_db`

- Type: `string`
- Required: yes
- Purpose: SQLite file used for:
  - checkpoints
  - delivery history
  - paused state
  - proof metadata
  - diagnostics snapshot default parent directory

### `proof_dir`

- Type: `string`
- Required: no
- Default: `"runtime/proofs"`
- Purpose: directory where exported proof bundles are written

### `proof_retention`

- Type: `integer`
- Required: no
- Default: `0`
- Meaning:
  - `0` disables pruning
  - positive values keep only the newest N proof artifacts per source
- Purpose: prune old proof metadata and exported proof JSON bundles together

### `delivery_retention`

- Type: `integer`
- Required: no
- Default: `0`
- Meaning:
  - `0` disables pruning
  - positive values keep only the newest N delivery history rows per source
- Purpose: bound the size of the local `deliveries` table during long-lived operation

### `dlq_retention`

- Type: `integer`
- Required: no
- Default: `0`
- Meaning:
  - `0` disables pruning
  - positive values keep only the newest N DLQ rows per source
- Purpose: keep local failure history bounded without manual SQLite cleanup

## `sources[]`

Each entry in `sources` is one logical input source.

Example:

```json
{
  "id": "pg-core-ledger",
  "adapter": "postgresql",
  "enabled": true,
  "source_instance": "erp-eu-1",
  "database_name": "ledger",
  "include": {
    "public": ["invoices", "payments"]
  },
  "exclude": {},
  "checkpoint_policy": "after_ack",
  "backfill_mode": "full",
  "batch_enabled": false,
  "batch_size": 100,
  "flush_interval_ms": 1000,
  "connection": {},
  "options": {}
}
```

### Common source fields

#### `id`

- Type: `string`
- Required: yes
- Purpose: stable source identifier used in state, logs, diagnostics, and CLI commands

#### `adapter`

- Type: `string`
- Required: yes
- Purpose: adapter name
- Current PostgreSQL value:
  - `"postgresql"`

#### `enabled`

- Type: `boolean`
- Required: no
- Default: `true`
- Purpose: enables or disables the source without removing it from config

#### `source_instance`

- Type: `string`
- Required: yes
- Purpose: logical source installation or environment name
- Example:
  - `"erp-eu-1"`

#### `database_name`

- Type: `string`
- Required: yes
- Purpose: business/database label used in the canonical envelope

#### `include`

- Type: `object<string, string[]>`
- Required: no
- Default: `{}`
- Purpose: schemas/namespaces and tracked tables
- PostgreSQL example:

```json
"include": {
  "public": ["invoices", "payments"]
}
```

#### `exclude`

- Type: `object<string, string[]>`
- Required: no
- Default: `{}`
- Purpose: optional exclusions inside included namespaces

#### `checkpoint_policy`

- Type: `string`
- Required: no
- Default: `"after_ack"`
- Current expected value:
  - `"after_ack"`

#### `backfill_mode`

- Type: `string`
- Required: no
- Default: `"none"`
- Common values:
  - `"none"`
  - `"full"`
- Recommendation for first deployment:
  - `"full"`

#### `batch_enabled`

- Type: `boolean`
- Required: no
- Default: `false`
- Purpose: whether the source should batch events before anchoring

#### `batch_size`

- Type: `integer`
- Required: no
- Default: `100`
- Purpose: target batch size when `batch_enabled = true`

#### `flush_interval_ms`

- Type: `integer`
- Required: no
- Default: `1000`
- Purpose: maximum wait before an open batch is flushed

#### `connection`

- Type: `object`
- Required: no
- Default: `{}`
- Purpose: DB-specific connection settings

#### `options`

- Type: `object`
- Required: no
- Default: `{}`
- Purpose: adapter-specific runtime settings

## PostgreSQL `connection`

Example:

```json
"connection": {
  "host": "127.0.0.1",
  "port": 5432,
  "username": "denotary",
  "database": "ledger"
}
```

### Supported keys

#### `host`

- Type: `string`
- Required: yes for live PostgreSQL use

#### `port`

- Type: `integer`
- Required: no
- Typical value:
  - `5432`

#### `username`

- Type: `string`
- Required: yes for live PostgreSQL use

#### `database`

- Type: `string`
- Required: yes for live PostgreSQL use

### Notes

- Password handling is deployment-specific; keep secrets out of git.
- The adapter uses live PostgreSQL introspection during:
  - `validate`
  - `bootstrap`
  - `inspect`
  - `refresh`
  - runtime processing

## PostgreSQL `options`

### Core capture settings

#### `capture_mode`

- Type: `string`
- Required: no
- Common values:
  - `"watermark"`
  - `"trigger"`
  - `"logical"`
- Purpose:
  - `watermark`: polling baseline
  - `trigger`: plugin-managed trigger CDC with `LISTEN/NOTIFY`
  - `logical`: logical replication slot based CDC

#### `watermark_column`

- Type: `string`
- Required: yes for `capture_mode = "watermark"`
- Purpose: monotonically increasing ordering column
- Recommended values:
  - `updated_at`
  - commit-like timestamp column

#### `commit_timestamp_column`

- Type: `string`
- Required: recommended
- Purpose: value used as event commit timestamp in the canonical envelope

#### `cleanup_processed_events`

- Type: `boolean`
- Required: no
- Default: `true`
- Relevant for:
  - `capture_mode = "trigger"`
- Purpose: delete processed rows from `denotary_cdc.events` after checkpoint advancement

#### `row_limit`

- Type: `integer`
- Required: no
- Default: adapter-controlled
- Purpose: max rows/changes fetched per iteration

### Logical replication settings

#### `slot_name`

- Type: `string`
- Required: yes for logical mode in practice
- Example:
  - `"denotary_slot"`

#### `output_plugin`

- Type: `string`
- Required: no
- Default: `"test_decoding"`
- Supported values:
  - `"test_decoding"`
  - `"pgoutput"`

#### `logical_runtime_mode`

- Type: `string`
- Required: no
- Effective default:
  - `stream` for `pgoutput`
- Supported values:
  - `"stream"`
  - `"peek"`
- Purpose:
  - `stream`: replication protocol runtime
  - `peek`: SQL polling fallback via `pg_logical_slot_peek_*`

#### `publication_name`

- Type: `string`
- Required: recommended for `output_plugin = "pgoutput"`
- Example:
  - `"denotary_pub"`

#### `auto_create_slot`

- Type: `boolean`
- Required: no
- Default: `true`
- Purpose: let the agent create the logical slot if missing

#### `auto_create_publication`

- Type: `boolean`
- Required: no
- Default: `true`
- Relevant for:
  - `output_plugin = "pgoutput"`
- Purpose: let the agent create/refresh the publication

#### `replica_identity_full`

- Type: `boolean`
- Required: no
- Default: `true`
- Purpose: enforce `REPLICA IDENTITY FULL` on tracked logical tables when needed

#### `logical_wait_poll_sec`

- Type: `number`
- Required: no
- Default: `0.5`
- Purpose: lightweight wait/poll interval for logical mode

#### `logical_stream_timeout_sec`

- Type: `number`
- Required: no
- Default: `2.0`
- Purpose: stream read timeout for replication protocol runtime

#### `logical_stream_idle_timeout_sec`

- Type: `number`
- Required: no
- Default: `0.5`
- Purpose: idle cutoff for bounded stream loops

### Stream reconnect, backoff, and fallback

#### `logical_stream_reconnect_base_delay_sec`

- Type: `number`
- Required: no
- Default: `0.5`
- Purpose: initial reconnect cooldown after stream failures

#### `logical_stream_reconnect_max_delay_sec`

- Type: `number`
- Required: no
- Default: `10.0`
- Purpose: upper bound for reconnect cooldown

#### `logical_stream_error_history_size`

- Type: `integer`
- Required: no
- Default: `5`
- Purpose: number of recent stream errors kept in diagnostics history

#### `logical_stream_fallback_failure_threshold`

- Type: `integer`
- Required: no
- Default: `3`
- Purpose: failure streak threshold before `stream` is temporarily demoted to `peek`

#### `logical_stream_fallback_sec`

- Type: `number`
- Required: no
- Default: `30.0`
- Purpose: duration of temporary `peek` fallback window

#### `logical_stream_probation_sec`

- Type: `number`
- Required: no
- Default: `15.0`
- Purpose: degraded observation window after returning from fallback to `stream`

### Logical warning thresholds

#### `logical_warn_retained_wal_bytes`

- Type: `integer`
- Required: no
- Default: `268435456`
- Meaning:
  - `256 MiB`
- Purpose: warn when retained WAL backlog crosses this threshold

#### `logical_warn_flush_lag_bytes`

- Type: `integer`
- Required: no
- Default: `67108864`
- Meaning:
  - `64 MiB`
- Purpose: warn when flush lag crosses this threshold

## Recommended PostgreSQL Starting Point

For a first real deployment:

```json
{
  "id": "pg-core-ledger",
  "adapter": "postgresql",
  "enabled": true,
  "source_instance": "erp-eu-1",
  "database_name": "ledger",
  "include": {
    "public": ["invoices", "payments"]
  },
  "checkpoint_policy": "after_ack",
  "backfill_mode": "full",
  "batch_enabled": false,
  "connection": {
    "host": "127.0.0.1",
    "port": 5432,
    "username": "denotary",
    "database": "ledger"
  },
  "options": {
    "capture_mode": "logical",
    "commit_timestamp_column": "updated_at",
    "slot_name": "denotary_slot",
    "output_plugin": "pgoutput",
    "publication_name": "denotary_pub",
    "logical_runtime_mode": "stream",
    "auto_create_slot": true,
    "auto_create_publication": true,
    "replica_identity_full": true,
    "row_limit": 250
  }
}
```

## Operational Notes

- `validate` expects reachable live services when URLs are configured.
- `inspect` and `refresh` for PostgreSQL are live operations.
- `diagnostics --save-snapshot` writes JSON snapshots under the runtime directory.
- diagnostics snapshot retention now defaults to keeping the newest `20` matching files per source.
- use `--snapshot-retention <N>` to tune local diagnostics history length.
- runtime artifact retention is configured separately in `storage`:
  - `proof_retention`
  - `delivery_retention`
  - `dlq_retention`
- when `proof_retention` is enabled, pruned proof bundle files are deleted from disk together with their SQLite metadata rows.

## Security Notes

- Keep `owner` offline.
- Do not place `active` on the DB Agent host.
- Use a dedicated hot permission such as `dnanchor`.
- Store `submitter_private_key` outside source control.
