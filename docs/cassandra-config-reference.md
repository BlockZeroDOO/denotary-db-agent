# Apache Cassandra Config Reference

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

`Apache Cassandra` is part of the active `Wave 2` adapter set.

Current supported model:

- connection-shape validation
- live cluster ping through `cassandra-driver`
- tracked-table introspection
- watermark snapshot polling
- deterministic checkpoint resume
- dry-run snapshot playback
- local full-cycle proof export

Native Cassandra CDC is not part of the current baseline.

## Source Example

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
  "connection": {
    "host": "127.0.0.1",
    "port": 9042,
    "username": "cassandra",
    "password": "secret"
  },
  "options": {
    "capture_mode": "watermark",
    "watermark_column": "updated_at",
    "commit_timestamp_column": "updated_at",
    "row_limit": 1000
  }
}
```

## Connection Fields

### `connection.host`

- Type: `string`
- Required: yes, unless `connection.hosts` is used

### `connection.hosts`

- Type: `string[]`
- Required: no
- Purpose: optional multi-node contact points

### `connection.port`

- Type: `integer`
- Required: no
- Default: `9042`

### `connection.username`

- Type: `string`
- Required: no

### `connection.password`

- Type: `string`
- Required: no

## Include Layout

`include` maps keyspaces to explicitly tracked tables.

Example:

```json
{
  "include": {
    "ledger": ["invoices", "payments"],
    "reporting": ["daily_totals"]
  }
}
```

Notes:

- keys are keyspace names
- values must be explicit table names
- the current baseline does not support wildcard table discovery

## Adapter Options

### `options.capture_mode`

- Type: `string`
- Supported values: `"watermark"`
- Default: `"watermark"`

### `options.watermark_column`

- Type: `string`
- Required: no
- Default: `"updated_at"`

### `options.commit_timestamp_column`

- Type: `string`
- Required: no
- Default: same as `watermark_column`

### `options.primary_key_columns`

- Type: `string[]`
- Required: no

### `options.primary_key_column`

- Type: `string`
- Required: no

### `options.row_limit`

- Type: `integer`
- Required: no
- Default: inherits the source `batch_size`

### `options.dry_run_events`

- Type: `array`
- Required: no
- Purpose: local adapter and pipeline testing without a live cluster

## Current Validation Status

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

## Current Limits

The current baseline does not yet provide:

- native Cassandra CDC integration
- delete tombstone reconstruction after a row disappears between polls
- wildcard table discovery
- a production-scale replacement for native mutation streaming

## Related Docs

- [wave2-cassandra-runbook.md](wave2-cassandra-runbook.md)
- [wave2-cassandra-validation.md](wave2-cassandra-validation.md)
- [wave2-cassandra-validation-report.md](wave2-cassandra-validation-report.md)
- [wave2-mainnet-budget-validation-report.md](wave2-mainnet-budget-validation-report.md)
- [wave2-mainnet-service-outage-validation-report.md](wave2-mainnet-service-outage-validation-report.md)
- [wave2-readiness-matrix.md](wave2-readiness-matrix.md)
- [../deploy/config/cassandra-agent.example.json](../deploy/config/cassandra-agent.example.json)
