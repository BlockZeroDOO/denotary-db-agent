# SQLite Config Reference

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

`SQLite` is part of the active `Wave 2` adapter set.

Current supported model:

- file-backed readiness validation
- tracked-table introspection through `PRAGMA table_info`
- watermark snapshot polling
- deterministic checkpoint resume
- dry-run snapshot playback for local pipeline validation
- local full-cycle proof export

Native SQLite CDC is not part of the current baseline.

## Source Example

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
  "connection": {
    "path": "./runtime/ledger.sqlite3"
  },
  "options": {
    "capture_mode": "watermark",
    "watermark_column": "updated_at",
    "commit_timestamp_column": "updated_at",
    "primary_key_columns": ["id"],
    "row_limit": 100
  }
}
```

## Connection Fields

### `connection.path`

- Type: `string`
- Required: yes
- Purpose: path to the SQLite database file

Notes:

- the current baseline expects an existing file-backed database
- for safety, the adapter does not silently create a new empty database during
  validation

## Include Layout

`include` maps attached SQLite database names to tracked tables.

Example:

```json
{
  "include": {
    "main": ["invoices"],
    "temp": ["staging_events"]
  }
}
```

Notes:

- keys are SQLite database names such as `main` or `temp`
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
- Default: discovered from the SQLite table primary key when available;
  otherwise falls back to `["id"]`

### `options.primary_key_column`

- Type: `string`
- Required: no
- Default: `"id"`
- Purpose: shorthand when only one primary key column is needed

### `options.row_limit`

- Type: `integer`
- Required: no
- Default: inherits the source `batch_size`
- Purpose: maximum number of rows emitted per polling cycle

### `options.dry_run_events`

- Type: `array`
- Required: no
- Purpose: local adapter and pipeline testing without reading a live SQLite file

## Current Validation Status

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

## Current Limits

The current baseline does not yet provide:

- native SQLite CDC
- file-watch driven wakeups
- wildcard table discovery
- automatic schema migration handling for edge files

## Related Docs

- [wave2-sqlite-edge-runbook.md](wave2-sqlite-edge-runbook.md)
- [wave2-sqlite-validation.md](wave2-sqlite-validation.md)
- [wave2-sqlite-validation-report.md](wave2-sqlite-validation-report.md)
- [wave2-readiness-matrix.md](wave2-readiness-matrix.md)
