# SQLite Config Reference

`SQLite` is part of the `Wave 2` roadmap.

Current baseline:

- file-backed readiness validation
- tracked-table introspection through `PRAGMA table_info`
- watermark snapshot polling
- deterministic checkpoint resume
- dry-run snapshot playback for local pipeline validation

Native SQLite CDC is not implemented in the current baseline.

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
- for safety, the adapter does not silently create a new empty database during validation

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

- Value today: `"watermark"`

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
- Default: discovered from the SQLite table primary key when available; otherwise falls back to `["id"]`

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

## Current Validation Scope

Implemented now:

- adapter registration
- config surface
- file-backed readiness validation
- tracked-table introspection
- watermark snapshot polling baseline
- deterministic checkpoint resume
- local full-cycle proof export
- dry-run snapshot playback

Planned next:

- edge-focused runbooks
- bounded soak validation for embedded and local-first scenarios

See also:

- [wave2-sqlite-validation.md](wave2-sqlite-validation.md)
- [wave2-sqlite-validation-report.md](wave2-sqlite-validation-report.md)
- [wave2-sqlite-edge-runbook.md](wave2-sqlite-edge-runbook.md)
