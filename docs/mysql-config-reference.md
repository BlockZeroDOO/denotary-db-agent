# MySQL Config Reference

This document describes MySQL-specific `connection` and `options` fields.

## Current Status

MySQL currently supports a live watermark/snapshot baseline.

Implemented now:

- live connection validation
- tracked table introspection
- watermark-based snapshot polling
- deterministic checkpoint resume
- bootstrap / inspect / runtime signature

Not implemented yet:

- row-based binlog CDC

## `connection`

```json
"connection": {
  "host": "127.0.0.1",
  "port": 3306,
  "username": "denotary",
  "database": "ledger"
}
```

### Supported keys

- `host`: `string`, required
- `port`: `integer`, typical value `3306`
- `username`: `string`, required
- `database`: `string`, required
- `password`: `string`, optional, deployment-specific

## `options`

### Supported now

- `capture_mode`
  - current practical value: `"watermark"`
- `watermark_column`
  - default: `"updated_at"`
- `commit_timestamp_column`
  - default: same as `watermark_column`
- `row_limit`

## Recommended Starting Point

```json
{
  "id": "mysql-core-ledger",
  "adapter": "mysql",
  "enabled": true,
  "source_instance": "erp-eu-1",
  "database_name": "ledger",
  "include": {
    "ledger": ["invoices", "payments"]
  },
  "checkpoint_policy": "after_ack",
  "backfill_mode": "full",
  "batch_enabled": false,
  "connection": {
    "host": "127.0.0.1",
    "port": 3306,
    "username": "denotary",
    "database": "ledger"
  },
  "options": {
    "capture_mode": "watermark",
    "watermark_column": "updated_at",
    "commit_timestamp_column": "updated_at",
    "row_limit": 250
  }
}
```

## Notes

- tables must have a primary key
- tracked tables must contain the configured watermark column
- binlog CDC is the next MySQL-specific step after this baseline
