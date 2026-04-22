# MySQL Config Reference

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

This document describes MySQL-specific `connection` and `options` fields.

## Current Status

MySQL currently supports both a live watermark/snapshot baseline and a live row-based binlog CDC baseline.

Implemented now:

- live connection validation
- tracked table introspection
- watermark-based snapshot polling
- row-based binlog CDC baseline
- live Docker-backed binlog validation
- deterministic checkpoint resume
- bootstrap / inspect / runtime signature

Still next:

- higher-volume MySQL-specific binlog soak and recovery passes

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
  - supported values: `"watermark"` and `"binlog"`
- `watermark_column`
  - default: `"updated_at"`
- `commit_timestamp_column`
  - default: same as `watermark_column`
- `row_limit`
- `binlog_server_id`
  - default: `1001`
- `binlog_blocking`
  - default: `false`
- `binlog_start_file`
  - optional explicit starting log file
- `binlog_start_pos`
  - optional explicit starting log position, default: `4`

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
- `capture_mode = "binlog"` expects:
  - `log_bin = ON`
  - `binlog_format = ROW`
  - `binlog_row_image = FULL`
- the current binlog path is a shared native CDC baseline for both `MySQL` and `MariaDB`
