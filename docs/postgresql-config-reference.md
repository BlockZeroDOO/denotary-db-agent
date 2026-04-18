# PostgreSQL Config Reference

This document describes PostgreSQL-specific `connection` and `options` fields.

## `connection`

```json
"connection": {
  "host": "127.0.0.1",
  "port": 5432,
  "username": "denotary",
  "database": "ledger"
}
```

### Supported keys

- `host`: `string`, required for live PostgreSQL use
- `port`: `integer`, typical value `5432`
- `username`: `string`, required for live PostgreSQL use
- `database`: `string`, required for live PostgreSQL use

## `options`

### Core capture settings

- `capture_mode`
  - values: `"watermark"`, `"trigger"`, `"logical"`
- `watermark_column`
- `commit_timestamp_column`
- `cleanup_processed_events`
- `row_limit`

### Logical replication settings

- `slot_name`
- `output_plugin`
  - `"test_decoding"` or `"pgoutput"`
- `logical_runtime_mode`
  - `"stream"` or `"peek"`
- `publication_name`
- `auto_create_slot`
- `auto_create_publication`
- `replica_identity_full`
- `logical_wait_poll_sec`
- `logical_stream_timeout_sec`
- `logical_stream_idle_timeout_sec`

### Stream reconnect, backoff, and fallback

- `logical_stream_reconnect_base_delay_sec`
- `logical_stream_reconnect_max_delay_sec`
- `logical_stream_error_history_size`
- `logical_stream_fallback_failure_threshold`
- `logical_stream_fallback_sec`
- `logical_stream_probation_sec`

### Logical warning thresholds

- `logical_warn_retained_wal_bytes`
- `logical_warn_flush_lag_bytes`

## Recommended Starting Point

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

## Notes

- `inspect` and `refresh` are live operations
- PostgreSQL is the most mature adapter today
- live modes currently include watermark, trigger CDC, and logical `pgoutput`
