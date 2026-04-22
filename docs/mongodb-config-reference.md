# MongoDB Config Reference

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

This document describes MongoDB-specific configuration.

## Current Status

MongoDB now has a live Docker-backed watermark/snapshot baseline adapter with local full-cycle proof export coverage.

Declared target path:

- watermark-based snapshot polling baseline
- MongoDB change streams

## `connection`

Expected keys:

- `uri`
- optional `server_selection_timeout_ms`
- optional `connect_timeout_ms`

## `options`

Supported now:

- `capture_mode = "watermark"`
- `capture_mode = "change_streams"`
- `watermark_column`
- `commit_timestamp_column`
- `primary_key_field`
- `row_limit`
- optional `change_stream_max_await_ms`

Planned next:

- MongoDB-specific change stream settings

## Notes

- current baseline expects explicit tracked collections in `include`
- `include` should use database-to-collection mapping, for example:
  - `"include": { "ledger": ["invoices", "payments"] }`
- each tracked collection must expose:
  - the configured `primary_key_field` (defaults to `_id`)
  - the configured `watermark_column`
  - the configured `commit_timestamp_column`
- current baseline emits full normalized documents in `after`
- `change_streams` mode requires a replica set or sharded deployment
- current change-stream implementation keeps source-side cursors alive across runtime loops for continuous daemon mode
