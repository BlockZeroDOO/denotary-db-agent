# MongoDB Config Reference

This document describes MongoDB-specific configuration.

## Current Status

MongoDB now has a live Docker-backed watermark/snapshot baseline adapter with local full-cycle proof export coverage.

Declared target path:

- watermark-based snapshot polling baseline
- MongoDB change streams next

## `connection`

Expected keys:

- `uri`
- optional `server_selection_timeout_ms`
- optional `connect_timeout_ms`

## `options`

Supported now:

- `capture_mode = "watermark"`
- `watermark_column`
- `commit_timestamp_column`
- `primary_key_field`
- `row_limit`

Planned next:

- `capture_mode = "change_streams"`
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
