# SQL Server Config Reference

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

This document describes SQL Server-specific configuration.

## Current Status

SQL Server now has:

- a live Docker-backed watermark/snapshot baseline
- a live native `change_tracking` CDC baseline
- local full-cycle proof export coverage for both modes

## `connection`

Expected keys:

- `host`
- `port`
- `username`
- `database`
- optional `password`
- optional `login_timeout`
- optional `timeout`

## `options`

Supported now:

- `capture_mode = "watermark"`
- `capture_mode = "change_tracking"`
- `watermark_column`
- `commit_timestamp_column`
- `row_limit`
- optional SQL Server-specific tracking settings may be added later

## Notes

- current baseline expects explicit tracked tables in `include`
- tracked tables must have:
  - a primary key
  - the configured `watermark_column`
  - the configured `commit_timestamp_column`
- for `capture_mode = "change_tracking"`:
  - database-level `CHANGE_TRACKING` must be enabled
  - each tracked table must have `CHANGE_TRACKING` enabled
  - the agent consumes net row changes since the last synchronized change-tracking version
  - when multiple mutations happen to the same primary key before the next sync window, SQL Server exposes the net result rather than every intermediate row image
- live runtime uses `python-tds` for SQL Server connectivity
