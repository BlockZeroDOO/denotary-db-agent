# SQL Server Config Reference

This document describes SQL Server-specific configuration.

## Current Status

SQL Server now has a live-ready watermark/snapshot baseline adapter.

Declared target path:

- watermark-based snapshot polling baseline
- SQL Server CDC or Change Tracking runtime next

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
- `watermark_column`
- `commit_timestamp_column`
- `row_limit`

Planned next:

- `capture_mode = "cdc"`
- `capture_mode = "change_tracking"`
- SQL Server-specific tracking settings

## Notes

- current baseline expects explicit tracked tables in `include`
- tracked tables must have:
  - a primary key
  - the configured `watermark_column`
  - the configured `commit_timestamp_column`
- live runtime uses `python-tds` for SQL Server connectivity
