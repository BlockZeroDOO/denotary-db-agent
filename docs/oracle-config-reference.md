# Oracle Config Reference

This document describes Oracle-specific configuration.

## Current Status

Oracle now has a live Docker-backed watermark/snapshot baseline adapter with local full-cycle proof export coverage.

Declared target path:

- watermark-based snapshot polling baseline
- redo / LogMiner compatible capture next

## `connection`

Expected keys:

- `host`
- `port`
- `username`
- `service_name`
- optional `password`
- optional `tcp_connect_timeout`

## `options`

Supported now:

- `capture_mode = "watermark"`
- `watermark_column`
- `commit_timestamp_column`
- `row_limit`

Planned next:

- `capture_mode = "logminer"`
- Oracle-specific redo / LogMiner settings

## Notes

- current baseline expects explicit tracked tables in `include`
- tracked tables must have:
  - a primary key
  - the configured `watermark_column`
  - the configured `commit_timestamp_column`
- live runtime uses `python-oracledb` in thin mode
