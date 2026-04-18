# MariaDB Config Reference

This document describes MariaDB-specific configuration.

## Current Status

MariaDB now has a live watermark/snapshot baseline.

Declared target path:

- watermark-based snapshot polling baseline
- MariaDB binlog CDC profile next

## `connection`

Expected keys:

- `host`
- `port`
- `username`
- `database`
- optional `password`

## `options`

Supported now:

- `capture_mode = "watermark"`
- `watermark_column`
- `commit_timestamp_column`
- `row_limit`

Planned next:

- MariaDB-specific binlog settings

## Notes

- MariaDB currently shares the same watermark baseline shape as MySQL
- `include` should list explicit tracked tables
- each tracked table must expose:
  - a primary key
  - the configured `watermark_column`
  - the configured `commit_timestamp_column`
