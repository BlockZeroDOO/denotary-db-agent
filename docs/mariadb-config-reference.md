# MariaDB Config Reference

This document describes MariaDB-specific configuration.

## Current Status

MariaDB now has both a live watermark/snapshot baseline and a live shared binlog CDC baseline.

Declared target path:

- watermark-based snapshot polling baseline
- shared binlog CDC baseline
- live MariaDB-specific binlog validation

Still next:

- MariaDB-specific soak and recovery passes on the shared binlog path

## `connection`

Expected keys:

- `host`
- `port`
- `username`
- `database`
- optional `password`

## `options`

Supported now:

- `capture_mode = "watermark"` or `"binlog"`
- `watermark_column`
- `commit_timestamp_column`
- `row_limit`
- `binlog_server_id`
- `binlog_blocking`
- `binlog_start_file`
- `binlog_start_pos`

## Notes

- MariaDB currently shares the same watermark and binlog baseline shape as MySQL
- `include` should list explicit tracked tables
- each tracked table must expose:
  - a primary key
  - the configured `watermark_column`
  - the configured `commit_timestamp_column`
- `capture_mode = "binlog"` expects row-based binlog settings equivalent to the MySQL baseline:
  - `log_bin = ON`
  - `binlog_format = ROW`
  - `binlog_row_image = FULL`
