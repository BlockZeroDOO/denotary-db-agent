# MariaDB Config Reference

This document describes MariaDB-specific configuration.

## Current Status

MariaDB is still a scaffold adapter.

Declared target path:

- snapshot support
- MariaDB binlog CDC profile

## `connection`

Expected keys:

- `host`
- `port`
- `username`
- `database`
- optional `password`

## `options`

Planned target path:

- `capture_mode`
- watermark baseline or binlog-specific settings

## Notes

- MariaDB shares high-level expectations with MySQL
- the current contract tests already enforce adapter shape and capability metadata
