# Oracle Config Reference

This document describes Oracle-specific configuration.

## Current Status

Oracle is still a scaffold adapter.

Declared target path:

- snapshot support
- redo / LogMiner compatible capture

## `connection`

Expected keys:

- `host`
- `port`
- `username`
- `service_name`
- optional `password`

## `options`

Planned target path:

- `capture_mode`
- LogMiner or approved CDC abstraction settings

## Notes

- capability metadata currently declares:
  - `capture_modes = ("snapshot", "logminer")`
