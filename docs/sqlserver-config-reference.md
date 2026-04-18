# SQL Server Config Reference

This document describes SQL Server-specific configuration.

## Current Status

SQL Server is still a scaffold adapter.

Declared target path:

- snapshot support
- CDC or Change Tracking runtime

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
- CDC-specific tracking settings
- checkpoint and table selection settings

## Notes

- capability metadata currently declares:
  - `capture_modes = ("snapshot", "cdc", "change_tracking")`
