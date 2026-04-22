# Oracle Config Reference

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

This document describes Oracle-specific configuration.

## Current Status

Oracle now has:

- a live Docker-backed watermark/snapshot baseline
- a live `logminer` CDC baseline with root-admin redo mining
- local full-cycle proof export coverage for both `watermark` and `logminer`

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
- `capture_mode = "logminer"`
- `watermark_column`
- `commit_timestamp_column`
- `row_limit`

Additional `logminer` settings:

- `connection.admin_username`
- `connection.admin_password`
- `connection.admin_service_name`
  or `options.logminer_root_service_name`

## Notes

- current baseline expects explicit tracked tables in `include`
- tracked tables must have:
  - a primary key
  - the configured `watermark_column`
  - the configured `commit_timestamp_column`
- for `capture_mode = "logminer"`:
  - the source tables still live in the PDB/app connection
  - LogMiner mining runs through a separate root-admin connection
  - the admin connection must target `CDB$ROOT`
  - supplemental logging must be enabled at the database level
  - online redo members must be discoverable from the admin connection
  - the current baseline mines the recent online redo window and resumes from an SCN checkpoint
  - when no checkpoint exists yet, the first CDC pass establishes a current-SCN baseline and starts mining only new changes after that point
  - if historical table backfill is required first, use the `watermark` mode baseline before switching the source to `logminer`
  - if the saved checkpoint falls outside the online redo window, the source must be re-bootstrapped or its checkpoint reset
- live runtime uses `python-oracledb` in thin mode
