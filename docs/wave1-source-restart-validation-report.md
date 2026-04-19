# Wave 1 Source Restart Validation Report

This report captures the current state of the native-CDC source restart drill for the Wave 1 adapters.

Validation date:

- `2026-04-19`

Execution mode:

- adapter-by-adapter
- local live Docker harnesses
- local mocked off-chain services

## Passing Adapters

### MySQL

- mode:
  - `binlog`
- result:
  - first batch after bootstrap: `3 processed`
  - second batch after `docker restart`: `3 processed`
  - total:
    - `delivery_count = 6`
    - `proof_count = 6`

### MariaDB

- mode:
  - `binlog`
- result:
  - first batch after bootstrap: `3 processed`
  - second batch after `docker restart`: `3 processed`
  - total:
    - `delivery_count = 6`
    - `proof_count = 6`

### SQL Server

- mode:
  - `change_tracking`
- result:
  - baseline pass: `0 processed`
  - first batch after bootstrap: `3 processed`
  - second batch after `docker restart`: `3 processed`
  - total:
    - `delivery_count = 6`
    - `proof_count = 6`

## Current Gaps

### Oracle

- mode:
  - `logminer`
- current result:
  - first post-bootstrap batch succeeds
  - second batch after `docker restart` fails
- observed failure:
  - `ValueError: oracle LogMiner checkpoint is older than the current online redo window; bootstrap a fresh source or reset the source checkpoint`
- interpretation:
  - current `LogMiner` baseline does not yet recover cleanly across this restart drill when the saved SCN is no longer covered by the currently available online redo window

### MongoDB

- mode:
  - `change_streams`
- current result:
  - adapter-specific restart drill does not complete within the current validation timeout window
- interpretation:
  - restart recovery for the current `change_streams` path still needs targeted debugging before it can be called stable in the same way as MySQL, MariaDB, and SQL Server

## Outcome

Confirmed passing native-CDC restart drills:

- MySQL
- MariaDB
- SQL Server

Confirmed open restart-recovery gaps:

- Oracle `logminer`
- MongoDB `change_streams`

This means Wave 1 restart recovery is partially validated, but not yet closed across all native CDC adapters.
