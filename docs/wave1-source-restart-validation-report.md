# Wave 1 Source Restart Validation Report

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

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

### Oracle

- mode:
  - `logminer`
- result:
  - baseline pass: `0 processed`
  - first batch after bootstrap: `3 processed`
  - second batch after `docker restart`: `3 processed`
  - total:
    - `delivery_count = 6`
    - `proof_count = 6`
- note:
  - the local Oracle harness now enables `ARCHIVELOG`, and the `LogMiner` adapter resumes across restart using archived redo plus current online redo

### MongoDB

- mode:
  - `change_streams`
- result:
  - baseline pass: `0 processed`
  - first batch after bootstrap: `3 processed`
  - second batch after `docker restart`: `3 processed`
  - total:
    - `delivery_count = 6`
    - `proof_count = 6`
- note:
  - the `change_streams` adapter now reopens stale streams from the saved resume token after source restart

## Outcome

Confirmed passing native-CDC restart drills:

- MySQL
- MariaDB
- SQL Server
- Oracle `logminer`
- MongoDB `change_streams`

This closes the Wave 1 source-restart matrix for the current local live harnesses.
