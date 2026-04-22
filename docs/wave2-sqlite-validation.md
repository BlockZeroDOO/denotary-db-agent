# Wave 2 SQLite Validation

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

This document describes the first edge-focused validation layer for the `Wave 2` `SQLite` adapter.

Current scope:

- cold restart recovery through a new agent process on the same local state store
- bounded short-soak under sustained row creation in a file-backed SQLite database

The validation uses:

- the real `SQLite` adapter
- a real file-backed SQLite database
- local mock `Ingress`, `Watcher`, `Receipt`, `Audit`, and `Chain` services
- full proof export through the normal `AgentEngine` path

## Commands

PowerShell:

```powershell
scripts/run-wave2-sqlite-validation.ps1 -Mode restart
scripts/run-wave2-sqlite-validation.ps1 -Mode short-soak -Cycles 5 -EventsPerCycle 3
```

Shell:

```bash
scripts/run-wave2-sqlite-validation.sh restart
scripts/run-wave2-sqlite-validation.sh short-soak 5 3
```

Python:

```bash
python scripts/run-wave2-sqlite-validation.py --mode restart
python scripts/run-wave2-sqlite-validation.py --mode short-soak --cycles 5 --events-per-cycle 3
```

## Restart Drill

The restart drill validates that the adapter can resume cleanly after a cold agent restart while continuing to use the same `state_db`.

Expected flow:

1. baseline `run_once()` processes `0` events
2. first write window inserts `3` rows and `run_once()` processes `3`
3. the first `AgentEngine` instance closes
4. a new `AgentEngine` instance starts against the same config and state store
5. second write window inserts `3` more rows and `run_once()` processes `3`
6. total state ends with:
   - `delivery_count = 6`
   - `proof_count = 6`
   - `dlq_count = 0`

## Short Soak

The short-soak validation applies bounded sustained load to the SQLite adapter.

Default profile:

- `5` cycles
- `3` rows per cycle
- `15` expected total events

Expected result:

- `baseline_processed = 0`
- `total_processed = cycles * events_per_cycle`
- `total_failed = 0`
- `delivery_count = total_processed`
- `proof_count = total_processed`
- `dlq_count = 0`

## Current Interpretation

Passing this validation means:

- the file-backed `SQLite` baseline handles repeated polling cleanly
- restart recovery through a new agent process works with the same checkpoint store
- the adapter is suitable for edge and embedded scenarios where periodic restart and bounded local proof export matter more than continuous CDC
