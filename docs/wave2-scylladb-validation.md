# Wave 2 ScyllaDB Validation

This document describes the next validation layer prepared for the `Wave 2` `ScyllaDB` adapter.

Current scope:

- cold agent restart recovery against a live ScyllaDB table
- bounded short-soak under sustained row creation

This layer is Docker-backed through the local ScyllaDB harness and uses the same environment variables as the live integration suite.

The validation uses:

- the real `ScyllaDB` adapter
- a real local ScyllaDB node running in Docker
- a temporary table created for the run
- local mock `Ingress`, `Watcher`, `Receipt`, `Audit`, and `Chain` services
- full proof export through the normal `AgentEngine` path

## Required Environment

At minimum:

- `DENOTARY_SCYLLADB_HOST`
- `DENOTARY_SCYLLADB_PORT`
- `DENOTARY_SCYLLADB_KEYSPACE`

Optional:

- `DENOTARY_SCYLLADB_USERNAME`
- `DENOTARY_SCYLLADB_PASSWORD`

The PowerShell and shell wrappers bootstrap these values automatically for the local Docker harness.

## Commands

PowerShell:

```powershell
scripts/run-wave2-scylladb-validation.ps1 -Mode restart
scripts/run-wave2-scylladb-validation.ps1 -Mode short-soak -Cycles 5 -EventsPerCycle 3
```

Shell:

```bash
scripts/run-wave2-scylladb-validation.sh restart
scripts/run-wave2-scylladb-validation.sh short-soak 5 3
```

Python:

```bash
python scripts/run-wave2-scylladb-validation.py --mode restart
python scripts/run-wave2-scylladb-validation.py --mode short-soak --cycles 5 --events-per-cycle 3
```

If the required environment is not present, the Python entrypoint exits cleanly with a `skipped` payload instead of failing.

## Restart Drill

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

## Interpretation

Once this Docker-backed layer is executed successfully, it closes the next practical readiness gap for `ScyllaDB` beyond:

- snapshot baseline
- local full-cycle proof export
- Docker-backed live snapshot/resume validation
