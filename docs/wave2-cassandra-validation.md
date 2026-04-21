# Wave 2 Apache Cassandra Validation

This document describes the first deeper local validation layer for the `Wave 2` `Apache Cassandra` adapter.

Current scope:

- cold restart recovery through a new agent process on the same local state store
- bounded short-soak under sustained row creation in a Docker-backed local Cassandra instance

The validation uses:

- the real `Cassandra` adapter
- a real local Docker-backed `Cassandra` source
- local mock `Ingress`, `Watcher`, `Receipt`, `Audit`, and `Chain` services
- full proof export through the normal `AgentEngine` path

## Commands

PowerShell:

```powershell
scripts/run-wave2-cassandra-validation.ps1 -Mode restart
scripts/run-wave2-cassandra-validation.ps1 -Mode short-soak -Cycles 5 -EventsPerCycle 3
```

Shell:

```bash
scripts/run-wave2-cassandra-validation.sh restart
scripts/run-wave2-cassandra-validation.sh short-soak 5 3
```

Python:

```bash
python scripts/run-wave2-cassandra-validation.py --mode restart
python scripts/run-wave2-cassandra-validation.py --mode short-soak --cycles 5 --events-per-cycle 3
```

## Restart Drill

The restart drill validates that the adapter resumes cleanly after a cold agent restart while continuing to use the same `state_db`.

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

The short-soak validation applies bounded sustained load to the `Cassandra` adapter.

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

- the `Cassandra` baseline handles repeated polling cleanly
- restart recovery through a new agent process works with the same checkpoint store
- the adapter now has local validation depth beyond basic live ping and full-cycle proof export
