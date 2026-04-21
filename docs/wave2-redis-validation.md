# Wave 2 Redis Validation

This document describes the first deeper validation layer for the `Wave 2` `Redis` adapter.

Current scope:

- source restart recovery
- bounded short-soak under sustained key creation

The validation uses:

- the Docker-backed Redis live harness
- the real `Redis` adapter in `capture_mode = "scan"`
- local mock `Ingress`, `Watcher`, `Receipt`, `Audit`, and `Chain` services
- full proof export through the normal `AgentEngine` path

## Commands

PowerShell:

```powershell
scripts/run-wave2-redis-validation.ps1 -Mode restart
scripts/run-wave2-redis-validation.ps1 -Mode short-soak -Cycles 5 -EventsPerCycle 3
```

Shell:

```bash
scripts/run-wave2-redis-validation.sh restart
scripts/run-wave2-redis-validation.sh short-soak 5 3
```

Python:

```bash
python scripts/run-wave2-redis-validation.py --mode restart
python scripts/run-wave2-redis-validation.py --mode short-soak --cycles 5 --events-per-cycle 3
```

## Restart Drill

The restart drill validates that the adapter can recover cleanly after the Redis container restarts.

Expected flow:

1. baseline `run_once()` processes `0` events
2. first write window inserts `3` keys and `run_once()` processes `3`
3. Redis restarts
4. second write window inserts `3` new keys and `run_once()` processes `3`
5. total state ends with:
   - `delivery_count = 6`
   - `proof_count = 6`
   - `dlq_count = 0`

## Short Soak

The short-soak validation applies bounded sustained load to the Redis adapter.

Default profile:

- `5` cycles
- `3` keys per cycle
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

- the baseline `Redis` snapshot path is not only functional, but stable under simple restart and short sustained load
- proof export remains consistent under repeated polling cycles
- the adapter is moving from baseline coverage toward the same validation depth already achieved in `Wave 1`
