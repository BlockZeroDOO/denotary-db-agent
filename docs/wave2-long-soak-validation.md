# Wave 2 Long-Soak Validation

This document defines the bounded long-soak profile for the active `Wave 2` adapters:

- Redis
- ScyllaDB
- IBM Db2
- Apache Cassandra
- Elasticsearch
- SQLite

The goal is to validate sustained multi-cycle processing on the local live harnesses that already back the `Wave 2` restart and short-soak paths.

## Profile

- `cycles = 10`
- `events_per_cycle = 5`
- expected total per adapter: `50` events

## Command

```powershell
C:\Python39\python.exe scripts\run-wave2-long-soak-validation.py --adapter all --cycles 10 --events-per-cycle 5 --output-root data\wave2-long-soak-validation-latest
```

PowerShell wrapper:

```powershell
scripts/run-wave2-long-soak-validation.ps1
```

POSIX wrapper:

```bash
scripts/run-wave2-long-soak-validation.sh
```

## Success Criteria

For each adapter:

- baseline run processes `0` events
- each cycle processes exactly `events_per_cycle`
- `total_processed = cycles * events_per_cycle`
- `total_failed = 0`
- `delivery_count = total_processed`
- `proof_count = total_processed`
- `dlq_count = 0`

## Artifacts

The unified harness writes:

- `summary.json`

Default output root:

- `data/wave2-long-soak-validation-latest`

## Adapter Notes

- `SQLite` is file-backed and does not require Docker.
- `Redis`, `ScyllaDB`, `IBM Db2`, `Apache Cassandra`, and `Elasticsearch` use their local Docker-backed validation paths.
- `ScyllaDB` and `Elasticsearch` are wrapped with local env/bootstrap handling so the long-soak suite remains fully reproducible from the repo.
