# Wave 2 Local Service-Outage Validation

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

This document describes the current local service-outage validation layer for the `Wave 2` adapters that already have repeatable local validation harnesses:

- `SQLite`
- `Redis`
- `IBM Db2`
- `Apache Cassandra`
- `ScyllaDB`
- `Elasticsearch`

The goal is to validate recovery when the source remains healthy but part of the off-chain notarization pipeline is temporarily unavailable.

Current scope:

- temporary `Ingress` prepare outage
- temporary `Watcher` register outage
- temporary `Receipt` fetch outage
- temporary `Audit` fetch outage

The validation uses:

- the real `SQLite` and `Redis` adapters
- a real file-backed SQLite database
- a real Docker-backed Redis instance
- local Docker-backed `Db2`, `Cassandra`, `ScyllaDB`, and `Elasticsearch` environments
- local mock `Ingress`, `Watcher`, `Receipt`, `Audit`, and `Chain` services
- the normal `AgentEngine` delivery / receipt / proof-export path

## Commands

PowerShell:

```powershell
scripts/run-wave2-service-outage-validation.ps1 -Adapter all
scripts/run-wave2-service-outage-validation.ps1 -Adapter sqlite
scripts/run-wave2-service-outage-validation.ps1 -Adapter redis
```

Shell:

```bash
scripts/run-wave2-service-outage-validation.sh all
scripts/run-wave2-service-outage-validation.sh sqlite
scripts/run-wave2-service-outage-validation.sh redis
```

Python:

```bash
python scripts/run-wave2-service-outage-validation.py --adapter all
python scripts/run-wave2-service-outage-validation.py --adapter sqlite
python scripts/run-wave2-service-outage-validation.py --adapter redis
```

## Expected Recovery Pattern

For each adapter and outage scenario:

1. baseline `run_once()` processes `0` events
2. one source event is inserted while one off-chain component is configured to fail
3. the first `run_once()` returns:
   - `processed = 0`
   - `failed = 1`
4. the fault budget is exhausted and the same source event is retried
5. the second `run_once()` returns:
   - `processed = 1`
   - `failed = 0`
6. the source ends with at least one exported proof and no data loss

This validates that the event is not checkpointed before successful notarization and that a repeat `run_once()` can recover cleanly after a short outage.

## Artifact

The harness writes:

- `summary.json`

Default output root:

- `data/wave2-service-outage-validation-latest`

## Current Interpretation

Passing this validation means:

- the active `Wave 2` adapters can tolerate short-lived failures in the off-chain notarization pipeline
- the retry path preserves enough local state to complete proof export on the next run
- the current `Wave 2` local story is stronger than baseline-only snapshot validation

It does not yet mean:

- long outage durability is fully characterized
- mainnet degraded-service behavior is validated
- every future `Wave 2` adapter will automatically inherit the same outage coverage
