# Wave 2 Elasticsearch Validation

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

This document describes the next validation layer prepared for the `Wave 2` `Elasticsearch` adapter.

Current scope:

- cold agent restart recovery against a live Elasticsearch index
- bounded short-soak under sustained document creation

This layer is env-gated and requires a reachable Elasticsearch environment through the same connection variables already used by the live integration harness.

The validation uses:

- the real `Elasticsearch` adapter
- a real Elasticsearch cluster or single-node deployment
- a temporary index created for the run
- local mock `Ingress`, `Watcher`, `Receipt`, `Audit`, and `Chain` services
- full proof export through the normal `AgentEngine` path

## Required Environment

At minimum:

- `DENOTARY_ELASTICSEARCH_URL`

Optional:

- `DENOTARY_ELASTICSEARCH_USERNAME`
- `DENOTARY_ELASTICSEARCH_PASSWORD`
- `DENOTARY_ELASTICSEARCH_VERIFY_CERTS`

## Commands

PowerShell:

```powershell
scripts/run-wave2-elasticsearch-validation.ps1 -Mode restart
scripts/run-wave2-elasticsearch-validation.ps1 -Mode short-soak -Cycles 5 -EventsPerCycle 3
```

Shell:

```bash
scripts/run-wave2-elasticsearch-validation.sh restart
scripts/run-wave2-elasticsearch-validation.sh short-soak 5 3
```

Python:

```bash
python scripts/run-wave2-elasticsearch-validation.py --mode restart
python scripts/run-wave2-elasticsearch-validation.py --mode short-soak --cycles 5 --events-per-cycle 3
```

If the required environment is not present, the script exits cleanly with a `skipped` payload instead of failing.

## Restart Drill

Expected flow:

1. baseline `run_once()` processes `0` events
2. first write window indexes `3` documents and `run_once()` processes `3`
3. the first `AgentEngine` instance closes
4. a new `AgentEngine` instance starts against the same config and state store
5. second write window indexes `3` more documents and `run_once()` processes `3`
6. total state ends with:
   - `delivery_count = 6`
   - `proof_count = 6`
   - `dlq_count = 0`

## Short Soak

Default profile:

- `5` cycles
- `3` documents per cycle
- `15` expected total events

Expected result:

- `baseline_processed = 0`
- `total_processed = cycles * events_per_cycle`
- `total_failed = 0`
- `delivery_count = total_processed`
- `proof_count = total_processed`
- `dlq_count = 0`

## Interpretation

Once this env-gated layer is executed in a real environment, it will close the next practical readiness gap for `Elasticsearch` beyond:

- snapshot baseline
- local full-cycle proof export
- env-gated live ping and tracked-index introspection
