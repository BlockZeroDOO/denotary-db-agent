# Wave 1 Service Outage Validation Report

This report captures one reproducible local run of temporary off-chain service outage recovery using mocked `ingress / watcher / receipt / audit` services.

Run command:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run-wave1-service-outage-validation.ps1
```

Validation date:

- `2026-04-19`

## Result

The service outage recovery drill completed successfully for all target scenarios.

### Ingress Prepare Outage

- first run:
  - `processed = 0`
  - `failed = 1`
- second run:
  - `processed = 1`
  - `failed = 0`
- resulting state:
  - `delivery_count = 2`
  - `proof_count = 1`
  - `dlq_count = 1`

### Watcher Register Outage

- first run:
  - `processed = 0`
  - `failed = 1`
- second run:
  - `processed = 1`
  - `failed = 0`
- resulting state:
  - `delivery_count = 2`
  - `proof_count = 1`
  - `dlq_count = 1`

### Receipt Fetch Outage

- first run:
  - `processed = 0`
  - `failed = 1`
- second run:
  - `processed = 1`
  - `failed = 0`
- resulting state:
  - `delivery_count = 2`
  - `proof_count = 1`
  - `dlq_count = 1`

### Audit Fetch Outage

- first run:
  - `processed = 0`
  - `failed = 1`
- second run:
  - `processed = 1`
  - `failed = 0`
- resulting state:
  - `delivery_count = 2`
  - `proof_count = 1`
  - `dlq_count = 1`

## Outcome

This confirms that the current pipeline can recover from temporary outages in the off-chain service layer by rerunning the source loop, without requiring source-side restart or a fresh bootstrap.

The current recovery model is explicit:

- the first failed attempt is recorded in `DLQ`
- the next source loop reprocesses the same event
- proof export succeeds on the recovered pass
