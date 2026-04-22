# Wave 1 Short Soak Validation Report

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

This report captures one reproducible sequential short-soak run for the Wave 1 non-PostgreSQL adapters using the local live Docker harnesses and local mocked off-chain services.

Run command:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run-wave1-short-soak-validation.ps1
```

Validation date:

- `2026-04-19`

## Result

The full Wave 1 short-soak run completed successfully.

### MySQL

- mode:
  - `binlog`
- result:
  - `5` cycles x `3` events
  - `total_processed = 15`
  - `total_failed = 0`
  - `delivery_count = 15`
  - `proof_count = 15`
  - `dlq_count = 0`

### MariaDB

- mode:
  - `binlog`
- result:
  - `5` cycles x `3` events
  - `total_processed = 15`
  - `total_failed = 0`
  - `delivery_count = 15`
  - `proof_count = 15`
  - `dlq_count = 0`

### SQL Server

- mode:
  - `change_tracking`
- result:
  - `5` cycles x `3` events
  - `total_processed = 15`
  - `total_failed = 0`
  - `delivery_count = 15`
  - `proof_count = 15`
  - `dlq_count = 0`

### Oracle

- mode:
  - `logminer`
- result:
  - `5` cycles x `3` events
  - `total_processed = 15`
  - `total_failed = 0`
  - `delivery_count = 15`
  - `proof_count = 15`
  - `dlq_count = 0`

### MongoDB

- mode:
  - `change_streams`
- result:
  - `5` cycles x `3` events
  - `total_processed = 15`
  - `total_failed = 0`
  - `delivery_count = 15`
  - `proof_count = 15`
  - `dlq_count = 0`

## Aggregate Outcome

Total validated adapters:

- `5`

Total cycle count per adapter:

- `5`

Total events written per adapter:

- `15`

This confirms that the current Wave 1 adapters are not only mainnet-happy-path validated and restart-safe, but also stable across a bounded sustained local native-CDC load.
