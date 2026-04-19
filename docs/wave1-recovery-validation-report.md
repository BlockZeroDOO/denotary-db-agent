# Wave 1 Recovery Validation Report

This report captures one reproducible sequential recovery-baseline run for the Wave 1 non-PostgreSQL adapters using the local live Docker harnesses.

Run command:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run-wave1-recovery-validation.ps1
```

Validation date:

- `2026-04-19`

## Result

The full Wave 1 recovery-baseline run completed successfully.

### MySQL

- suite:
  - `test_mysql_live_integration.py`
- result:
  - `6 OK`
- validated paths:
  - snapshot resume
  - schema drift runtime refresh
  - `backfill_mode = none`
  - native `binlog` CDC `insert / update / delete`

### MariaDB

- suite:
  - `test_mariadb_live_integration.py`
- result:
  - `3 OK`
- validated paths:
  - snapshot resume
  - `backfill_mode = none`
  - native `binlog` CDC `insert / update / delete`

### SQL Server

- suite:
  - `test_sqlserver_live_integration.py`
- result:
  - `5 OK`
- validated paths:
  - snapshot resume
  - schema drift runtime refresh
  - `backfill_mode = none`
  - native `change_tracking` CDC `insert / update / delete`

### Oracle

- suite:
  - `test_oracle_live_integration.py`
- result:
  - `6 OK`
- validated paths:
  - snapshot resume
  - schema drift runtime refresh
  - `backfill_mode = none`
  - `logminer` admin prerequisite checks
  - native `logminer` CDC `insert / update / delete`

### MongoDB

- suite:
  - `test_mongodb_live_integration.py`
- result:
  - `5 OK`
- validated paths:
  - snapshot resume
  - `backfill_mode = none`
  - ObjectId checkpoint normalization
  - native `change_streams` CDC `insert / update / delete`

## Aggregate Outcome

Total live recovery-baseline suites:

- `5`

Total passing test cases:

- `25`

This confirms that the current Wave 1 adapters are not only mainnet-happy-path validated, but also have a reproducible local recovery-sensitive baseline for:

- checkpoint resume
- schema/runtime refresh where supported
- source bootstrap/inspect correctness
- native CDC path correctness

## Related Docs

- [docs/denotary-wave1-mainnet-validation-report.md](denotary-wave1-mainnet-validation-report.md)
- [docs/wave1-recovery-validation.md](wave1-recovery-validation.md)
