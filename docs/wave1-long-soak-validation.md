# Wave 1 Long Soak Validation

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

This runbook defines a heavier sustained-load validation for the Wave 1 non-PostgreSQL adapters:

- MySQL
- MariaDB
- SQL Server
- Oracle
- MongoDB

The goal is to go beyond the bounded short-soak and confirm that the native CDC path remains stable through a larger repeated event stream without DLQ growth.

## Validation Shape

For each adapter:

1. start the live Docker harness
2. bootstrap the source in native CDC mode
3. run a baseline `run_once()`
4. write `10` cycles of fresh data
5. in each cycle, write `5` fresh events and immediately run the agent again
6. confirm:
   - `processed = 50`
   - `failed = 0`
   - `delivery_count = 50`
   - `proof_count = 50`
   - `dlq_count = 0`

## Native CDC Modes Used

- MySQL:
  - `binlog`
- MariaDB:
  - `binlog`
- SQL Server:
  - `change_tracking`
- Oracle:
  - `logminer`
- MongoDB:
  - `change_streams`

## Command

Use one of:

- [scripts/run-wave1-long-soak-validation.ps1](../scripts/run-wave1-long-soak-validation.ps1)
- [scripts/run-wave1-long-soak-validation.sh](../scripts/run-wave1-long-soak-validation.sh)

The underlying Python driver is:

- [scripts/run-wave1-long-soak-validation.py](../scripts/run-wave1-long-soak-validation.py)

You can also run one adapter at a time:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run-wave1-long-soak-validation.ps1 -Adapter mysql
powershell -ExecutionPolicy Bypass -File scripts/run-wave1-long-soak-validation.ps1 -Adapter mariadb
powershell -ExecutionPolicy Bypass -File scripts/run-wave1-long-soak-validation.ps1 -Adapter sqlserver
powershell -ExecutionPolicy Bypass -File scripts/run-wave1-long-soak-validation.ps1 -Adapter oracle
powershell -ExecutionPolicy Bypass -File scripts/run-wave1-long-soak-validation.ps1 -Adapter mongodb
```
