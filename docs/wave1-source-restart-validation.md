# Wave 1 Source Restart Validation

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

This runbook defines a reproducible source-restart drill for the Wave 1 adapters:

- MySQL
- MariaDB
- SQL Server
- Oracle
- MongoDB

The drill purpose is narrow and practical:

- prove that the adapter can continue after a source container restart
- prove that multi-event native CDC capture still works both before and after restart
- keep the validation local, without spending mainnet entitlement

## Drill Shape

For each adapter:

1. start the live Docker harness
2. configure the native CDC mode
3. bootstrap the source
4. run the initial baseline pass if the adapter requires it
5. insert three fresh events
6. run the agent and confirm they are exported
7. restart the source container
8. wait for the source to become ready again
9. insert three more fresh events
10. run the agent again and confirm they are exported

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

- [scripts/run-wave1-source-restart-validation.ps1](../scripts/run-wave1-source-restart-validation.ps1)
- [scripts/run-wave1-source-restart-validation.sh](../scripts/run-wave1-source-restart-validation.sh)

The underlying Python driver is:

- [scripts/run-wave1-source-restart-validation.py](../scripts/run-wave1-source-restart-validation.py)

You can also run one adapter at a time:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run-wave1-source-restart-validation.ps1 -Adapter mysql
powershell -ExecutionPolicy Bypass -File scripts/run-wave1-source-restart-validation.ps1 -Adapter mariadb
powershell -ExecutionPolicy Bypass -File scripts/run-wave1-source-restart-validation.ps1 -Adapter sqlserver
powershell -ExecutionPolicy Bypass -File scripts/run-wave1-source-restart-validation.ps1 -Adapter oracle
powershell -ExecutionPolicy Bypass -File scripts/run-wave1-source-restart-validation.ps1 -Adapter mongodb
```
