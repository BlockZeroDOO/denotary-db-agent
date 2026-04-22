# Wave 1 Recovery Validation

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

This runbook defines the short recovery baseline for the Wave 1 adapters beyond PostgreSQL:

- MySQL
- MariaDB
- SQL Server
- Oracle
- MongoDB

The goal is not long soak duration. The goal is a reproducible live-harness validation that the adapter still behaves correctly when exercising the recovery-relevant paths already built into the current baseline:

- snapshot resume after stored checkpoints
- `backfill_mode = none` guard behavior
- runtime signature refresh after schema drift where supported
- native CDC path capture where supported:
  - MySQL / MariaDB `binlog`
  - SQL Server `change_tracking`
  - Oracle `logminer`
  - MongoDB `change_streams`

## Scope

This validation uses the local live Docker harnesses and does not spend mainnet entitlement.

It complements, rather than replaces:

- [docs/denotary-wave1-mainnet-validation-report.md](docs/denotary-wave1-mainnet-validation-report.md)

Mainnet validation proves the happy path against real `denotary`.
This runbook proves that the local recovery-sensitive adapter paths stay green and reproducible.

## Recovery Baseline Command

Use one of:

- [scripts/run-wave1-recovery-validation.ps1](../scripts/run-wave1-recovery-validation.ps1)
- [scripts/run-wave1-recovery-validation.sh](../scripts/run-wave1-recovery-validation.sh)

Both scripts run the live integration suites sequentially for:

- `test_mysql_live_integration.py`
- `test_mariadb_live_integration.py`
- `test_sqlserver_live_integration.py`
- `test_oracle_live_integration.py`
- `test_mongodb_live_integration.py`

Sequential execution matters because each suite manages its own Docker-backed database lifecycle.

## What Each Adapter Validates

### MySQL

- snapshot resume after checkpoint
- runtime signature refresh after schema drift
- `backfill_mode = none`
- native row-based `binlog` CDC `insert / update / delete`

### MariaDB

- snapshot resume after checkpoint
- `backfill_mode = none`
- native row-based `binlog` CDC `insert / update / delete`

### SQL Server

- snapshot resume after checkpoint
- runtime signature refresh after schema drift
- `backfill_mode = none`
- native `change_tracking` CDC `insert / update / delete`

### Oracle

- snapshot resume after checkpoint
- runtime signature refresh after schema drift
- `backfill_mode = none`
- `logminer` bootstrap/admin prerequisite checks
- native `logminer` CDC `insert / update / delete`

### MongoDB

- snapshot resume after checkpoint
- `backfill_mode = none`
- ObjectId checkpoint normalization
- native `change_streams` CDC `insert / update / delete`

## Expected Outcome

The run is considered healthy when:

- every live suite passes
- no adapter remains stuck on bootstrap/inspect after its harness starts
- native CDC suites emit the expected `insert / update / delete` events
- snapshot resume tests confirm checkpoint continuity

## Next Layer After This Runbook

After this baseline is green, the next useful validation layer is:

- short sustained load runs per adapter
- source restart drills
- temporary off-chain service outage drills
- backlog catch-up after source availability returns
