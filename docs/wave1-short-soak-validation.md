# Wave 1 Short Soak Validation

This runbook defines a bounded sustained-load validation for the Wave 1 non-PostgreSQL adapters:

- MySQL
- MariaDB
- SQL Server
- Oracle
- MongoDB

The goal is not a long endurance test. The goal is a reproducible local short-soak that proves:

- the native CDC path remains stable across repeated capture loops
- the agent continues to export proofs without DLQ growth
- the local live harness remains healthy under a small sustained event stream

## Validation Shape

For each adapter:

1. start the live Docker harness
2. bootstrap the source in native CDC mode
3. run a baseline `run_once()`
4. write `5` cycles of fresh data
5. in each cycle, write `3` fresh events and immediately run the agent again
6. confirm:
   - `processed = 15`
   - `failed = 0`
   - `delivery_count = 15`
   - `proof_count = 15`
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

- [scripts/run-wave1-short-soak-validation.ps1](../scripts/run-wave1-short-soak-validation.ps1)
- [scripts/run-wave1-short-soak-validation.sh](../scripts/run-wave1-short-soak-validation.sh)

The underlying Python driver is:

- [scripts/run-wave1-short-soak-validation.py](../scripts/run-wave1-short-soak-validation.py)
