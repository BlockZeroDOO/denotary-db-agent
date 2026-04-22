# Wave 1 Long Soak Validation Report

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

This report captures the first sustained long-soak pass for the Wave 1 non-PostgreSQL adapters.

Validation command:

```powershell
C:\Python39\python.exe scripts\run-wave1-long-soak-validation.py
```

Validation profile:

- `10` cycles
- `5` fresh events per cycle
- `50` processed events per adapter
- `0` failed events
- `0` DLQ growth
- `50` deliveries
- `50` proofs

Results:

- MySQL `binlog`:
  - `50 processed`
  - `0 failed`
  - `50 deliveries`
  - `50 proofs`
  - `0 dlq`
- MariaDB `binlog`:
  - `50 processed`
  - `0 failed`
  - `50 deliveries`
  - `50 proofs`
  - `0 dlq`
- SQL Server `change_tracking`:
  - `50 processed`
  - `0 failed`
  - `50 deliveries`
  - `50 proofs`
  - `0 dlq`
- Oracle `logminer`:
  - `50 processed`
  - `0 failed`
  - `50 deliveries`
  - `50 proofs`
  - `0 dlq`
- MongoDB `change_streams`:
  - `50 processed`
  - `0 failed`
  - `50 deliveries`
  - `50 proofs`
  - `0 dlq`

Conclusion:

- the Wave 1 non-PostgreSQL adapters passed the first bounded long-soak profile
- native CDC paths remained stable through repeated cycles without DLQ growth
