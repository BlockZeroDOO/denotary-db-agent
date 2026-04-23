# Wave 1 Readiness Matrix

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

This document is the compact readiness view for the Wave 1 adapters:

- PostgreSQL
- MySQL
- MariaDB
- SQL Server
- Oracle
- MongoDB

It complements the detailed validation reports and is intended to answer one practical question quickly:

- what is already validated
- on which layer it was validated
- where validation depth differs across the current Wave 1 set

For the combined rollout view across both implementation waves, see:

- [wave-readiness-summary.md](wave-readiness-summary.md)

## Legend

- `yes`: confirmed
- `shared`: confirmed through a shared pipeline-level drill rather than a source-specific drill
- `partial`: validated, but not with the same depth as the strongest Wave 1 paths

## Matrix

| Adapter | Native CDC path | Local full cycle | Mainnet happy path | Source restart | Short soak | Long soak | Local service outage | Mainnet degraded-service | Overall Wave 1 status |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| PostgreSQL | `trigger`, `logical`, `pgoutput stream` | yes | yes | yes | yes on `Jungle4` / testnet | partial | shared | shared | strong |
| MySQL | `binlog` | yes | yes | yes | yes | yes | shared | shared | strong |
| MariaDB | `binlog` | yes | yes | yes | yes | yes | shared | shared | strong |
| SQL Server | `change_tracking` | yes | yes | yes | yes | yes | shared | shared | strong |
| Oracle | `logminer` | yes | yes | yes | yes | yes | shared | shared | strong |
| MongoDB | `change_streams` | yes | yes | yes | yes | yes | shared | shared | strong |

## Notes

- `Local service outage` is a pipeline-level validation for:
  - `ingress`
  - `watcher`
  - `receipt`
  - `audit`
- `Mainnet degraded-service` is also pipeline-level:
  - it uses the real `denotary` mainnet chain/signer path
  - it uses synthetic `dry_run_events` so the source database is not the moving part
- PostgreSQL already has the deepest validation history:
  - real `denotary` mainnet
  - stable `Jungle4`
  - native CDC recovery work
- The non-PostgreSQL Wave 1 adapters now have a consistent validation ladder:
  - local full-cycle
  - mainnet happy path
  - source restart recovery
  - short soak
  - long soak

## Supporting Reports

- [denotary-postgresql-validation-report.md](denotary-postgresql-validation-report.md)
- [denotary-wave1-mainnet-validation-report.md](denotary-wave1-mainnet-validation-report.md)
- [wave1-recovery-validation-report.md](wave1-recovery-validation-report.md)
- [wave1-source-restart-validation-report.md](wave1-source-restart-validation-report.md)
- [wave1-short-soak-validation-report.md](wave1-short-soak-validation-report.md)
- [wave1-long-soak-validation-report.md](wave1-long-soak-validation-report.md)
- [wave1-service-outage-validation-report.md](wave1-service-outage-validation-report.md)
- [wave1-mainnet-service-outage-validation-report.md](wave1-mainnet-service-outage-validation-report.md)
