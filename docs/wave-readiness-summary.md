# Wave 1 and Wave 2 Readiness Summary

This document is the compact rollout-level view across both implementation waves in `denotary-db-agent`.

For the stakeholder-facing short version, see:

- [wave-rollout-pack.md](wave-rollout-pack.md)

It is meant to answer three questions quickly:

- which adapters are already validated deeply enough to treat as strong production candidates
- which adapters are already anchored on real `denotary` mainnet
- where the remaining validation depth is still uneven

## Executive View

- `Wave 1` is operationally mature.
- `Wave 2` now has broad adapter coverage and several adapters already validated through the same mainnet and recovery layers as the stronger `Wave 1` paths.
- The largest remaining gaps are no longer baseline adapter implementation; they are mostly selective hardening and endurance extension beyond the current bounded long-soak profile.

## Combined Matrix

Legend:

- `yes`: confirmed
- `shared`: confirmed through a shared pipeline-level drill
- `partial`: validated, but not yet at the same depth as the strongest adapters

| Wave | Adapter | Local full cycle | Live harness | Restart | Short soak | Long soak | Mainnet happy path | Mainnet budget | Local service outage | Mainnet degraded-service | Current status |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Wave 1 | PostgreSQL | yes | yes | yes | partial | partial | yes | yes | shared | shared | strongest native CDC path and longest validation history |
| Wave 1 | MySQL | yes | yes | yes | yes | yes | yes | yes | shared | shared | strong |
| Wave 1 | MariaDB | yes | yes | yes | yes | yes | yes | yes | shared | shared | strong |
| Wave 1 | SQL Server | yes | yes | yes | yes | yes | yes | yes | shared | shared | strong |
| Wave 1 | Oracle | yes | yes | yes | yes | yes | yes | yes | shared | shared | strong |
| Wave 1 | MongoDB | yes | yes | yes | yes | yes | yes | yes | shared | shared | strong |
| Wave 2 | Redis | yes | yes | yes | yes | yes | yes | yes | yes | yes | strong operational-state baseline |
| Wave 2 | ScyllaDB | yes | yes | yes | yes | yes | yes | yes | yes | yes | strong wide-column baseline |
| Wave 2 | IBM Db2 | yes | yes | yes | yes | yes | yes | yes | yes | yes | strong enterprise SQL baseline |
| Wave 2 | Apache Cassandra | yes | yes | yes | yes | yes | yes | yes | yes | yes | strong distributed wide-column baseline |
| Wave 2 | Elasticsearch | yes | yes | yes | yes | yes | yes | yes | yes | yes | strong search/index baseline |
| Wave 2 | SQLite | yes | file-backed | yes | yes | yes | yes | yes | yes | yes | strong edge and embedded baseline |

## Interpretation

### Wave 1

`Wave 1` should be read as the production-depth set.

It now has:

- local full-cycle validation across all adapters
- real `denotary` mainnet happy-path validation across all adapters
- restart recovery validation across all non-PostgreSQL native CDC adapters
- short-soak and long-soak validation across all non-PostgreSQL native CDC adapters
- shared local service-outage validation
- shared real-mainnet degraded-service validation

### Wave 2

`Wave 2` should be read as the expansion set.

It now has:

- broad adapter coverage across operational state, wide-column, enterprise SQL, search, and embedded/file-backed sources
- real `denotary` mainnet happy-path validation across all currently active `Wave 2` adapters
- bounded mainnet batch validation across all currently active `Wave 2` adapters
- bounded long-soak validation across all currently active `Wave 2` adapters
- real mainnet degraded-service validation for:
  - `Redis`
  - `SQLite`
  - `ScyllaDB`
  - `IBM Db2`
  - `Apache Cassandra`
  - `Elasticsearch`
- local restart, short-soak, and long-soak validation for all currently active `Wave 2` adapters
- local service-outage validation for all currently active `Wave 2` adapters

## Remaining Gaps

The main remaining work is now selective depth, not core platform coverage:

- longer endurance runs for `Wave 2` beyond the current bounded long-soak and budget profiles
- optional deeper outage-duration drills beyond the current bounded local and mainnet degraded-service profiles
- eventual return to `Snowflake` if a real credentialed environment becomes available again

## Recommended Packaging

For stakeholders, the cleanest way to present current readiness is:

1. this combined summary for executive status
2. [wave-rollout-pack.md](wave-rollout-pack.md) for stakeholder rollout framing
3. [wave1-readiness-matrix.md](wave1-readiness-matrix.md) for production-depth adapter detail
4. [wave2-readiness-matrix.md](wave2-readiness-matrix.md) for expansion-layer adapter detail

## Supporting Documents

- [wave-rollout-pack.md](wave-rollout-pack.md)
- [wave1-readiness-matrix.md](wave1-readiness-matrix.md)
- [wave2-readiness-matrix.md](wave2-readiness-matrix.md)
- [denotary-wave1-mainnet-validation-report.md](denotary-wave1-mainnet-validation-report.md)
- [wave1-mainnet-service-outage-validation-report.md](wave1-mainnet-service-outage-validation-report.md)
- [wave2-denotary-validation-report.md](wave2-denotary-validation-report.md)
- [wave2-mainnet-budget-validation-report.md](wave2-mainnet-budget-validation-report.md)
- [wave2-mainnet-service-outage-validation-report.md](wave2-mainnet-service-outage-validation-report.md)
- [wave2-long-soak-validation-report.md](wave2-long-soak-validation-report.md)
