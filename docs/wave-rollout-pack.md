# Wave Rollout Pack

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

This document is the stakeholder-facing rollout pack for `denotary-db-agent`.

It is intentionally shorter than the detailed readiness matrices and validation reports.

Its purpose is to answer four practical questions:

- what is already strong enough for production-oriented rollout conversations
- what is strong enough for pilots and controlled deployments
- what is already confirmed on real `denotary` mainnet
- what level of rollout each adapter set supports today

## Executive Position

`Wave 1` is the production-depth set.

`Wave 2` is the expansion set and is now materially beyond simple prototype status. It already has broad local validation, real mainnet happy-path coverage, bounded mainnet batch validation, and bounded degraded-service validation across all active adapters.

## Rollout Tiers

### Tier 1: Strong Production Candidates

These adapters now have the deepest and most even validation ladder:

- PostgreSQL
- MySQL
- MariaDB
- SQL Server
- Oracle
- MongoDB

Why they belong here:

- local full-cycle proof export is validated
- live harnesses are in place
- mainnet happy-path validation is confirmed
- restart recovery is confirmed
- soak validation is confirmed
- degraded-service recovery is confirmed through shared pipeline drills

Recommended use:

- production onboarding
- regulated or audit-heavy pilots
- customer-facing rollout packaging

### Tier 2: Strong Expansion Candidates

These adapters now have strong bounded validation and are suitable for controlled rollout and serious pilot work:

- Redis
- SQLite
- ScyllaDB
- IBM Db2
- Apache Cassandra
- Elasticsearch

Why they belong here:

- local full-cycle proof export is validated
- local live or Docker-backed validation is confirmed
- restart validation is confirmed
- short-soak and long-soak validation are confirmed
- local service-outage recovery is confirmed
- real `denotary` mainnet happy-path validation is confirmed
- bounded mainnet batch validation is confirmed
- real mainnet degraded-service validation is confirmed

Recommended use:

- controlled customer pilots
- expansion into adjacent source classes
- platform positioning for broader database coverage

## Mainnet-Confirmed Scope

Real `denotary` mainnet confirmation now exists for:

- Wave 1:
  - PostgreSQL
  - MySQL
  - MariaDB
  - SQL Server
  - Oracle
  - MongoDB
- Wave 2:
  - Redis
  - SQLite
  - ScyllaDB
  - IBM Db2
  - Apache Cassandra
  - Elasticsearch

## Recommended External Positioning

For rollout conversations, the cleanest framing is:

- `Wave 1`:
  production-depth and ready for serious rollout packaging
- `Wave 2`:
  expansion-ready and already strong enough for controlled pilots, targeted production onboarding, and partner demonstrations

## Primary Supporting Documents

- [wave-readiness-summary.md](wave-readiness-summary.md)
- [wave1-readiness-matrix.md](wave1-readiness-matrix.md)
- [wave2-readiness-matrix.md](wave2-readiness-matrix.md)
- [wave2-merge-prep.md](wave2-merge-prep.md)
