# Wave 2 Release Roadmap

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

This document defines the remaining work needed to promote the active `Wave 2`
database adapters to release status.

It is intentionally narrower than the original `Wave 2` product roadmap. The
goal here is not to expand scope, but to close the final release and operations
gaps for the adapters already in active use:

- `Redis`
- `ScyllaDB`
- `IBM Db2`
- `Apache Cassandra`
- `Elasticsearch`
- `SQLite`

## Release Position

Current repository evidence already shows that the active `Wave 2` set is well
beyond baseline proof export:

- local full-cycle validation exists across the active adapter set
- restart, short-soak, and bounded long-soak coverage exists
- local service-outage validation exists
- real `denotary` mainnet happy-path validation exists
- bounded mainnet budget validation exists
- real mainnet degraded-service validation exists

That means the remaining work is mainly selective hardening for release rather
than baseline implementation.

## Release Target

The recommended release target is:

- `GA` for the current polling-based `Wave 2` baseline

This means:

- `SQLite`, `Db2`, `Cassandra`, `ScyllaDB`, and `Elasticsearch` release with
  `watermark` polling as the supported capture path
- `Redis` releases with `scan` polling as the supported capture path
- native CDC remains a post-release commercial or product expansion track

This avoids blocking release on a much larger parity project with `Wave 1`
native CDC paths.

## P0 Release Blockers

These items should be completed before any release promotion.

### 1. Freeze the supported product contract

- explicitly document that current `Wave 2` `GA` is polling-based
- define support boundaries for each adapter capture mode
- state known operator assumptions such as required monotonic watermark fields,
  tracked-table visibility, and polling-based lag behavior

### 2. Make operator status authoritative

- ensure release tooling reports the true active adapter, cycle, and last update
- remove stale top-level run status behavior where summary files can lag behind
  adapter progress
- confirm `status`, `health`, `metrics`, `diagnostics`, and `doctor` present a
  consistent operator story for `Wave 2` adapters

### 3. Align the documentation set

- reconcile `README`, readiness matrices, runbooks, and roadmap language so
  they describe the same current state
- add one release-oriented `Wave 2` summary that states what is already proven,
  what is supported in `GA`, and what remains post-release
- remove old wording that still implies earlier pre-validation stages
- keep runbooks, config references, and readiness summaries descriptive only:
  current state, supported configuration, and current limits belong there;
  release sequencing and future work belong in roadmap documents

### 4. Define a release gate

Every active `Wave 2` adapter should pass a common release checklist:

1. unit and contract tests
2. live integration harness
3. restart validation
4. short-soak validation
5. bounded long-soak validation
6. local service-outage validation
7. mainnet happy-path validation
8. bounded mainnet budget validation
9. mainnet degraded-service validation
10. `doctor`, `inspect`, `metrics`, and `diagnostics` evidence capture

## P1 Hardening Work

These items should be completed in the release cycle unless an explicit
exception is accepted.

### Cross-cutting hardening

- create a repeatable release-candidate run that archives proof artifacts,
  summaries, and diagnostics evidence
- define alert thresholds for polling lag, proof latency, error rate, and retry
  streaks
- run repeated pre-release validation instead of relying on a single successful
  mainnet run
- lock the example configs and operator runbooks used for release evidence

### Adapter-specific hardening

#### Redis

- validate `SCAN` behavior on larger keyspaces and document expected cost
- define safe defaults for `scan_count`
- document when keyspace notifications are unnecessary and when they might
  become a later product enhancement

#### SQLite

- expand release guidance for WAL mode, file locking, backup rotation, and
  read-only host scenarios
- document the watermark correctness contract for embedded and edge operators

#### IBM Db2

- harden deployment and driver-install guidance
- validate reconnect behavior under source restarts and connection churn
- confirm timestamp precision and ordering behavior on release reference setups

#### Apache Cassandra

- replace lab-style guidance with production-safe examples for replication,
  authentication, and consistency settings
- validate release guidance against more realistic cluster topology defaults

#### ScyllaDB

- provide explicit release guidance for Scylla-specific production settings even
  when the capture path stays Cassandra-compatible
- confirm that the Cassandra-derived runbook is sufficient for day-2 operation

#### Elasticsearch

- validate behavior with aliases, rollover, mapping drift, and refresh lag
- document the operational limits of watermark polling on large or busy indices

## P2 Post-release Expansion

These items are valuable, but they should not block the initial `Wave 2`
release unless product scope changes.

- native CDC paths for `Db2`, `Cassandra`, `ScyllaDB`, `Elasticsearch`,
  `SQLite`, or `Redis`
- endurance runs beyond the current bounded long-soak profile
- broader degraded-service scenarios with live source mutations rather than the
  current bounded outage model
- expansion to new `Wave 2` targets before the current set is fully released
- deferred credential-gated targets such as `Snowflake` until real validation
  environments are available again

## Recommended Delivery Sequence

### Phase A: Release Definition

- freeze the `GA` support model
- align docs
- define release gates and evidence requirements

### Phase B: Operational Hardening

- fix status and observability gaps
- standardize release-candidate execution
- confirm operator commands provide trustworthy rollout signals

### Phase C: Adapter Hardening

- close the adapter-specific runbook and tuning gaps
- validate production-safe defaults for each backend family

### Phase D: Release Candidate

- run at least two or three repeatable release-candidate passes
- archive proofs, summaries, `doctor`, and diagnostics evidence
- prepare `wave-2 -> main` merge and release notes

## Exit Criteria

The active `Wave 2` set is ready for release when:

- every active adapter passes the agreed release gate
- the supported polling-based capture model is clearly documented
- status and operator evidence are trustworthy
- each adapter has a stable example config and runbook
- release-candidate validation is repeatable without special author knowledge

## Non-goals for This Release

- full native CDC parity with `Wave 1`
- adding new database adapters before current `Wave 2` promotion is complete
- expanding the validation scope faster than the operator and release evidence
  story can support
