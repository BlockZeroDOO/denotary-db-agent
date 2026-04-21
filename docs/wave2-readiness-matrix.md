# Wave 2 Readiness Matrix

This document summarizes the current implementation state of `Wave 2` adapters in `denotary-db-agent`.

For the combined rollout view across both implementation waves, see:

- [wave-readiness-summary.md](wave-readiness-summary.md)

It is intentionally narrower than the `Wave 1` readiness matrix: most `Wave 2` sources are still at the baseline and local-validation stage, while deeper restart/soak/mainnet validation remains future work.

## Current Matrix

| Adapter | Snapshot baseline | Local full-cycle proof export | Live validation harness | Restart | Short soak | Long soak | Mainnet happy path | Mainnet budget run | Mainnet degraded-service | Native CDC | Current readiness summary |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Redis | yes | yes | yes, Docker-backed | yes | yes | yes | yes | yes | yes | no | strong operational-state baseline with scan polling, resume, live full-cycle validation, restart recovery, short-soak validation, long-soak validation, local service-outage recovery, real `denotary` mainnet validation, bounded mainnet batch validation, and real mainnet degraded-service recovery |
| ScyllaDB | yes | yes | yes, Docker-backed | yes | yes | yes | yes | yes | yes | no | dedicated wide-column adapter with local full-cycle proof export, Docker-backed live validation, restart recovery, short-soak validation, long-soak validation, local service-outage recovery, real `denotary` mainnet validation, bounded mainnet batch validation, real mainnet degraded-service recovery, and deployment guidance |
| IBM Db2 | yes | yes | yes, env-gated and local Docker-backed | yes | yes | yes | yes | yes | yes | no | strong enterprise SQL baseline with tracked-table introspection, local full-cycle proof export, local Docker live validation, restart recovery, short-soak validation, long-soak validation, local service-outage recovery, real `denotary` mainnet happy-path validation, bounded mainnet batch validation, and real mainnet degraded-service recovery |
| Apache Cassandra | yes | yes | yes, env-gated and local Docker-backed | yes | yes | yes | yes | yes | yes | no | strong distributed wide-column baseline with tracked-table introspection, local full-cycle proof export, local Docker live validation, restart recovery, short-soak validation, long-soak validation, local service-outage recovery, real `denotary` mainnet happy-path validation, bounded mainnet batch validation, and real mainnet degraded-service recovery |
| Elasticsearch | yes | yes | yes, env-gated and local Docker-backed | yes | yes | yes | yes | yes | yes | no | strong search/index baseline with tracked-index introspection, query-based polling, local Docker live validation, restart, short-soak, long-soak, and local service-outage validation, real `denotary` mainnet happy-path validation, bounded mainnet batch validation, and real mainnet degraded-service recovery |
| SQLite | yes | yes | file-backed adapter tests | yes | yes | yes | yes | yes | yes | no | strong edge and embedded baseline with file-backed validation, local full-cycle proof export, cold restart recovery, short-soak validation, long-soak validation, local service-outage recovery, real `denotary` mainnet validation, bounded mainnet batch validation, and real mainnet degraded-service recovery |

## Implemented Scope

Implemented across the current `Wave 2` set:

- adapter registry integration
- source-specific config references
- shared adapter contract conformance
- bootstrap and inspect support
- deterministic runtime signatures
- snapshot / watermark polling baselines
- checkpoint resume
- dry-run playback for pipeline validation
- local full-cycle proof export
- long-soak validation across all active `Wave 2` adapters

Additionally implemented where applicable:

- live connection or cluster validation
- tracked-object introspection
- env-gated or Docker-backed live harnesses
- local full-cycle proof export for `ScyllaDB`
- Docker-backed live validation for `ScyllaDB`
- restart, short-soak, and long-soak validation for `ScyllaDB`
- local service-outage recovery validation for `ScyllaDB`
- real `denotary` mainnet happy-path validation for `ScyllaDB`
- bounded `denotary` mainnet batch validation for `ScyllaDB`
- real `denotary` mainnet degraded-service recovery validation for `ScyllaDB`
- restart, short-soak, and long-soak validation for `IBM Db2`
- restart, short-soak, and long-soak validation for `Apache Cassandra`
- long-soak validation for `Redis`, `SQLite`, and `Elasticsearch`
- local service-outage recovery validation for `IBM Db2`, `Apache Cassandra`, and `Elasticsearch`
- real `denotary` mainnet happy-path validation for `IBM Db2`, `Apache Cassandra`, and `Elasticsearch`
- bounded `denotary` mainnet batch validation for `IBM Db2`, `Apache Cassandra`, and `Elasticsearch`
- real `denotary` mainnet degraded-service recovery validation for `Apache Cassandra`
- real `denotary` mainnet degraded-service recovery validation for `IBM Db2`
- real `denotary` mainnet degraded-service recovery validation for `Elasticsearch`
- local service-outage recovery validation for all active `Wave 2` adapters
- real `denotary` mainnet validation for `SQLite` and `Redis`
- bounded `denotary` mainnet batch validation for `SQLite` and `Redis`
- real `denotary` mainnet degraded-service recovery validation for `SQLite` and `Redis`

## Remaining Validation Layers

The main remaining work is now selective hardening rather than baseline coverage:

1. longer endurance runs beyond the current bounded long-soak profile
2. additional outage duration and recovery-shape exploration beyond the current bounded local drills
3. optional additional mainnet source-backed degraded drills where we want live source mutations instead of the current outage pattern
4. merge planning for `wave-2 -> main`
5. future re-entry of `Snowflake` only when a real credentialed environment becomes available again

## Suggested Next Steps

Priority after the baseline phase:

1. deeper endurance runs where commercially justified
2. deeper outage-duration drills beyond the current bounded recovery profile
3. merge preparation and packaging for `main`
4. future credential-backed re-entry for deferred sources if needed

Mainnet-confirmed references:

- [wave2-denotary-validation.md](wave2-denotary-validation.md)
- [wave2-denotary-validation-report.md](wave2-denotary-validation-report.md)
- [wave2-mainnet-budget-validation.md](wave2-mainnet-budget-validation.md)
- [wave2-mainnet-budget-validation-report.md](wave2-mainnet-budget-validation-report.md)
- [wave2-mainnet-service-outage-validation.md](wave2-mainnet-service-outage-validation.md)
- [wave2-mainnet-service-outage-validation-report.md](wave2-mainnet-service-outage-validation-report.md)
- [wave2-long-soak-validation.md](wave2-long-soak-validation.md)
- [wave2-long-soak-validation-report.md](wave2-long-soak-validation-report.md)

## Interpretation

Current `Wave 2` status should be read as:

- adapter coverage is now broad
- proof-export integration is already real
- production-readiness depth is now much closer to the stronger `Wave 1` baseline, though still not identical across every operational drill

That is still a strong milestone: the platform can now onboard the next six commercially relevant source classes without inventing new core agent architecture for each one.
