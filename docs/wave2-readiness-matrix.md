# Wave 2 Readiness Matrix

This document summarizes the current implementation state of `Wave 2` adapters in `denotary-db-agent`.

It is intentionally narrower than the `Wave 1` readiness matrix: most `Wave 2` sources are still at the baseline and local-validation stage, while deeper restart/soak/mainnet validation remains future work.

## Current Matrix

| Adapter | Snapshot baseline | Local full-cycle proof export | Live validation harness | Mainnet happy path | Mainnet budget run | Mainnet degraded-service | Native CDC | Current readiness summary |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Redis | yes | yes | yes, Docker-backed | yes | yes | yes | no | strong operational-state baseline with scan polling, resume, live full-cycle validation, restart recovery, short-soak validation, local service-outage recovery, real `denotary` mainnet validation, bounded mainnet batch validation, and real mainnet degraded-service recovery |
| ScyllaDB | yes | yes | yes, Docker-backed | no | no | no | no | dedicated adapter alias over the Cassandra-compatible snapshot baseline with local full-cycle proof export, Docker-backed live validation, and deployment guidance |
| IBM Db2 | yes | yes | yes, env-gated and local Docker-backed | yes | no | no | no | strong enterprise SQL baseline with tracked-table introspection, local full-cycle proof export, local Docker live validation, and real `denotary` mainnet happy-path validation |
| Apache Cassandra | yes | yes | yes, env-gated and local Docker-backed | yes | no | no | no | strong distributed wide-column baseline with tracked-table introspection, local full-cycle proof export, local Docker live validation, and real `denotary` mainnet happy-path validation |
| Elasticsearch | yes | yes | yes, env-gated and local Docker-backed | yes | no | no | no | strong search/index baseline with tracked-index introspection, query-based polling, local Docker live validation, restart/short-soak validation, and real `denotary` mainnet happy-path validation |
| SQLite | yes | yes | file-backed adapter tests | yes | yes | yes | no | strong edge and embedded baseline with file-backed validation, local full-cycle proof export, cold restart recovery, short-soak validation, local service-outage recovery, real `denotary` mainnet validation, bounded mainnet batch validation, and real mainnet degraded-service recovery |

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

Additionally implemented where applicable:

- live connection or cluster validation
- tracked-object introspection
- env-gated or Docker-backed live harnesses
- local full-cycle proof export for `ScyllaDB`
- Docker-backed live validation for `ScyllaDB`
- real `denotary` mainnet happy-path validation for `IBM Db2`, `Apache Cassandra`, and `Elasticsearch`
- local service-outage recovery validation for `SQLite` and `Redis`
- real `denotary` mainnet validation for `SQLite` and `Redis`
- bounded `denotary` mainnet batch validation for `SQLite` and `Redis`
- real `denotary` mainnet degraded-service recovery validation for `SQLite` and `Redis`

## Remaining Validation Layers

The following layers still remain open for most `Wave 2` adapters:

1. restart recovery validation
2. short soak validation
3. long soak validation
4. service outage validation beyond `SQLite` and `Redis`
5. mainnet `denotary` happy-path validation beyond `SQLite` and `Redis`
6. bounded budget validation where commercially justified

## Suggested Next Steps

Priority after the baseline phase:

1. run the env-gated `Elasticsearch` restart and short-soak validation in a real environment
2. bounded budget validation for `IBM Db2`, `Apache Cassandra`, and `Elasticsearch`
3. `Wave 2` service-outage validation beyond `SQLite` and `Redis`

Mainnet-confirmed references:

- [wave2-denotary-validation.md](wave2-denotary-validation.md)
- [wave2-denotary-validation-report.md](wave2-denotary-validation-report.md)
- [wave2-mainnet-budget-validation.md](wave2-mainnet-budget-validation.md)
- [wave2-mainnet-budget-validation-report.md](wave2-mainnet-budget-validation-report.md)
- [wave2-mainnet-service-outage-validation.md](wave2-mainnet-service-outage-validation.md)
- [wave2-mainnet-service-outage-validation-report.md](wave2-mainnet-service-outage-validation-report.md)

## Interpretation

Current `Wave 2` status should be read as:

- adapter coverage is now broad
- proof-export integration is already real
- production-readiness depth is still uneven compared with fully validated `Wave 1`

That is still a strong milestone: the platform can now onboard the next six commercially relevant source classes without inventing new core agent architecture for each one.
