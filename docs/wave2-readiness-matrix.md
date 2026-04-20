# Wave 2 Readiness Matrix

This document summarizes the current implementation state of `Wave 2` adapters in `denotary-db-agent`.

It is intentionally narrower than the `Wave 1` readiness matrix: most `Wave 2` sources are still at the baseline and local-validation stage, while deeper restart/soak/mainnet validation remains future work.

## Current Matrix

| Adapter | Snapshot baseline | Local full-cycle proof export | Live validation harness | Mainnet happy path | Native CDC | Current readiness summary |
| --- | --- | --- | --- | --- | --- | --- |
| Snowflake | yes | yes | yes, env-gated | no | no | good enterprise analytics baseline with watermark polling and live warehouse/object validation |
| Redis | yes | yes | yes, Docker-backed | yes | no | strong operational-state baseline with scan polling, resume, live full-cycle validation, restart recovery, short-soak validation, local service-outage recovery, and real `denotary` mainnet validation |
| IBM Db2 | yes | yes | yes, env-gated | no | no | strong enterprise SQL baseline with tracked-table introspection and local full-cycle proof export |
| Apache Cassandra | yes | yes | yes, env-gated | no | no | strong distributed wide-column baseline with tracked-table introspection and local full-cycle proof export |
| Elasticsearch | yes | yes | yes, env-gated | no | no | strong search/index baseline with tracked-index introspection, query-based polling, and env-gated restart/short-soak harness |
| SQLite | yes | yes | file-backed adapter tests | yes | no | strong edge and embedded baseline with file-backed validation, local full-cycle proof export, cold restart recovery, short-soak validation, local service-outage recovery, and real `denotary` mainnet validation |

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
- local service-outage recovery validation for `SQLite` and `Redis`
- real `denotary` mainnet validation for `SQLite` and `Redis`

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
2. `Snowflake` live validation in a real account
3. `IBM Db2` and `Apache Cassandra` live validation in real environments
4. `Wave 2` service-outage validation beyond `SQLite` and `Redis`

Mainnet-confirmed references:

- [wave2-denotary-validation.md](wave2-denotary-validation.md)
- [wave2-denotary-validation-report.md](wave2-denotary-validation-report.md)

## Interpretation

Current `Wave 2` status should be read as:

- adapter coverage is now broad
- proof-export integration is already real
- production-readiness depth is still uneven compared with fully validated `Wave 1`

That is still a strong milestone: the platform can now onboard the next six commercially relevant source classes without inventing new core agent architecture for each one.
