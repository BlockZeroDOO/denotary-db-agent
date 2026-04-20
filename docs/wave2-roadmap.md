# Wave 2 Database Roadmap

`Wave 2` expands `denotary-db-agent` beyond the current `Wave 1` production set and targets the next commercially relevant database classes that remain in active scope.

## Goal

Extend coverage into:

- fast operational state stores
- legacy enterprise SQL estates
- distributed wide-column systems
- search-backed operational evidence
- embedded and edge databases

## Active Order

1. `Redis`
2. `ScyllaDB`
3. `IBM Db2`
4. `Apache Cassandra`
5. `Elasticsearch`
6. `SQLite`

## Why This Order

### 1. Redis

- strong operational relevance
- useful for session, cache, and application-state proofing
- good candidate for a fast baseline with real mainnet validation

### 2. ScyllaDB

- relevant for high-throughput Cassandra-compatible deployments
- commercially useful where customers standardize on Scylla instead of Apache Cassandra
- lets us reuse the proven wide-column baseline without inventing a new core model

### 3. IBM Db2

- important for large enterprise and regulated environments
- strengthens the serious enterprise SQL positioning of the agent

### 4. Apache Cassandra

- relevant for distributed high-write workloads
- expands the product into wide-column and scale-out workloads

### 5. Elasticsearch

- important as an operational search and index layer
- useful where customers want to notarize the data actually exposed through search systems

### 6. SQLite

- very broad ecosystem footprint
- useful for edge, embedded, desktop, mobile, and local-first scenarios

## Phase Plan

## Phase 1: Highest Commercial Value

- `Redis`
- `ScyllaDB`
- `IBM Db2`

Target outcome:

- expand enterprise sales coverage quickly

## Phase 2: Distributed / Search Expansion

- `Apache Cassandra`
- `Elasticsearch`

Target outcome:

- support infrastructure-heavy and high-scale workloads

## Phase 3: Edge / Embedded

- `SQLite`

Target outcome:

- extend the platform to edge and local-first environments

## Validation Standard

Every active `Wave 2` adapter should eventually pass the same validation ladder:

1. snapshot / watermark baseline
2. native CDC path where practical
3. local full-cycle proof export
4. restart recovery
5. short soak
6. service outage recovery
7. mainnet happy-path
8. bounded budget run when commercially important

## Current Implementation Scope

Implementation currently covers:

- `Redis` adapter contract registration
- `Redis` configuration reference
- `Redis` live ping and explicit key-pattern snapshot baseline
- `Redis` local full-cycle proof export
- `Redis` Docker-backed live integration and full-cycle harnesses
- `Redis` restart recovery and short-soak validation
- `ScyllaDB` adapter contract registration
- `ScyllaDB` configuration reference
- `ScyllaDB` Cassandra-compatible snapshot baseline and deployment/runbook layer
- `ScyllaDB` local full-cycle proof export
- `ScyllaDB` Docker-backed live integration harness
- `ScyllaDB` restart and short-soak validation
- real `denotary` mainnet happy-path validation for `ScyllaDB`
- `IBM Db2` adapter contract registration
- `IBM Db2` configuration reference
- `IBM Db2` live ping, tracked-table introspection, snapshot baseline, local full-cycle proof export, env-gated live validation harness, local Docker live validation, and real `denotary` mainnet happy-path validation
- `IBM Db2` starter deployment config and runbook
- `Apache Cassandra` adapter contract registration
- `Apache Cassandra` configuration reference
- `Apache Cassandra` live cluster ping, tracked-table introspection, snapshot baseline, local full-cycle proof export, env-gated live validation harness, local Docker live validation, and real `denotary` mainnet happy-path validation
- `Apache Cassandra` starter deployment config and runbook
- `Elasticsearch` adapter contract registration
- `Elasticsearch` configuration reference
- `Elasticsearch` live cluster ping, tracked-index introspection, snapshot baseline, local full-cycle proof export, env-gated live validation harness, local Docker live validation, restart and short-soak validation, and real `denotary` mainnet happy-path validation
- `SQLite` adapter contract registration
- `SQLite` configuration reference
- `SQLite` file-backed readiness validation, tracked-table introspection, snapshot baseline, local full-cycle proof export, cold restart recovery, short-soak validation, and real `denotary` mainnet validation
- `Wave 2` planning and documentation

## Planned Next Step

- bounded mainnet budget validation for `IBM Db2`, `Apache Cassandra`, and `Elasticsearch`
- service-outage and deeper restart validation beyond `SQLite` and `Redis`
- use the unified `Wave 2` live validation checklist to drive real-environment validation for `Db2`, `Cassandra`, and `Elasticsearch`
