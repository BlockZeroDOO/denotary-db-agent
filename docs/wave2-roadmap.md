# Wave 2 Database Roadmap

`Wave 2` expands `denotary-db-agent` beyond the current `Wave 1` production set and targets the next commercially relevant database classes.

## Goal

Extend coverage into:

- cloud analytics platforms
- fast operational state stores
- legacy enterprise SQL estates
- distributed wide-column systems
- search-backed operational evidence
- embedded and edge databases

## Recommended Order

1. `Snowflake`
2. `Redis`
3. `IBM Db2`
4. `Apache Cassandra`
5. `Elasticsearch`
6. `SQLite`

## Why This Order

### 1. Snowflake

- strongest next enterprise target for analytics and warehouse evidence
- high commercial value
- good fit for snapshot and query-based incremental capture

### 2. Redis

- strong operational relevance
- useful for session, cache, and application-state proofing
- good candidate for a fast baseline after `Snowflake`

### 3. IBM Db2

- important for large enterprise and regulated environments
- strengthens the “serious enterprise SQL” positioning of the agent

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

- `Snowflake`
- `Redis`
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

Every `Wave 2` adapter should eventually pass the same validation ladder:

1. snapshot / watermark baseline
2. native CDC path where practical
3. local full-cycle proof export
4. restart recovery
5. short soak
6. service outage recovery
7. mainnet happy-path
8. bounded budget run when commercially important

## Current Implementation Start

Implementation starts now with:

- `Snowflake` adapter contract registration
- `Snowflake` configuration reference
- `Snowflake` tracked-object bootstrap and inspect baseline
- `Snowflake` live warehouse ping and configured object introspection
- `Snowflake` query-based snapshot polling with watermark resume
- `Snowflake` local full-cycle proof export
- `Snowflake` env-gated live integration and full-cycle harnesses
- `Redis` adapter contract registration
- `Redis` configuration reference
- `Redis` live ping and explicit key-pattern snapshot baseline
- `Redis` local full-cycle proof export
- `Redis` Docker-backed live integration and full-cycle harnesses
- `Redis` restart recovery and short-soak validation
- `IBM Db2` adapter contract registration
- `IBM Db2` configuration reference
- `IBM Db2` live ping, tracked-table introspection, snapshot baseline, local full-cycle proof export, and env-gated live validation harness
- `Apache Cassandra` adapter contract registration
- `Apache Cassandra` configuration reference
- `Apache Cassandra` live cluster ping, tracked-table introspection, snapshot baseline, local full-cycle proof export, and env-gated live validation harness
- `Elasticsearch` adapter contract registration
- `Elasticsearch` configuration reference
- `Elasticsearch` live cluster ping, tracked-index introspection, snapshot baseline, local full-cycle proof export, and env-gated live validation harness
- `SQLite` adapter contract registration
- `SQLite` configuration reference
- `SQLite` file-backed readiness validation, tracked-table introspection, snapshot baseline, and local full-cycle proof export
- `SQLite` cold restart recovery and short-soak validation
- `Wave 2` planning and documentation

Planned next step:

- extend the next `Wave 2` sources beyond baseline depth, starting with `Elasticsearch` restart and short-soak validation
