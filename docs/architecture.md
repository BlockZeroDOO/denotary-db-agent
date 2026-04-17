# deNotary DB Agent Architecture

## Purpose

`denotary-db-agent` is the enterprise CDC sidecar that bridges database change events into the existing deNotary off-chain stack.

The package assumes:

- `Ingress API` prepares canonical request payloads
- `Finality Watcher` tracks and verifies on-chain results
- `Receipt Service` and `Audit API` expose finalized read models
- enterprise writes flow through `verifbill` into internal registry `verif`

## Core Components

- `adapters`
  - one adapter per database family
  - capability discovery
  - connectivity validation
  - snapshot/bootstrap and stream interfaces
- `canonical`
  - deterministic event hashing
  - stable `external_ref`
  - source-independent event envelope
- `checkpoint_store`
  - SQLite persistence for source cursors, delivery attempts, and DLQ records
- `transport`
  - `Ingress API` prepare requests
  - Finality Watcher registration handoff
- `engine`
  - orchestration, retries, checkpoint updates, and delivery bookkeeping
- `cli`
  - operator entrypoints

## Current State

This version implements the platform skeleton and the first live PostgreSQL baseline.

Implemented now:

- config schema
- adapter contract
- capability registry for PostgreSQL, MySQL, MariaDB, SQL Server, Oracle, and MongoDB
- deterministic canonicalization
- checkpoint store and DLQ
- enterprise prepare + watcher registration clients
- CLI and tests
- PostgreSQL watermark-based snapshot/poll adapter with per-table checkpoint state

Not implemented yet:

- live CDC streaming drivers for PostgreSQL logical decoding and the remaining databases
- enterprise transaction signing/broadcast inside the agent
- batching into `Ingress API /v1/batch/prepare`
- production metrics export
