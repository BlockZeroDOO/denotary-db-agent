# Wave 2 Merge Preparation

This document describes the recommended path for merging `wave-2` back into `main`.

It is not a code-level changelog. It is a rollout and integration checklist for deciding when and how to merge the branch safely.

## Goal

Merge `wave-2` into `main` without collapsing operational clarity.

The key objective is to preserve three things:

- a clear production story for the already-mature `Wave 1` adapters
- a controlled rollout story for the now strongly validated `Wave 2` adapters
- a manageable rollback surface if any post-merge issue appears

## Current Readiness

`wave-2` is no longer just an experimental adapter branch.

It now includes:

- six active `Wave 2` adapters:
  - `Redis`
  - `SQLite`
  - `ScyllaDB`
  - `IBM Db2`
  - `Apache Cassandra`
  - `Elasticsearch`
- local full-cycle validation across all active `Wave 2` adapters
- restart, short-soak, and long-soak validation across all active `Wave 2` adapters
- local service-outage validation across all active `Wave 2` adapters
- real `denotary` mainnet happy-path validation across all active `Wave 2` adapters
- bounded mainnet batch validation across all active `Wave 2` adapters
- real mainnet degraded-service validation across all active `Wave 2` adapters

## Merge Recommendation

Recommended approach:

1. merge `wave-2` into `main` as a controlled readiness milestone
2. immediately follow the merge with one final `main` branch validation pass
3. treat `Wave 2` adapters as enabled product surface, but position them externally as:
   - strong controlled-rollout candidates
   - not deeper-than-`Wave 1` by default

## Recommended Merge Gates

Before merge, confirm:

- `wave-2` working tree is clean
- `main` is clean and up to date with `origin/main`
- full unit and integration suite passes on `wave-2`
- `Wave 1` readiness docs are unchanged in substance, except where intentionally improved by shared packaging
- `Wave 2` readiness docs, reports, and rollout pack are committed
- no deferred `Snowflake` references remain in active code or validation requirements

## Recommended Merge Slices

If a single merge feels too large, the safest slicing is:

### Slice 1: Core Adapter Surface

- adapter registry additions
- config docs
- local full-cycle tests
- deployment examples

### Slice 2: Live and Docker Validation

- live harnesses
- Docker-backed validation scripts
- restart / soak validation

### Slice 3: Mainnet Validation

- mainnet happy-path
- bounded budget runs
- degraded-service validation
- readiness matrices and reports

### Slice 4: Packaging

- combined summary
- rollout pack
- merge-prep docs

If the branch remains stable, a single merge is still reasonable because the validation layer is now broad and consistent.

## Post-Merge Checklist

Immediately after merge to `main`:

- run the full test suite
- rerun the shared validation launchers that do not depend on remote credentials
- sanity-check:
  - `README.md`
  - [supported-databases.md](supported-databases.md)
  - [wave-readiness-summary.md](wave-readiness-summary.md)
  - [wave-rollout-pack.md](wave-rollout-pack.md)
- confirm `main` still tells a clear story:
  - `Wave 1` is the production-depth set
  - `Wave 2` is the expansion set with strong bounded validation

## Rollback Surface

The safest rollback framing after merge is not "remove Wave 2 completely".

Instead, the rollback surface should be:

- documentation positioning
- exposure of example configs and runbooks
- operational recommendation level for specific adapters

The adapter code itself is now broad enough that a full revert should be a last resort.

## Recommended External Messaging After Merge

After merge to `main`, the cleanest positioning is:

- `Wave 1`:
  mature production-depth adapters
- `Wave 2`:
  strongly validated expansion adapters suitable for controlled rollout, targeted onboarding, and partner pilots

## Supporting Documents

- [wave-rollout-pack.md](wave-rollout-pack.md)
- [wave-readiness-summary.md](wave-readiness-summary.md)
- [wave1-readiness-matrix.md](wave1-readiness-matrix.md)
- [wave2-readiness-matrix.md](wave2-readiness-matrix.md)
