# deNotary DB Agent Architecture

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

## Purpose

`denotary-db-agent` is a source-to-notarization sidecar for database-backed
systems.

It reads source changes from supported databases, canonicalizes them into a
stable change-event envelope, submits the resulting request through the
`deNotary` service stack, and stores local checkpoints, delivery state, and
proof metadata.

The agent is designed around two concerns:

- source capture
- notarization delivery and proof export

That separation is implemented in code and described in more detail in
[adapter-separation-architecture.md](adapter-separation-architecture.md).

## High-Level Flow

For each enabled source, the runtime flow is:

1. load source config and build the adapter runtime
2. validate connectivity and inspect source capabilities
3. resume from the stored checkpoint
4. read events through polling or CDC, depending on the adapter and
   `capture_mode`
5. canonicalize each event into a stable envelope and `external_ref`
6. prepare a notarization request through `Ingress API`
7. validate the returned `prepared_action` against the locally expected payload
8. register the request in `Finality Watcher`
9. sign and broadcast the prepared action through the configured enterprise
   signer
10. wait for inclusion/finality when `wait_for_finality = true`
11. retrieve receipt and audit-chain data
12. export the local proof bundle and advance the source checkpoint

If a request was already prepared or broadcast earlier, the agent can resume
from stored delivery state instead of rebuilding the whole flow from scratch.

## Security Model

The live trust boundary for `denotary-db-agent` includes more than the agent
process itself.

In a live notarization deployment, the agent:

- sends canonical request payloads to `Ingress`
- receives a `prepared_action` back from `Ingress`
- signs and broadcasts that action with an enterprise hot key
- relies on `Watcher`, `Receipt`, and `Audit` to finalize and export proof

That means the following components should be treated as part of the same
trusted operational zone:

- the agent
- `Ingress`
- `Finality Watcher`
- `Receipt`
- `Audit`

The current production-style baseline is:

- local or private-only deNotary backend services
- a dedicated enterprise hot permission such as `dnanchor`
- no use of `owner` or `active` on the DB Agent host
- local state, proof directories, and evidence snapshots treated as sensitive
  operator artifacts

## Main Layers

### Source Layer

The source layer is responsible for database-specific capture behavior.

Key modules:

- `denotary_db_agent/adapters/*`
- `denotary_db_agent/adapters/base.py`
- `denotary_db_agent/adapters/registry.py`
- `denotary_db_agent/source_runtime.py`

Main responsibilities:

- adapter capability discovery
- connection validation
- bootstrap and inspect payloads
- snapshot polling or CDC iteration
- checkpoint serialization and resume
- runtime signatures and tracked-object reporting

The shared adapter contract exposes:

- supported `capture_modes`
- CDC capability flags
- checkpoint strategy
- activity model
- tracked-object summaries used by `inspect`, `doctor`, `diagnostics`, and
  `report`

### Canonicalization Layer

The canonicalization layer converts source-specific changes into a stable,
source-independent payload.

Key module:

- `denotary_db_agent/canonical.py`

Main responsibilities:

- stable event normalization
- deterministic hashing
- `external_ref` generation
- canonical envelope generation for single and batch delivery

### Delivery and Proof Layer

The delivery layer turns canonical events into notarized requests and exported
proof artifacts.

Key modules:

- `denotary_db_agent/pipeline.py`
- `denotary_db_agent/transport.py`

Main responsibilities:

- prepare requests through `Ingress API`
- validate `prepared_action` against the locally expected payload before
  registration and signing
- register requests with `Finality Watcher`
- sign and broadcast prepared actions to the chain
- detect and recover duplicate-submit cases
- finalize requests through receipt and audit-chain retrieval
- export proof bundles
- reconcile pending finality from stored delivery context

### Orchestration Layer

The orchestration layer owns the process-level runtime.

Key modules:

- `denotary_db_agent/engine.py`
- `denotary_db_agent/cli.py`

Main responsibilities:

- build runtimes for enabled sources
- run one-pass or daemon polling loops
- batch or single-event execution
- pause/resume and replay controls
- DLQ handling
- retention and housekeeping
- health, doctor, metrics, diagnostics, and report views

### Local State Layer

The local state layer persists agent runtime state in SQLite.

Key module:

- `denotary_db_agent/checkpoint_store.py`

Stored state includes:

- per-source checkpoints
- delivery attempts
- prepared action payloads needed for resume
- stored event payloads needed for reconciliation
- proof metadata
- DLQ records
- per-source pause controls
- per-source runtime signatures

## Runtime Modes

The agent supports both single-event and batch delivery, depending on source
configuration.

Per source, the engine can:

- read one or more events from the adapter
- send them as single deliveries
- chunk them into batch deliveries when `batch_enabled = true`
- run once for controlled validation
- run continuously with polling or stream-aware waiting

For sources that use finality-aware delivery, the agent can also reconcile
previously prepared or broadcast requests from local state by rebuilding the
canonical envelope from stored event context.

## Operator Surfaces

The CLI exposes both operational commands and evidence-oriented views.

Main commands:

- `validate`
- `status`
- `health`
- `doctor`
- `metrics`
- `diagnostics`
- `report`
- `bootstrap`
- `inspect`
- `refresh`
- `pause`
- `resume`
- `replay`
- `run`
- `checkpoint`
- `proof`
- `artifacts`

Evidence-oriented commands such as `doctor`, `diagnostics`, and `report`
produce contract-tagged JSON payloads so rollout evidence can be archived and
read consistently over time.

## Current Adapter Scope

The current adapter registry includes:

- `postgresql`
- `mysql`
- `mariadb`
- `sqlserver`
- `oracle`
- `mongodb`
- `redis`
- `scylladb`
- `db2`
- `cassandra`
- `elasticsearch`
- `sqlite`

Current capture scope by family:

- `PostgreSQL`: watermark polling, trigger-managed CDC, and logical decoding /
  `pgoutput`
- `MySQL` and `MariaDB`: watermark polling and row-based binlog CDC
- `SQL Server`: watermark polling and `change_tracking`
- `Oracle`: watermark polling and `logminer`
- `MongoDB`: watermark polling and `change_streams`
- `Redis`: explicit key-pattern polling through `SCAN`
- `ScyllaDB`, `Db2`, `Cassandra`, `Elasticsearch`, `SQLite`: polling-based
  baselines centered on tracked tables, indices, or keyspaces

Detailed per-adapter configuration and runtime guidance lives in the adapter
config references and runbooks under [docs](.).

## Service Dependencies

The notarization side of the runtime assumes the following external services:

- `Ingress API` for request preparation
- `Finality Watcher` for register / inclusion / finality tracking
- `Receipt Service` for finalized receipt reads
- `Audit API` for audit-chain reads
- chain RPC access for enterprise signing and broadcast verification

The enterprise signer model is based on:

- `submitter` as the paying account
- `submitter_permission` as the runtime hot permission
- `owner` and `active` staying outside the DB Agent runtime

## Current Boundaries

This architecture currently assumes:

- source payloads remain off-chain
- only canonical event hashes and source metadata are notarized
- local state is persisted in SQLite
- proof export is file-based under the configured `proof_dir`
- adapter-specific operational limits remain adapter-specific

Current platform-wide boundaries:

- `Wave 2` adapters are documented as supported polling-based baselines
- native CDC depth is uneven by database family and is not universal across the
  full adapter set
- the architecture supports both live chain delivery and local/offline proof
  flows depending on configuration
- the live notarization path is fail-closed when the signer / broadcaster is
  not ready

## Related Documents

- [config-reference.md](config-reference.md)
- [denotary-service-config-reference.md](denotary-service-config-reference.md)
- [storage-config-reference.md](storage-config-reference.md)
- [security-baseline.md](security-baseline.md)
- [security-minimal-roadmap.md](security-minimal-roadmap.md)
- [adapter-separation-architecture.md](adapter-separation-architecture.md)
- [wave-readiness-summary.md](wave-readiness-summary.md)
- [wave1-readiness-matrix.md](wave1-readiness-matrix.md)
- [wave2-readiness-matrix.md](wave2-readiness-matrix.md)
