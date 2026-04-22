# Adapter Separation Architecture

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

## Goal

`denotary-db-agent` should treat database capture and deNotary notarization as two separate layers:

1. `data source layer`
2. `notarization pipeline layer`

This keeps PostgreSQL-specific CDC/runtime behavior isolated and makes it faster to plug in MySQL, MariaDB, SQL Server, Oracle, or MongoDB without copying delivery/finality logic.

## Current Split

The codebase now separates responsibilities into three runtime areas:

### 1. Source adapters

Files:

- `denotary_db_agent/adapters/*`
- `denotary_db_agent/source_runtime.py`

Responsibilities:

- validate source connectivity
- discover capabilities
- bootstrap source-specific artifacts
- emit `ChangeEvent`
- serialize source checkpoint tokens
- manage source-specific CDC runtime state

Important rule:

- adapters know how to read changes from a source
- adapters do **not** know how to prepare deNotary actions, broadcast transactions, or wait for finality

### 2. Notarization pipeline

File:

- `denotary_db_agent/pipeline.py`

Responsibilities:

- canonicalize `ChangeEvent` into deNotary payloads
- call `Ingress API`
- register with `Finality Watcher`
- broadcast prepared actions
- finalize through receipt + audit
- export proof bundles
- recover duplicate deliveries

Important rule:

- the pipeline knows how to turn a source event into a proof
- the pipeline does **not** know how a database produces that event

### 3. Orchestrator

File:

- `denotary_db_agent/engine.py`

Responsibilities:

- build runtimes
- run loops
- pause/resume sources
- health/doctor/metrics/diagnostics/report views
- apply retention
- coordinate source adapters with the notarization pipeline

Important rule:

- `AgentEngine` should stay a coordinator, not the place where adapter-specific CDC logic grows

## Target Pattern For New Databases

When adding a new database, the normal path should be:

1. implement adapter in `denotary_db_agent/adapters/<db>.py`
2. expose it in `adapters/registry.py`
3. make it emit standard `ChangeEvent`
4. reuse the existing pipeline unchanged

That means most new database work should stay inside:

- connection handling
- CDC/snapshot capture
- checkpoint format
- source-specific bootstrap/inspect/refresh

And should avoid touching:

- `transport.py`
- `pipeline.py`
- proof export logic
- watcher/receipt/audit handling

## Boundary Contract

The boundary between both layers is the `ChangeEvent`.

Adapters are expected to output:

- source identity
- database/schema/table
- operation
- primary key
- before/after payloads
- metadata
- checkpoint token

The pipeline takes that event and owns everything after that boundary.

## Why This Matters

This split gives us:

- faster onboarding of new databases
- less duplicated delivery/finality code
- safer PostgreSQL changes, because source work is isolated
- clearer testing:
  - adapter tests focus on CDC behavior
  - pipeline tests focus on delivery/proof behavior

## Next Recommended Refactor Direction

To push this separation further, future work should prefer:

- source-specific docs under `docs/sources/`
- adapter contract tests shared across non-PostgreSQL adapters
- optional source capability flags for:
  - snapshot only
  - polling CDC
  - trigger CDC
  - logical/stream CDC
- database-neutral source fixtures for contract testing

The main architectural rule going forward:

- new data sources should extend the **source layer**
- not reimplement the **notarization pipeline**

## Shared Adapter Contract Harness

The shared harness for scaffold adapters now lives in:

- `tests/test_adapter_contract.py`

Its role is to keep non-PostgreSQL adapters aligned on the same minimal source-runtime
contract before any database-specific CDC implementation lands.

The harness verifies that every scaffold adapter:

- advertises capabilities consistently
- validates required connection fields
- supports `bootstrap()` and `inspect()` as source-only operations
- returns an empty snapshot safely in scaffold mode
- serializes checkpoint progress consistently
- raises a clear `NotImplementedError` for unimplemented CDC streaming

That gives us one reusable baseline for MySQL, MariaDB, SQL Server, Oracle, MongoDB,
and future adapters without coupling those tests to PostgreSQL behavior.

Capabilities now also declare:

- `capture_modes`
- `default_capture_mode`
- `cdc_modes`
- `bootstrap_requirements`
- `checkpoint_strategy`
- `activity_model`

This makes adapter behavior more declarative for operator tooling and for future adapter
work, instead of forcing every new database integration to infer those expectations from
implementation details alone.

These two newer flags make CDC-capable adapters easier to reason about consistently:

- `checkpoint_strategy`
  - examples: `table_watermark`, `binlog_cursor`, `lsn_cursor`, `resume_token`, `logminer_scn`
- `activity_model`
  - examples: `polling`, `notification_polling`, `stream`

That gives `inspect` and future operator automation a database-neutral way to answer:

- what kind of cursor/checkpoint this source persists
- whether the source waits on notifications/streams or is purely poll-based

For CDC-capable adapters, the `cdc` payload itself should also carry this shared contract so
`inspect`, `health`, `doctor`, and `diagnostics` can reason about sources without parsing
database-specific fields first.

`BaseAdapter` now also owns the generic source-runtime choice between snapshot and CDC:

- `capture_mode()`
- `is_cdc_mode()`
- `iter_events(checkpoint)`
- `should_wait_for_activity()`

And it now also owns the common shape of source-facing bootstrap and inspect payloads:

- `build_bootstrap_result(...)`
- `build_inspect_result(...)`

That keeps `AgentEngine` out of source-specific mode branching such as
`trigger/logical/change_streams`, so future CDC-capable adapters can plug in by declaring
capabilities instead of extending orchestrator conditionals. It also keeps adapters focused
on their tracked objects and CDC state instead of hand-assembling the same bootstrap/inspect
JSON envelopes over and over again.

Those shared envelopes now also include a database-neutral `tracked_objects` list and
`tracked_object_count`, so operator tooling can reason about source coverage without caring
whether a given adapter exposes tables, collections, or other source-native object types.

`AgentEngine` now also turns that into a shared `coverage` view in operator outputs, so
`doctor`, `diagnostics`, and later rollout/report tooling can consume one stable coverage
contract instead of branching on `tracked_tables` vs `tracked_collections`.

`report` is now moving onto the same model as well through a shared `source_reports` view,
so evidence bundles can carry one stable source snapshot instead of reassembling coverage,
CDC contract, and runtime state independently in each report consumer.

That report bundle is now explicitly versioned through `report_contract`, so snapshot
export and downstream evidence tooling can key off a stable contract version instead of
inferring schema changes from arbitrary JSON shape differences.

The same direction now applies to `doctor` and `diagnostics` through matching
`doctor_contract` and `diagnostics_contract` blocks, so all evidence artifacts exported by
the agent can be indexed and consumed through one consistent contract-version mechanism.

The contract metadata itself is now built from one shared helper rather than duplicated
inline, which keeps artifact versioning aligned as the evidence/export surface grows.
