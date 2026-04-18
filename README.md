# deNotary DB Agent

`denotary-db-agent` is the CDC sidecar for enterprise database integrations.

Current scope:

- one agent process with pluggable source adapters
- canonical change-event envelope
- deterministic hashing and external reference generation
- local SQLite checkpoint and DLQ state
- deNotary `Ingress API` prepare flow
- built-in enterprise signing and broadcast through `verifbill`
- Finality Watcher registration and inclusion/finality updates
- receipt / audit-chain retrieval and local proof bundle export
- CLI for run / validate / status / health / diagnostics / bootstrap / inspect / refresh / pause / resume / replay / checkpoint / proof
- CLI for run / validate / status / health / doctor / metrics / diagnostics / bootstrap / inspect / refresh / pause / resume / replay / checkpoint / proof

The first wave of database targets is:

- PostgreSQL
- MySQL
- MariaDB
- Microsoft SQL Server
- Oracle Database
- MongoDB

This initial implementation focuses on the platform layer and adapter contract. Real CDC transport details per database remain adapter-specific work on top of this package scaffold.

Current PostgreSQL status:

- live `snapshot + watermark polling` baseline is implemented
- trigger-managed CDC with `LISTEN/NOTIFY` wakeups is implemented
- processed trigger events can be cleaned up automatically after checkpoint advance
- logical decoding / WAL CDC through a logical replication slot is implemented
- logical mode now uses a transaction-safe cursor and does not drop later rows from the same transaction when `row_limit` is small
- logical mode now supports both:
  - `output_plugin = "test_decoding"` for live decoded polling
  - `output_plugin = "pgoutput"` for publication-managed live binary polling
- deterministic checkpoints resume per table
- full single-event cycle is implemented inside the agent:
  - prepare
  - sign and broadcast
  - watcher inclusion/finality
  - receipt + audit proof-chain export
- current logical polling supports both PostgreSQL `test_decoding` and `pgoutput`
- `pgoutput` supports both:
  - default `logical_runtime_mode = "stream"` via low-level replication protocol
  - optional `logical_runtime_mode = "peek"` via `pg_logical_slot_peek_binary_changes()`
- bounded streaming now includes standby-status feedback using the last safe acknowledged LSN
- in stream mode, post-delivery checkpoint advancement now updates the active replication session ack before close

## Quick Start

```bash
python -m venv .venv
. .venv/Scripts/activate
pip install -e .
denotary-db-agent status --config examples/agent.example.json
denotary-db-agent health --config examples/agent.example.json
denotary-db-agent doctor --config examples/agent.example.json --source pg-core-ledger
denotary-db-agent doctor --config examples/agent.example.json --source pg-core-ledger --save-snapshot
denotary-db-agent doctor --config examples/agent.example.json --source pg-core-ledger --strict
denotary-db-agent metrics --config examples/agent.example.json --source pg-core-ledger
denotary-db-agent diagnostics --config examples/agent.example.json --source pg-core-ledger
denotary-db-agent diagnostics --config examples/agent.example.json --source pg-core-ledger --save-snapshot
denotary-db-agent diagnostics --config examples/agent.example.json --source pg-core-ledger --save-snapshot --snapshot-retention 10
denotary-db-agent bootstrap --config examples/agent.example.json --source pg-core-ledger
denotary-db-agent inspect --config examples/agent.example.json --source pg-core-ledger
denotary-db-agent refresh --config examples/agent.example.json --source pg-core-ledger
denotary-db-agent pause --config examples/agent.example.json --source pg-core-ledger
denotary-db-agent resume --config examples/agent.example.json --source pg-core-ledger
denotary-db-agent run --config examples/agent.example.json --once
denotary-db-agent run --config examples/agent.example.json --interval-sec 5
```

## Config

See:

- [examples/agent.example.json](examples/agent.example.json)
- [docs/config-reference.md](docs/config-reference.md)
- [docs/architecture.md](docs/architecture.md)
- [docs/operator-guide.md](docs/operator-guide.md)
- [docs/deployment-guide.md](docs/deployment-guide.md)
- [docs/supported-databases.md](docs/supported-databases.md)
- [docs/verifbill-permission-model.md](docs/verifbill-permission-model.md)
- [docs/verifbill-permission-commands.md](docs/verifbill-permission-commands.md)

## Local Validation

```bash
python -m unittest discover -s tests -v
python -m denotary_db_agent --config examples/agent.example.json status
python -m denotary_db_agent --config examples/agent.example.json health
python -m denotary_db_agent --config examples/agent.example.json doctor --source pg-core-ledger
python -m denotary_db_agent --config examples/agent.example.json doctor --source pg-core-ledger --save-snapshot
python -m denotary_db_agent --config examples/agent.example.json doctor --source pg-core-ledger --strict
python -m denotary_db_agent --config examples/agent.example.json metrics --source pg-core-ledger
python -m denotary_db_agent --config examples/agent.example.json diagnostics --source pg-core-ledger
python -m denotary_db_agent --config examples/agent.example.json diagnostics --source pg-core-ledger --save-snapshot
python -m denotary_db_agent --config examples/agent.example.json diagnostics --source pg-core-ledger --save-snapshot --snapshot-retention 10
python -m denotary_db_agent --config examples/agent.example.json bootstrap --source pg-core-ledger
python -m denotary_db_agent --config examples/agent.example.json inspect --source pg-core-ledger
python -m denotary_db_agent --config examples/agent.example.json refresh --source pg-core-ledger
python -m denotary_db_agent --config examples/agent.example.json proof --request-id <request_id>
```

Note:

- `validate` performs live adapter validation for PostgreSQL and expects reachable deNotary services plus chain RPC when they are configured
- `status` is safe to run without a live database
- `health` shows local source state and best-effort health for configured chain/receipt/audit services
- `health` now also surfaces logical slot warnings such as publication drift, REPLICA IDENTITY drift, and WAL lag thresholds
- `health` now classifies each source as `healthy`, `degraded`, `critical`, or `error`
- `doctor` is the compact live preflight report for deploy readiness:
  - config paths
  - HTTP reachability for deNotary services
  - chain RPC readiness
  - signer hot-permission readiness
  - per-source connectivity and tracked table visibility
- `doctor --save-snapshot` stores the same preflight report under the local runtime directory for rollout evidence
- `doctor --strict` exits nonzero only for `critical` / `error` overall severity, so it can be wired into CI/CD and service pre-start checks without failing on normal `degraded` warnings
- `metrics` gives a compact export-friendly summary of source counters, backlog indicators, stream state, and severity
- `diagnostics` gives a compact stream/logical-slot focused report per source
- `diagnostics --save-snapshot` writes the report to a timestamped JSON file under the local runtime directory
- snapshot retention defaults to keeping the newest `20` matching diagnostics files per source when saving snapshots
- use `--snapshot-retention <N>` to override that retention window for automation or tighter local disk budgets
- runtime artifact retention can also be configured in `storage`:
  - `proof_retention`
  - `delivery_retention`
  - `dlq_retention`
- when enabled, proof metadata and proof bundle JSON files are pruned together after the configured per-source limit is exceeded
- daemon mode can also write periodic diagnostics snapshots automatically via:
  - `diagnostics_snapshot_interval_sec`
  - `diagnostics_snapshot_retention`
- `inspect` / `health` now surface PostgreSQL stream runtime stats such as active session state, acknowledged LSN, reconnect counters, reconnect reasons, and last stream errors
- `inspect` now also exposes a short ring buffer of recent PostgreSQL stream errors
- after repeated stream failures, PostgreSQL `pgoutput` can temporarily fall back from `stream` to `peek`
- after fallback expires, PostgreSQL `pgoutput` re-enters `stream` with a short probation window visible in `inspect/health`
- `bootstrap` installs or refreshes source-side runtime artifacts such as PostgreSQL trigger CDC objects, logical replication slot setup, and `pgoutput` publications
- `inspect` shows tracked tables, selected columns, and live PostgreSQL CDC state for a source
- for `pgoutput`, `inspect` also shows whether publication tables are in sync with tracked tables
- for `pgoutput`, `inspect` also shows REPLICA IDENTITY state for tracked logical tables
- for logical mode, `inspect` also shows slot backlog indicators such as pending changes and WAL lag bytes
- `refresh` forces runtime artifact refresh and stores the new runtime signature
- `refresh` can repair PostgreSQL publication drift when `pgoutput` publication tables no longer match tracked tables
- `refresh` can also repair PostgreSQL REPLICA IDENTITY drift when `replica_identity_full = true`
- daemon mode now uses logical slot activity checks before the fallback interval elapses
- `pause` / `resume` let operators stop one source without changing the config file
- `run_once` and daemon mode now auto-refresh PostgreSQL runtime artifacts when tracked table shape changes, including `ALTER TABLE` column drift
- when `output_plugin = "pgoutput"`, `inspect` shows publication state and tracked publication tables
- `run --once` uses the configured `dnanchor` private key to sign `verifbill::submit` inside the agent
- `run` without `--once` keeps the agent in daemon mode and, for PostgreSQL trigger sources, waits on `LISTEN/NOTIFY` before the fallback interval elapses
- for PostgreSQL `pgoutput`, bounded replication-protocol streaming is now the default runtime path
- `logical_runtime_mode = "peek"` remains available as an explicit fallback
- streaming feedback only reports already acknowledged LSNs, so it doesn't replace post-delivery checkpoint advancement
- in stream mode, that post-delivery checkpoint advancement feeds the active session ack directly; SQL slot advance remains the fallback for non-stream paths
- stream mode now classifies reconnect/runtime failures, exposes `stream_last_error*` and `stream_last_reconnect_reason`, and applies bounded reconnect cooldown after repeated failures
- finalized receipts and proof chains are exported under `storage.proof_dir`

## Live PostgreSQL Harness

For a reproducible live PostgreSQL integration pass:

- [deploy/postgres-live/docker-compose.yml](deploy/postgres-live/docker-compose.yml)
- [deploy/postgres-live/init.sql](deploy/postgres-live/init.sql)
- [scripts/run-live-postgres-integration.ps1](scripts/run-live-postgres-integration.ps1)
- [scripts/run-live-postgres-integration.sh](scripts/run-live-postgres-integration.sh)

This harness:

- starts PostgreSQL in Docker
- creates test `invoices` and `payments` tables
- inserts live rows
- runs `denotary-db-agent` against the live database
- verifies watcher registration, checkpoint resume behavior, cleanup of processed trigger CDC rows, and logical decoding capture

## Deployment

Production packaging templates are included for:

- `systemd`
- Windows Service
- Docker Compose
- Linux / Windows / Docker config packs

See:

- [docs/deployment-guide.md](docs/deployment-guide.md)
- [deploy/systemd/denotary-db-agent.service.example](deploy/systemd/denotary-db-agent.service.example)
- [deploy/docker-compose.example.yml](deploy/docker-compose.example.yml)
- [scripts/install-windows-service.ps1](scripts/install-windows-service.ps1)
- [scripts/run-windows-service.ps1](scripts/run-windows-service.ps1)
- [deploy/config/linux-agent.example.json](deploy/config/linux-agent.example.json)
- [deploy/config/windows-agent.example.json](deploy/config/windows-agent.example.json)
- [deploy/config/docker-agent.example.json](deploy/config/docker-agent.example.json)

## Full-Cycle Result

For PostgreSQL `single` events, the plugin now covers the end-to-end path without `cleos` or other manual broadcast tools:

1. read and canonicalize the database change
2. call `Ingress API /v1/single/prepare`
3. sign and broadcast `verifbill::submit` with `submitter@submitter_permission`
4. register and advance the request in `Finality Watcher`
5. wait for finalized and inclusion-verified state
6. fetch receipt from `Receipt Service`
7. fetch proof chain from `Audit API`
8. export a local proof bundle JSON per request

## Enterprise Signer Permission

The recommended enterprise runtime permission is:

- `submitter_permission = "dnanchor"`

Use a dedicated custom permission for:

- `verifbill::submit`
- `verifbill::submitroot`

and keep:

- `owner` offline
- `active` off the application server
- token transfer permissions out of the DB Agent hot key
