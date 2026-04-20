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

Wave 2 planning is now defined for:

- Snowflake
- Redis
- IBM Db2
- Apache Cassandra
- Elasticsearch
- SQLite

Current Snowflake Wave 2 status:

- adapter registration and config surface are implemented
- live warehouse ping and configured-object introspection are implemented
- query-based snapshot polling with watermark resume is implemented
- local full-cycle proof export is implemented for the snapshot baseline
- env-gated live integration and full-cycle harnesses are implemented
- dry-run snapshot playback is implemented for pipeline validation

Current Redis Wave 2 status:

- adapter registration and config surface are implemented
- live readiness ping is implemented
- explicit key-pattern snapshot polling is implemented
- deterministic lexicographic key resume is implemented
- local full-cycle proof export is implemented for the snapshot baseline
- Docker-backed live integration and full-cycle harnesses are implemented
- restart recovery validation is implemented
- short-soak validation is implemented
- local service-outage recovery validation is implemented
- real `denotary` mainnet validation is implemented
- bounded `denotary` mainnet batch validation is implemented
- real `denotary` mainnet degraded-service recovery validation is implemented
- dry-run snapshot playback is implemented for pipeline validation

Current IBM Db2 Wave 2 status:

- adapter registration and config surface are implemented
- live readiness ping is implemented
- tracked-table introspection is implemented
- watermark snapshot polling with deterministic resume is implemented
- local full-cycle proof export is implemented
- env-gated live integration harness is implemented
- dry-run snapshot playback is implemented for pipeline validation

Current Apache Cassandra Wave 2 status:

- adapter registration and config surface are implemented
- live cluster ping is implemented
- tracked-table introspection is implemented
- watermark snapshot polling with deterministic resume is implemented
- local full-cycle proof export is implemented
- env-gated live integration harness is implemented
- dry-run snapshot playback is implemented for pipeline validation

Current Elasticsearch Wave 2 status:

- adapter registration and config surface are implemented
- live cluster ping is implemented
- tracked-index introspection is implemented
- query-based snapshot polling with deterministic resume is implemented
- local full-cycle proof export is implemented
- env-gated live integration harness is implemented
- env-gated restart and short-soak validation harness is implemented
- dry-run snapshot playback is implemented for pipeline validation

Current SQLite Wave 2 status:

- adapter registration and config surface are implemented
- file-backed readiness validation is implemented
- tracked-table introspection is implemented
- watermark snapshot polling with deterministic resume is implemented
- local full-cycle proof export is implemented
- cold restart recovery validation is implemented
- short-soak validation is implemented
- local service-outage recovery validation is implemented
- real `denotary` mainnet validation is implemented
- bounded `denotary` mainnet batch validation is implemented
- real `denotary` mainnet degraded-service recovery validation is implemented
- dry-run snapshot playback is implemented for pipeline validation

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

Current MySQL status:

- live `watermark` / snapshot baseline is implemented
- deterministic checkpoint resume is implemented
- source bootstrap / inspect / runtime signature are implemented
- shared `MySQL/MariaDB` row-based binlog CDC baseline is implemented
- live Docker-backed `MySQL` and `MariaDB` binlog validation is implemented

Current SQL Server status:

- live `watermark` / snapshot baseline is implemented
- native `change_tracking` CDC baseline is implemented
- live Docker-backed `change_tracking` validation is implemented
- local full-cycle proof export is implemented for both `watermark` and `change_tracking`

Current Oracle status:

- live `watermark` / snapshot baseline is implemented
- `logminer` live CDC baseline is implemented through a root-admin connection
- live Docker-backed `insert / update / delete` validation is implemented for `logminer`
- local full-cycle proof export is implemented for `watermark`
- local full-cycle proof export is implemented for `logminer`

Current Wave 1 mainnet status:

- PostgreSQL is validated on real `denotary` mainnet
- MySQL is validated on real `denotary` mainnet
- MariaDB is validated on real `denotary` mainnet
- SQL Server is validated on real `denotary` mainnet
- Oracle is validated on real `denotary` mainnet
- MongoDB is validated on real `denotary` mainnet

Current Wave 1 restart-recovery status:

- MySQL `binlog` restart drill is validated
- MariaDB `binlog` restart drill is validated
- SQL Server `change_tracking` restart drill is validated
- Oracle `logminer` restart drill is validated
- MongoDB `change_streams` restart drill is validated

Current Wave 1 short-soak status:

- MySQL `binlog` short-soak is validated
- MariaDB `binlog` short-soak is validated
- SQL Server `change_tracking` short-soak is validated
- Oracle `logminer` short-soak is validated
- MongoDB `change_streams` short-soak is validated

Current Wave 1 long-soak status:

- MySQL `binlog` long-soak is validated
- MariaDB `binlog` long-soak is validated
- SQL Server `change_tracking` long-soak is validated
- Oracle `logminer` long-soak is validated
- MongoDB `change_streams` long-soak is validated

Current service-outage recovery status:

- temporary `ingress` prepare outage is validated
- temporary `watcher` register outage is validated
- temporary `receipt` fetch outage is validated
- temporary `audit` fetch outage is validated

Current Wave 1 mainnet degraded-service status:

- `ingress` outage recovery is validated on real `denotary` mainnet path
- `watcher` outage recovery is validated on real `denotary` mainnet path
- `receipt` outage recovery is validated on real `denotary` mainnet path
- `audit` outage recovery is validated on real `denotary` mainnet path

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
denotary-db-agent report --config examples/agent.example.json --source pg-core-ledger --save-snapshot
denotary-db-agent artifacts --config examples/agent.example.json --source pg-core-ledger
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
- [examples/agent.secrets.env.example](examples/agent.secrets.env.example)
- [docs/config-reference.md](docs/config-reference.md)
- [docs/denotary-service-config-reference.md](docs/denotary-service-config-reference.md)
- [docs/storage-config-reference.md](docs/storage-config-reference.md)
- [docs/postgresql-config-reference.md](docs/postgresql-config-reference.md)
- [docs/mysql-config-reference.md](docs/mysql-config-reference.md)
- [docs/mariadb-config-reference.md](docs/mariadb-config-reference.md)
- [docs/sqlserver-config-reference.md](docs/sqlserver-config-reference.md)
- [docs/oracle-config-reference.md](docs/oracle-config-reference.md)
- [docs/mongodb-config-reference.md](docs/mongodb-config-reference.md)
- [docs/snowflake-config-reference.md](docs/snowflake-config-reference.md)
- [docs/redis-config-reference.md](docs/redis-config-reference.md)
- [docs/db2-config-reference.md](docs/db2-config-reference.md)
- [docs/cassandra-config-reference.md](docs/cassandra-config-reference.md)
- [docs/elasticsearch-config-reference.md](docs/elasticsearch-config-reference.md)
- [docs/sqlite-config-reference.md](docs/sqlite-config-reference.md)
- [scripts/run-live-snowflake-integration.ps1](scripts/run-live-snowflake-integration.ps1)
- [scripts/run-live-redis-integration.ps1](scripts/run-live-redis-integration.ps1)
- [scripts/run-live-db2-integration.ps1](scripts/run-live-db2-integration.ps1)
- [scripts/run-live-cassandra-integration.ps1](scripts/run-live-cassandra-integration.ps1)
- [scripts/run-live-elasticsearch-integration.ps1](scripts/run-live-elasticsearch-integration.ps1)
- [docs/architecture.md](docs/architecture.md)
- [docs/adapter-separation-architecture.md](docs/adapter-separation-architecture.md)
- [docs/wave2-roadmap.md](docs/wave2-roadmap.md)
- [docs/wave2-readiness-matrix.md](docs/wave2-readiness-matrix.md)
- [docs/wave2-redis-validation.md](docs/wave2-redis-validation.md)
- [docs/wave2-redis-validation-report.md](docs/wave2-redis-validation-report.md)
- [scripts/run-wave2-redis-validation.ps1](scripts/run-wave2-redis-validation.ps1)
- [docs/wave2-redis-runbook.md](docs/wave2-redis-runbook.md)
- [deploy/config/redis-agent.example.json](deploy/config/redis-agent.example.json)
- [docs/wave2-denotary-validation.md](docs/wave2-denotary-validation.md)
- [docs/wave2-denotary-validation-report.md](docs/wave2-denotary-validation-report.md)
- [scripts/run-wave2-denotary-validation.ps1](scripts/run-wave2-denotary-validation.ps1)
- [docs/wave2-mainnet-budget-validation.md](docs/wave2-mainnet-budget-validation.md)
- [docs/wave2-mainnet-budget-validation-report.md](docs/wave2-mainnet-budget-validation-report.md)
- [scripts/run-wave2-mainnet-budget-validation.py](scripts/run-wave2-mainnet-budget-validation.py)
- [docs/wave2-mainnet-service-outage-validation.md](docs/wave2-mainnet-service-outage-validation.md)
- [docs/wave2-mainnet-service-outage-validation-report.md](docs/wave2-mainnet-service-outage-validation-report.md)
- [scripts/run-wave2-mainnet-service-outage-validation.ps1](scripts/run-wave2-mainnet-service-outage-validation.ps1)
- [docs/wave2-sqlite-validation.md](docs/wave2-sqlite-validation.md)
- [docs/wave2-sqlite-validation-report.md](docs/wave2-sqlite-validation-report.md)
- [scripts/run-wave2-sqlite-validation.ps1](scripts/run-wave2-sqlite-validation.ps1)
- [docs/wave2-service-outage-validation.md](docs/wave2-service-outage-validation.md)
- [docs/wave2-service-outage-validation-report.md](docs/wave2-service-outage-validation-report.md)
- [scripts/run-wave2-service-outage-validation.ps1](scripts/run-wave2-service-outage-validation.ps1)
- [docs/wave2-elasticsearch-validation.md](docs/wave2-elasticsearch-validation.md)
- [scripts/run-wave2-elasticsearch-validation.ps1](scripts/run-wave2-elasticsearch-validation.ps1)
- [docs/wave2-elasticsearch-runbook.md](docs/wave2-elasticsearch-runbook.md)
- [deploy/config/elasticsearch-agent.example.json](deploy/config/elasticsearch-agent.example.json)
- [docs/wave2-snowflake-runbook.md](docs/wave2-snowflake-runbook.md)
- [deploy/config/snowflake-agent.example.json](deploy/config/snowflake-agent.example.json)
- [docs/wave2-db2-runbook.md](docs/wave2-db2-runbook.md)
- [deploy/config/db2-agent.example.json](deploy/config/db2-agent.example.json)
- [docs/wave2-cassandra-runbook.md](docs/wave2-cassandra-runbook.md)
- [deploy/config/cassandra-agent.example.json](deploy/config/cassandra-agent.example.json)
- [docs/wave2-sqlite-edge-runbook.md](docs/wave2-sqlite-edge-runbook.md)
- [deploy/config/sqlite-edge-agent.example.json](deploy/config/sqlite-edge-agent.example.json)
- [docs/operator-guide.md](docs/operator-guide.md)
- [docs/deployment-guide.md](docs/deployment-guide.md)
- [docs/denotary-env-file-runbook.md](docs/denotary-env-file-runbook.md)
- [docs/denotary-postgresql-validation-report.md](docs/denotary-postgresql-validation-report.md)
- [docs/denotary-wave1-mainnet-validation-report.md](docs/denotary-wave1-mainnet-validation-report.md)
- [docs/wave1-readiness-matrix.md](docs/wave1-readiness-matrix.md)
- [docs/wave1-recovery-validation-report.md](docs/wave1-recovery-validation-report.md)
- [docs/wave1-source-restart-validation.md](docs/wave1-source-restart-validation.md)
- [docs/wave1-source-restart-validation-report.md](docs/wave1-source-restart-validation-report.md)
- [docs/wave1-short-soak-validation.md](docs/wave1-short-soak-validation.md)
- [docs/wave1-short-soak-validation-report.md](docs/wave1-short-soak-validation-report.md)
- [docs/wave1-long-soak-validation.md](docs/wave1-long-soak-validation.md)
- [docs/wave1-long-soak-validation-report.md](docs/wave1-long-soak-validation-report.md)
- [docs/wave1-mainnet-service-outage-validation.md](docs/wave1-mainnet-service-outage-validation.md)
- [docs/wave1-mainnet-service-outage-validation-report.md](docs/wave1-mainnet-service-outage-validation-report.md)
- [docs/wave1-service-outage-validation.md](docs/wave1-service-outage-validation.md)
- [docs/wave1-service-outage-validation-report.md](docs/wave1-service-outage-validation-report.md)
- [docs/denotary-postgresql-rollout-checklist.md](docs/denotary-postgresql-rollout-checklist.md)
- [docs/jungle4-postgresql-short-soak-report.md](docs/jungle4-postgresql-short-soak-report.md)
- [docs/jungle4-postgresql-soak-runbook.md](docs/jungle4-postgresql-soak-runbook.md)
- [docs/postgresql-hot-key-rotation.md](docs/postgresql-hot-key-rotation.md)
- [docs/postgresql-recovery-scenarios.md](docs/postgresql-recovery-scenarios.md)
- [docs/wave1-recovery-validation.md](docs/wave1-recovery-validation.md)
- [scripts/run-wave1-source-restart-validation.ps1](scripts/run-wave1-source-restart-validation.ps1)
- [scripts/run-wave1-short-soak-validation.ps1](scripts/run-wave1-short-soak-validation.ps1)
- [scripts/run-wave1-long-soak-validation.ps1](scripts/run-wave1-long-soak-validation.ps1)
- [scripts/run-wave1-mainnet-service-outage-validation.ps1](scripts/run-wave1-mainnet-service-outage-validation.ps1)
- [scripts/run-wave1-service-outage-validation.ps1](scripts/run-wave1-service-outage-validation.ps1)
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
python -m denotary_db_agent --config examples/agent.example.json report --source pg-core-ledger --save-snapshot
python -m denotary_db_agent --config examples/agent.example.json artifacts --source pg-core-ledger
python -m denotary_db_agent --config examples/agent.example.json artifacts --source pg-core-ledger --latest 5
python -m denotary_db_agent --config examples/agent.example.json metrics --source pg-core-ledger
python -m denotary_db_agent --config examples/agent.example.json diagnostics --source pg-core-ledger
python -m denotary_db_agent --config examples/agent.example.json diagnostics --source pg-core-ledger --save-snapshot
python -m denotary_db_agent --config examples/agent.example.json diagnostics --source pg-core-ledger --save-snapshot --snapshot-retention 10
python -m denotary_db_agent --config examples/agent.example.json bootstrap --source pg-core-ledger
python -m denotary_db_agent --config examples/agent.example.json inspect --source pg-core-ledger
python -m denotary_db_agent --config examples/agent.example.json refresh --source pg-core-ledger
python -m denotary_db_agent --config examples/agent.example.json proof --request-id <request_id>
scripts/run-wave1-recovery-validation.ps1
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
- on POSIX hosts, `doctor` also checks whether an env-file hot key is stored in a `0600`-style secret file and raises severity when permissions are too broad
- for inline/env-backed hot keys, `doctor` also compares the derived public key with the keys on `submitter@submitter_permission`
- `doctor` also warns when the runtime permission is broader than recommended, for example multiple keys, linked accounts, waits, or a threshold above `1`
- `report --save-snapshot` exports one rollout evidence bundle that combines:
  - `doctor`
  - `metrics`
  - `diagnostics`
  - `status`
- saved `diagnostics` / `doctor` / `report` snapshots are also registered in:
  - `data/diagnostics/evidence-manifest.json`
- `artifacts` reads that manifest with optional filters by `source` and `kind`
- `artifacts --latest N` returns only the newest matching entries after filters are applied
- `artifacts --prune-missing` removes stale manifest entries whose files were already deleted from disk
- the manifest itself is now capped by `storage.evidence_manifest_retention`
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
- `refresh` can recreate a missing PostgreSQL logical slot or missing `pgoutput` publication when auto-create is enabled
- daemon mode now uses logical slot activity checks before the fallback interval elapses
- `pause` / `resume` let operators stop one source without changing the config file
- `run_once` and daemon mode now auto-refresh PostgreSQL runtime artifacts when tracked table shape changes, including `ALTER TABLE` column drift
- when `output_plugin = "pgoutput"`, `inspect` shows publication state and tracked publication tables
- `run --once` can sign `verifbill::submit` inside the agent with:
  - `broadcast_backend = "private_key_env"` and a hot key loaded from `env_file`
  - `broadcast_backend = "private_key"` for inline/debug-only WIF in config
  - `broadcast_backend = "cleos_wallet"` only as a temporary manual fallback
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
