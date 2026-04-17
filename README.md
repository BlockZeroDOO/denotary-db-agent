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
- CLI for run / validate / status / replay / checkpoint / proof

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
- deterministic checkpoints resume per table
- full single-event cycle is implemented inside the agent:
  - prepare
  - sign and broadcast
  - watcher inclusion/finality
  - receipt + audit proof-chain export
- `logical decoding / WAL CDC` remains the next PostgreSQL upgrade step

## Quick Start

```bash
python -m venv .venv
. .venv/Scripts/activate
pip install -e .
denotary-db-agent status --config examples/agent.example.json
denotary-db-agent run --config examples/agent.example.json --once
denotary-db-agent run --config examples/agent.example.json --interval-sec 5
```

## Config

See:

- [examples/agent.example.json](examples/agent.example.json)
- [docs/architecture.md](docs/architecture.md)
- [docs/operator-guide.md](docs/operator-guide.md)
- [docs/supported-databases.md](docs/supported-databases.md)
- [docs/verifbill-permission-model.md](docs/verifbill-permission-model.md)
- [docs/verifbill-permission-commands.md](docs/verifbill-permission-commands.md)

## Local Validation

```bash
python -m unittest discover -s tests -v
python -m denotary_db_agent --config examples/agent.example.json status
python -m denotary_db_agent --config examples/agent.example.json proof --request-id <request_id>
```

Note:

- `validate` performs live adapter validation for PostgreSQL and expects reachable deNotary services plus chain RPC when they are configured
- `status` is safe to run without a live database
- `run --once` uses the configured `dnanchor` private key to sign `verifbill::submit` inside the agent
- `run` without `--once` keeps the agent in daemon mode and, for PostgreSQL trigger sources, waits on `LISTEN/NOTIFY` before the fallback interval elapses
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
- verifies watcher registration, checkpoint resume behavior, and cleanup of processed trigger CDC rows

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
