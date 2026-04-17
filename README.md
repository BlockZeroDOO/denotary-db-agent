# deNotary DB Agent

`denotary-db-agent` is the CDC sidecar for enterprise database integrations.

Current scope:

- one agent process with pluggable source adapters
- canonical change-event envelope
- deterministic hashing and external reference generation
- local SQLite checkpoint and DLQ state
- deNotary `Ingress API` prepare flow
- Finality Watcher registration handoff
- CLI for run / validate / status / replay / checkpoint

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
- deterministic checkpoints resume per table
- `logical decoding / WAL CDC` is the planned next PostgreSQL upgrade step

## Quick Start

```bash
python -m venv .venv
. .venv/Scripts/activate
pip install -e .
denotary-db-agent validate --config examples/agent.example.json
```

## Config

See:

- [examples/agent.example.json](examples/agent.example.json)
- [docs/architecture.md](docs/architecture.md)
- [docs/operator-guide.md](docs/operator-guide.md)
- [docs/supported-databases.md](docs/supported-databases.md)

## Local Validation

```bash
python -m unittest discover -s tests -v
python -m denotary_db_agent --config examples/agent.example.json validate
python -m denotary_db_agent --config examples/agent.example.json status
```

Note:

- `validate` performs live adapter validation for PostgreSQL and expects a reachable database
- `status` is safe to run without a live database

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
- verifies watcher registration and checkpoint resume behavior
