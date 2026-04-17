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
