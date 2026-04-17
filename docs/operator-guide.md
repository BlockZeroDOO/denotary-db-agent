# deNotary DB Agent Operator Guide

## Commands

### Validate

```bash
denotary-db-agent --config examples/agent.example.json validate
```

Checks:

- config shape
- adapter availability
- basic source connection fields

### Run One Pass

```bash
denotary-db-agent --config examples/agent.example.json run --once
```

In the current scaffold this is intended for:

- dry-run events
- snapshot/bootstrap testing
- local integration with `Ingress API` and `Finality Watcher`

### Status

```bash
denotary-db-agent --config examples/agent.example.json status
```

Returns:

- source ids
- configured adapters
- current checkpoints
- delivery count
- DLQ count

### Replay / Reset Checkpoint

```bash
denotary-db-agent --config examples/agent.example.json replay --source pg-core-ledger
```

or:

```bash
denotary-db-agent --config examples/agent.example.json checkpoint --source pg-core-ledger --reset
```

## Current Adapter Targets

- PostgreSQL: logical decoding / WAL plan
- MySQL: row-based binlog plan
- MariaDB: MariaDB binlog profile
- SQL Server: CDC / Change Tracking plan
- Oracle: redo / LogMiner plan
- MongoDB: change streams plan

## Permissions Planning

Per-database operator docs still need to be expanded in later waves, but the expected direction is:

- read-only access to the target objects
- CDC-specific privileges where required
- metadata/catalog visibility for included schemas and tables

## State Files

The local SQLite state file stores:

- source checkpoints
- delivery attempts
- DLQ records

Back up this file if replay/recovery history matters operationally.

