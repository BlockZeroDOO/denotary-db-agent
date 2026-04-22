# deNotary DB Agent Config Reference

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

This document is now the entry point for configuration documentation.

Use it together with:

- [examples/agent.example.json](../examples/agent.example.json)
- [examples/agent.secrets.env.example](../examples/agent.secrets.env.example)
- [README.md](../README.md)
- [docs/operator-guide.md](operator-guide.md)

## Config Map

The full agent config is split into:

1. top-level agent structure
2. deNotary service and signer configuration
3. storage and local runtime state
4. source-specific configuration per database

## Core Documents

- [denotary-service-config-reference.md](denotary-service-config-reference.md)
- [storage-config-reference.md](storage-config-reference.md)

## Database Source Documents

- [postgresql-config-reference.md](postgresql-config-reference.md)
- [mysql-config-reference.md](mysql-config-reference.md)
- [mariadb-config-reference.md](mariadb-config-reference.md)
- [sqlserver-config-reference.md](sqlserver-config-reference.md)
- [oracle-config-reference.md](oracle-config-reference.md)
- [mongodb-config-reference.md](mongodb-config-reference.md)
- [redis-config-reference.md](redis-config-reference.md)
- [scylladb-config-reference.md](scylladb-config-reference.md)
- [db2-config-reference.md](db2-config-reference.md)
- [cassandra-config-reference.md](cassandra-config-reference.md)
- [elasticsearch-config-reference.md](elasticsearch-config-reference.md)
- [sqlite-config-reference.md](sqlite-config-reference.md)

## Top-Level Shape

The config file is a single JSON object:

```json
{
  "agent_name": "denotary-db-agent",
  "log_level": "INFO",
  "denotary": {},
  "storage": {},
  "sources": []
}
```

## Top-Level Fields

### `agent_name`

- Type: `string`
- Required: no
- Default: `"denotary-db-agent"`
- Purpose: logical name for logs, diagnostics, and snapshots

### `log_level`

- Type: `string`
- Required: no
- Default: `"INFO"`
- Typical values:
  - `DEBUG`
  - `INFO`
  - `WARNING`
  - `ERROR`

### `denotary`

- Type: `object`
- Required: yes
- Reference:
  - [denotary-service-config-reference.md](denotary-service-config-reference.md)

### `storage`

- Type: `object`
- Required: yes
- Reference:
  - [storage-config-reference.md](storage-config-reference.md)

### `sources`

- Type: `array`
- Required: yes
- Must be non-empty: yes
- Purpose: one or more database sources handled by the agent

## Common `sources[]` Fields

Each entry in `sources` is one logical input source.

```json
{
  "id": "pg-core-ledger",
  "adapter": "postgresql",
  "enabled": true,
  "source_instance": "erp-eu-1",
  "database_name": "ledger",
  "include": {
    "public": ["invoices", "payments"]
  },
  "exclude": {},
  "checkpoint_policy": "after_ack",
  "backfill_mode": "full",
  "batch_enabled": false,
  "batch_size": 100,
  "flush_interval_ms": 1000,
  "connection": {},
  "options": {}
}
```

### `id`

- Type: `string`
- Required: yes
- Purpose: stable source identifier used in state, logs, diagnostics, and CLI commands

### `adapter`

- Type: `string`
- Required: yes
- Supported values:
  - `"postgresql"`
  - `"mysql"`
  - `"mariadb"`
  - `"sqlserver"`
  - `"oracle"`
  - `"mongodb"`
  - `"redis"`
  - `"scylladb"`
  - `"db2"`
  - `"cassandra"`
  - `"elasticsearch"`
  - `"sqlite"`

### `enabled`

- Type: `boolean`
- Required: no
- Default: `true`

### `source_instance`

- Type: `string`
- Required: yes
- Purpose: logical source installation or environment name

### `database_name`

- Type: `string`
- Required: yes
- Purpose: business/database label used in the canonical envelope

### `include`

- Type: `object<string, string[]>`
- Required: no
- Default: `{}`
- Purpose: schemas/namespaces and tracked tables or collections

### `exclude`

- Type: `object<string, string[]>`
- Required: no
- Default: `{}`
- Purpose: optional exclusions inside included namespaces

### `checkpoint_policy`

- Type: `string`
- Required: no
- Default: `"after_ack"`

### `backfill_mode`

- Type: `string`
- Required: no
- Default: `"none"`
- Common values:
  - `"none"`
  - `"full"`

### `batch_enabled`

- Type: `boolean`
- Required: no
- Default: `false`

### `batch_size`

- Type: `integer`
- Required: no
- Default: `100`

### `flush_interval_ms`

- Type: `integer`
- Required: no
- Default: `1000`

### `connection`

- Type: `object`
- Required: no
- Default: `{}`
- Purpose: DB-specific connection settings

### `options`

- Type: `object`
- Required: no
- Default: `{}`
- Purpose: adapter-specific runtime settings

## Source-Specific References

- PostgreSQL:
  - [postgresql-config-reference.md](postgresql-config-reference.md)
- MySQL:
  - [mysql-config-reference.md](mysql-config-reference.md)
- MariaDB:
  - [mariadb-config-reference.md](mariadb-config-reference.md)
- SQL Server:
  - [sqlserver-config-reference.md](sqlserver-config-reference.md)
- Oracle:
  - [oracle-config-reference.md](oracle-config-reference.md)
- MongoDB:
  - [mongodb-config-reference.md](mongodb-config-reference.md)
- Redis:
  - [redis-config-reference.md](redis-config-reference.md)
- ScyllaDB:
  - [scylladb-config-reference.md](scylladb-config-reference.md)
- IBM Db2:
  - [db2-config-reference.md](db2-config-reference.md)
- Apache Cassandra:
  - [cassandra-config-reference.md](cassandra-config-reference.md)
- Elasticsearch:
  - [elasticsearch-config-reference.md](elasticsearch-config-reference.md)
- SQLite:
  - [sqlite-config-reference.md](sqlite-config-reference.md)
