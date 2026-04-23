# IBM Db2 Config Reference

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

`IBM Db2` is part of the active `Wave 2` adapter set.

Current supported model:

- connection-shape validation
- live ping through `ibm_db`
- tracked-table introspection
- watermark snapshot polling
- deterministic checkpoint resume
- dry-run snapshot playback
- local full-cycle proof export

Native Db2 CDC is not part of the current baseline.

## Security Note

For enterprise production use:

- keep `Ingress`, `Watcher`, `Receipt`, and `Audit` in the same trusted
  deployment boundary as the agent
- use a dedicated hot permission such as `dnanchor`
- do not use `owner` or `active`
- keep signer material in `env_file` or a secret mount

Reference:

- [security-baseline.md](security-baseline.md)
- [denotary-env-file-runbook.md](denotary-env-file-runbook.md)

## Source Example

```json
{
  "id": "db2-core-ledger",
  "adapter": "db2",
  "enabled": true,
  "source_instance": "erp-eu-1",
  "database_name": "LEDGER",
  "include": {
    "DB2INST1": ["INVOICES", "PAYMENTS"]
  },
  "connection": {
    "host": "127.0.0.1",
    "port": 50000,
    "username": "db2inst1",
    "password": "secret",
    "database": "LEDGER"
  },
  "options": {
    "capture_mode": "watermark",
    "watermark_column": "UPDATED_AT",
    "commit_timestamp_column": "UPDATED_AT",
    "row_limit": 1000
  }
}
```

## Connection Fields

### `connection.host`

- Type: `string`
- Required: yes

### `connection.port`

- Type: `integer`
- Required: yes

### `connection.username`

- Type: `string`
- Required: yes

### `connection.password`

- Type: `string`
- Required: yes

### `connection.database`

- Type: `string`
- Required: yes

## Include Layout

`include` maps schemas to explicitly tracked tables.

Example:

```json
{
  "include": {
    "DB2INST1": ["INVOICES", "PAYMENTS"],
    "REPORTING": ["DAILY_TOTALS"]
  }
}
```

Notes:

- keys are schema names
- values must be explicit table names
- the baseline normalizes schema and table names to uppercase

## Adapter Options

### `options.capture_mode`

- Type: `string`
- Supported values: `"watermark"`
- Default: `"watermark"`

### `options.watermark_column`

- Type: `string`
- Required: no
- Default: `"UPDATED_AT"`

### `options.commit_timestamp_column`

- Type: `string`
- Required: no
- Default: same as `watermark_column`

### `options.primary_key_columns`

- Type: `string[]`
- Required: no

### `options.primary_key_column`

- Type: `string`
- Required: no

### `options.row_limit`

- Type: `integer`
- Required: no
- Default: inherits the source `batch_size`

### `options.dry_run_events`

- Type: `array`
- Required: no
- Purpose: local adapter and pipeline testing without a live Db2 instance

## Current Validation Status

The current `Db2` validation already confirms:

- env-gated live baseline validation
- local Docker-backed validation
- local full-cycle proof export
- restart recovery validation
- short-soak validation
- bounded long-soak validation
- local service-outage recovery validation
- real `denotary` mainnet happy-path validation
- bounded mainnet budget validation
- real mainnet degraded-service recovery validation

## Current Limits

The current baseline does not provide:

- native Db2 CDC integration
- delete tombstone reconstruction after a row disappears between polls
- wildcard table discovery
- a production-scale replacement for native log-based capture

## Related Docs

- [security-baseline.md](security-baseline.md)
- [wave2-db2-runbook.md](wave2-db2-runbook.md)
- [wave2-db2-validation.md](wave2-db2-validation.md)
- [wave2-mainnet-budget-validation-report.md](wave2-mainnet-budget-validation-report.md)
- [wave2-mainnet-service-outage-validation-report.md](wave2-mainnet-service-outage-validation-report.md)
- [wave2-readiness-matrix.md](wave2-readiness-matrix.md)
- [../deploy/config/db2-agent.example.json](../deploy/config/db2-agent.example.json)
