# Elasticsearch Config Reference

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

`Elasticsearch` is part of the active `Wave 2` adapter set.

Current supported model:

- connection-shape validation
- live cluster ping through `elasticsearch`
- tracked-index introspection
- query-based snapshot polling
- deterministic checkpoint resume
- dry-run snapshot playback
- local full-cycle proof export

Native Elasticsearch CDC is not part of the current baseline.

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
  "id": "elasticsearch-orders",
  "adapter": "elasticsearch",
  "enabled": true,
  "source_instance": "search-eu-1",
  "database_name": "search",
  "include": {
    "default": ["orders"]
  },
  "connection": {
    "url": "http://127.0.0.1:9200"
  },
  "options": {
    "capture_mode": "watermark",
    "watermark_field": "updated_at",
    "commit_timestamp_field": "updated_at",
    "primary_key_field": "_id",
    "row_limit": 1000
  }
}
```

## Connection Fields

### `connection.url`

- Type: `string`
- Required: yes, unless `connection.host` or `connection.hosts` is used

### `connection.host`

- Type: `string`
- Required: no

### `connection.hosts`

- Type: `string[]`
- Required: no

### `connection.port`

- Type: `integer`
- Required: no
- Default: `9200`

### `connection.scheme`

- Type: `string`
- Required: no
- Default: `"http"`

### `connection.username`

- Type: `string`
- Required: no

### `connection.password`

- Type: `string`
- Required: no

### `connection.verify_certs`

- Type: `boolean`
- Required: no

## Include Layout

`include` maps logical namespaces to explicitly tracked index names or patterns.

Example:

```json
{
  "include": {
    "default": ["orders-*", "payments-*"]
  }
}
```

Notes:

- values must be explicit index names or patterns
- the current baseline does not support index-agnostic discovery

## Adapter Options

### `options.capture_mode`

- Type: `string`
- Supported values: `"watermark"`
- Default: `"watermark"`

### `options.watermark_field`

- Type: `string`
- Required: no
- Default: `"updated_at"`

### `options.commit_timestamp_field`

- Type: `string`
- Required: no
- Default: same as `watermark_field`

### `options.primary_key_field`

- Type: `string`
- Required: no
- Default: `"_id"`

### `options.row_limit`

- Type: `integer`
- Required: no
- Default: inherits the source `batch_size`

### `options.dry_run_events`

- Type: `array`
- Required: no
- Purpose: local adapter and pipeline testing without a live cluster

## Current Validation Status

The current `Elasticsearch` validation already confirms:

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

- native change-stream style CDC
- delete tombstone reconstruction after a document disappears between polls
- full index-agnostic discovery
- a production-scale replacement for mutation-stream capture

## Related Docs

- [security-baseline.md](security-baseline.md)
- [wave2-elasticsearch-runbook.md](wave2-elasticsearch-runbook.md)
- [wave2-elasticsearch-validation.md](wave2-elasticsearch-validation.md)
- [wave2-mainnet-budget-validation-report.md](wave2-mainnet-budget-validation-report.md)
- [wave2-mainnet-service-outage-validation-report.md](wave2-mainnet-service-outage-validation-report.md)
- [wave2-readiness-matrix.md](wave2-readiness-matrix.md)
- [../deploy/config/elasticsearch-agent.example.json](../deploy/config/elasticsearch-agent.example.json)
