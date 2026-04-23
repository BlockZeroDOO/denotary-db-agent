# Redis Config Reference

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

`Redis` is part of the active `Wave 2` adapter set.

Current supported model:

- explicit key-pattern polling through `SCAN`
- live `PING` readiness validation
- deterministic lexicographic key resume
- dry-run snapshot playback for local pipeline validation

Native keyspace notifications or stream-style CDC are not part of the current
baseline.

## Source Example

```json
{
  "id": "redis-cache-core",
  "adapter": "redis",
  "enabled": true,
  "source_instance": "cache-eu-1",
  "database_name": "db0",
  "include": {
    "0": ["orders:*", "payments:*"]
  },
  "connection": {
    "host": "127.0.0.1",
    "port": 6379
  },
  "options": {
    "capture_mode": "scan",
    "row_limit": 100,
    "scan_count": 100
  }
}
```

## Connection Fields

### `connection.host`

- Type: `string`
- Required: yes, unless `connection.url` is used

### `connection.port`

- Type: `integer`
- Required: yes, unless `connection.url` is used

### `connection.url`

- Type: `string`
- Required: no
- Purpose: optional Redis URL instead of `host` / `port`

### `connection.db`

- Type: `integer`
- Required: no
- Default: `0`
- Purpose: default database number when `include` uses `"default"` or an empty
  namespace

### `connection.username`

- Type: `string`
- Required: no

### `connection.password`

- Type: `string`
- Required: no

### `connection.ssl`

- Type: `boolean`
- Required: no
- Default: `false`

### `connection.socket_timeout_sec`

- Type: `number`
- Required: no
- Default: `10`

## Include Layout

`include` maps Redis database numbers to explicit key patterns.

Example:

```json
{
  "include": {
    "0": ["orders:*"],
    "1": ["session:*"]
  }
}
```

Notes:

- keys are Redis database numbers serialized as strings
- values must be explicit key patterns
- the current baseline does not support an implicit "all keys everywhere" mode

## Adapter Options

### `options.capture_mode`

- Type: `string`
- Supported values: `"scan"`
- Default: `"scan"`

### `options.row_limit`

- Type: `integer`
- Required: no
- Default: inherits the source `batch_size`
- Purpose: maximum number of keys emitted per polling cycle

### `options.scan_count`

- Type: `integer`
- Required: no
- Default: `100`
- Purpose: advisory `SCAN` batch size during key enumeration

### `options.dry_run_events`

- Type: `array`
- Required: no
- Purpose: local adapter and pipeline testing without a live Redis instance

## Current Validation Status

The current `Redis` validation already confirms:

- Docker-backed live baseline validation
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

- keyspace notification CDC
- Redis Streams consumption
- wildcard full-database discovery
- delete tombstone reconstruction after a key disappears between polls

## Related Docs

- [wave2-redis-runbook.md](wave2-redis-runbook.md)
- [wave2-redis-validation.md](wave2-redis-validation.md)
- [wave2-redis-validation-report.md](wave2-redis-validation-report.md)
- [wave2-readiness-matrix.md](wave2-readiness-matrix.md)
