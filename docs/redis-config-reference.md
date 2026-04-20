# Redis Config Reference

`Redis` is part of the `Wave 2` roadmap.

Current baseline:

- explicit key-pattern snapshot polling
- live `PING` readiness validation
- deterministic lexicographic key resume
- dry-run snapshot playback for local pipeline validation

Native keyspace notifications or stream-style CDC are not implemented yet.

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
- Purpose: default database number when `include` uses `"default"` or an empty namespace

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
- the current baseline does not support an implicit “all keys everywhere” mode

## Adapter Options

### `options.capture_mode`

- Value today: `"scan"`

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

## Current Validation Scope

Implemented now:

- adapter registration
- config surface
- live readiness ping
- snapshot polling baseline
- deterministic checkpoint resume
- local full-cycle proof export
- Docker-backed live integration and full-cycle harnesses

Planned next:

- optional keyspace notification CDC path if justified

See also:

- [wave2-redis-validation.md](wave2-redis-validation.md)
- [wave2-redis-validation-report.md](wave2-redis-validation-report.md)
- [wave2-redis-runbook.md](wave2-redis-runbook.md)
