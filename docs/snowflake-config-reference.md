# Snowflake Config Reference

This document describes the current `Snowflake` configuration surface for `denotary-db-agent`.

## Current Status

`Snowflake` is the first `Wave 2` target now entering implementation.

Implemented now:

- adapter registry support
- shared adapter contract integration
- connection-shape validation
- live warehouse ping through the Snowflake connector
- configured tracked table and view planning
- live tracked-object introspection for configured objects
- shared `bootstrap` / `inspect` payloads for tracked Snowflake objects
- query-based snapshot polling over configured objects
- deterministic checkpoint resume by watermark + primary key
- deterministic runtime signature for tracked-object configuration
- dry-run snapshot playback for pipeline validation
- declared snapshot / watermark baseline capabilities

Planned next:

- local full-cycle proof export
- mainnet happy-path validation
- next `Wave 2` adapter after `Snowflake`

## `connection`

```json
"connection": {
  "account": "acme-org.eu-central-1",
  "username": "denotary",
  "database": "ANALYTICS",
  "schema": "PUBLIC",
  "warehouse": "NOTARY_WH"
}
```

### Supported keys

- `account`: `string`, required
- `username`: `string`, required
- `database`: `string`, required
- `schema`: `string`, required
- `warehouse`: `string`, required
- `password`: `string`, optional, deployment-specific
- `role`: `string`, optional

## `options`

### Supported now

- `capture_mode`
  - supported values: `"watermark"`
- `tracked_object_type`
  - supported values: `"table"` or `"view"`
  - default: `"table"`
- `watermark_column`
  - default incremental cursor column for snapshot polling
- `commit_timestamp_column`
  - timestamp field included in canonical metadata
- `primary_key_column`
  - single-column shortcut, default: `"id"`
- `primary_key_columns`
  - optional array override for multi-column keys
- `row_limit`
- `dry_run_events`
  - optional local validation payloads for non-live pipeline testing

## Recommended Starting Point

```json
{
  "id": "snowflake-analytics-core",
  "adapter": "snowflake",
  "enabled": true,
  "source_instance": "analytics-us-1",
  "database_name": "analytics",
  "include": {
    "PUBLIC": ["ORDERS", "INVOICES"]
  },
  "checkpoint_policy": "after_ack",
  "backfill_mode": "full",
  "batch_enabled": false,
  "connection": {
    "account": "acme-org.eu-central-1",
    "username": "denotary",
    "database": "ANALYTICS",
    "schema": "PUBLIC",
    "warehouse": "NOTARY_WH"
  },
  "options": {
    "capture_mode": "watermark",
    "watermark_column": "UPDATED_AT",
    "row_limit": 1000
  }
}
```

## Notes

- current `Wave 2` work already provides live ping, configured object introspection, and snapshot polling baseline
- `Snowflake` is not yet production-validated
- native Snowflake streams/tasks are not part of this first implementation slice
- local full-cycle proof export is the next implementation step
