# IBM Db2 Config Reference

`IBM Db2` is part of the `Wave 2` roadmap.

Current baseline:

- connection-shape validation
- live ping through `ibm_db`
- tracked-table introspection
- watermark snapshot polling
- deterministic checkpoint resume
- dry-run snapshot playback
- local full-cycle proof export
- env-gated live integration harness

Native Db2 CDC is not implemented yet.

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

- `host`: `string`, required
- `port`: `integer`, required
- `username`: `string`, required
- `password`: `string`, required
- `database`: `string`, required

## Options

Supported now:

- `capture_mode`
  - supported values: `"watermark"`
- `watermark_column`
- `commit_timestamp_column`
- `primary_key_column`
- `primary_key_columns`
- `row_limit`
- `dry_run_events`

## Notes

- schemas and table names are normalized to uppercase for the baseline implementation
- current work focuses on a snapshot / watermark baseline first
- local proof export is already covered with dry-run snapshot playback
- live validation can be driven through `scripts/run-live-db2-integration.ps1` once Db2 credentials are available in the environment
- native Db2 change capture can be added later if commercially justified
