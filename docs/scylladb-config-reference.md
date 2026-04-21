# ScyllaDB Config Reference

`ScyllaDB` is part of the active `Wave 2` roadmap.

Current baseline:

- connection-shape validation
- Cassandra-compatible live cluster ping through `cassandra-driver`
- tracked-table introspection
- watermark snapshot polling
- deterministic checkpoint resume
- dry-run snapshot playback
- local full-cycle proof export
- Docker-backed live integration harness

Current implementation note:

- the `scylladb` adapter reuses the shared Cassandra-compatible baseline
- native Scylla-specific CDC is not implemented yet

## Source Example

```json
{
  "id": "scylladb-core-ledger",
  "adapter": "scylladb",
  "enabled": true,
  "source_instance": "cluster-eu-1",
  "database_name": "ledger",
  "include": {
    "ledger": ["invoices", "payments"]
  },
  "connection": {
    "host": "127.0.0.1",
    "port": 9042,
    "username": "scylla",
    "password": "secret"
  },
  "options": {
    "capture_mode": "watermark",
    "watermark_column": "updated_at",
    "commit_timestamp_column": "updated_at",
    "row_limit": 1000
  }
}
```

## Connection Fields

- `host`: `string`, required unless `hosts` is provided
- `hosts`: `string[]`, optional multi-node contact points
- `port`: `integer`, optional, default `9042`
- `username`: `string`, optional
- `password`: `string`, optional

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

- the current baseline is intentionally snapshot-first
- live reads use tracked tables and a watermark field with deterministic client-side resume
- the baseline query path may rely on `ALLOW FILTERING`, so it is aimed at validation and early production rollout rather than high-throughput CDC
- native Scylla-specific CDC can be added later if commercially justified
- local validation can be driven through:
  - `scripts/run-live-scylladb-integration.ps1`
  - `scripts/run-wave2-scylladb-validation.ps1`

Deployment guidance:

- [wave2-scylladb-runbook.md](wave2-scylladb-runbook.md)
- [wave2-scylladb-validation.md](wave2-scylladb-validation.md)
- [../deploy/config/scylladb-agent.example.json](../deploy/config/scylladb-agent.example.json)
