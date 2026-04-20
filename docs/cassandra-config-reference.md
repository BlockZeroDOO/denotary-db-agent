# Apache Cassandra Config Reference

`Apache Cassandra` is part of the `Wave 2` roadmap.

Current baseline:

- connection-shape validation
- live cluster ping through `cassandra-driver`
- tracked-table introspection
- watermark snapshot polling
- deterministic checkpoint resume
- dry-run snapshot playback
- local full-cycle proof export
- env-gated live integration harness

Native Cassandra CDC is not implemented yet.

## Source Example

```json
{
  "id": "cassandra-core-ledger",
  "adapter": "cassandra",
  "enabled": true,
  "source_instance": "events-eu-1",
  "database_name": "ledger",
  "include": {
    "ledger": ["invoices", "payments"]
  },
  "connection": {
    "host": "127.0.0.1",
    "port": 9042,
    "username": "cassandra",
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
- local proof export is already covered with dry-run snapshot playback
- live validation can be driven through `scripts/run-live-cassandra-integration.ps1` once Cassandra credentials are available in the environment
- native Cassandra CDC can be added later if commercially justified

Deployment guidance:

- [wave2-cassandra-runbook.md](wave2-cassandra-runbook.md)
- [../deploy/config/cassandra-agent.example.json](../deploy/config/cassandra-agent.example.json)
