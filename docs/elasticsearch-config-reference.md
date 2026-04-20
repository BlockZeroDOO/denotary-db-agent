# Elasticsearch Config Reference

`Elasticsearch` is part of the `Wave 2` roadmap.

Current baseline:

- connection-shape validation
- live cluster ping through `elasticsearch`
- tracked-index introspection
- query-based snapshot polling
- deterministic checkpoint resume
- dry-run snapshot playback
- local full-cycle proof export
- env-gated live integration harness
- env-gated restart and short-soak validation harness

Native Elasticsearch CDC is not implemented yet.

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

- `url`: `string`, required unless `host` or `hosts` is provided
- `host`: `string`, optional
- `hosts`: `string[]`, optional
- `port`: `integer`, optional, default `9200`
- `scheme`: `string`, optional, default `http`
- `username`: `string`, optional
- `password`: `string`, optional
- `verify_certs`: `boolean`, optional

## Options

Supported now:

- `capture_mode`
  - supported values: `"watermark"`
- `watermark_field`
- `commit_timestamp_field`
- `primary_key_field`
- `row_limit`
- `dry_run_events`

## Notes

- the current baseline is intentionally snapshot-first
- tracked documents are read via sorted search requests using the watermark field and `_id`
- this baseline is useful for validating search-backed evidence, not for replacing a true source-of-truth CDC feed
- local proof export is already covered with dry-run snapshot playback
- live validation can be driven through `scripts/run-live-elasticsearch-integration.ps1` once Elasticsearch credentials are available in the environment
- deeper restart and short-soak validation can be driven through `scripts/run-wave2-elasticsearch-validation.ps1`
- native Elasticsearch CDC can be added later if commercially justified

Deployment guidance:

- [wave2-elasticsearch-runbook.md](wave2-elasticsearch-runbook.md)
- [../deploy/config/elasticsearch-agent.example.json](../deploy/config/elasticsearch-agent.example.json)
