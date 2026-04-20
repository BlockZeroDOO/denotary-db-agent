# Wave 2 Elasticsearch Runbook

This runbook describes the recommended baseline for deploying `denotary-db-agent` against `Elasticsearch`.

The target model is:

- `Elasticsearch` remains the operational search and index layer
- `denotary-db-agent` polls explicit index patterns through the `Elasticsearch` adapter
- notarization proofs are exported locally and anchored through the normal `deNotary` service stack
- checkpoint state survives process restarts in the local `state_db`

## Recommended Fit

Use the `Elasticsearch` adapter when:

- the business workflow depends on the indexed representation exposed through search
- explicit index-pattern selection is acceptable
- bounded polling is acceptable
- the goal is to prove the search-visible record set rather than reconstruct a full source-of-truth transaction log

Good examples:

- operational search indices exposed to customers or counterparties
- searchable order or case views
- document metadata indices
- mirrored audit or reporting indices

## Reference Config

Starter config:

- [../deploy/config/elasticsearch-agent.example.json](../deploy/config/elasticsearch-agent.example.json)

Reference docs:

- [elasticsearch-config-reference.md](elasticsearch-config-reference.md)
- [storage-config-reference.md](storage-config-reference.md)
- [denotary-service-config-reference.md](denotary-service-config-reference.md)
- [denotary-env-file-runbook.md](denotary-env-file-runbook.md)

## Preconditions

Before starting the agent, confirm:

1. the Elasticsearch endpoint is reachable
2. the tracked index patterns are explicitly defined
3. the selected patterns are narrow enough to poll predictably
4. the chosen watermark field is present and monotonic enough for polling
5. the service user can write the configured `storage.state_db` and `proof_dir`
6. the `deNotary` service stack and signer are already reachable

## Recommended Layout

Example Linux layout:

- Elasticsearch endpoint:
  - `https://elasticsearch.example.com:9200`
- agent state:
  - `/var/lib/denotary-db-agent/elasticsearch-agent-state.sqlite3`
- proof export directory:
  - `/var/lib/denotary-db-agent/proofs`
- env-file secret:
  - `/etc/denotary-db-agent/agent.secrets.env`

## Recommended Source Settings

For the current baseline, use:

- `adapter = "elasticsearch"`
- `capture_mode = "watermark"`
- explicit `include` index patterns
- bounded `row_limit`
- one stable watermark field and one stable primary key field

Typical source section:

```json
{
  "id": "elasticsearch-orders",
  "adapter": "elasticsearch",
  "enabled": true,
  "source_instance": "search-eu-1",
  "database_name": "search",
  "include": {
    "default": ["orders-*", "payments-*"]
  },
  "checkpoint_policy": "after_ack",
  "backfill_mode": "full",
  "connection": {
    "url": "https://elasticsearch.example.com:9200",
    "username": "denotary",
    "password": "replace-me",
    "verify_certs": true
  },
  "options": {
    "capture_mode": "watermark",
    "watermark_field": "updated_at",
    "commit_timestamp_field": "updated_at",
    "primary_key_field": "_id",
    "row_limit": 250
  }
}
```

## First-Time Startup

Recommended order:

1. `doctor`
2. `bootstrap`
3. `inspect`
4. one controlled `run --once`
5. verify proof export
6. start daemon mode

Example:

```bash
python -m denotary_db_agent --config /etc/denotary-db-agent/elasticsearch-agent.json doctor --source elasticsearch-orders --strict
python -m denotary_db_agent --config /etc/denotary-db-agent/elasticsearch-agent.json bootstrap --source elasticsearch-orders
python -m denotary_db_agent --config /etc/denotary-db-agent/elasticsearch-agent.json inspect --source elasticsearch-orders
python -m denotary_db_agent --config /etc/denotary-db-agent/elasticsearch-agent.json run --once
python -m denotary_db_agent --config /etc/denotary-db-agent/elasticsearch-agent.json run --interval-sec 15
```

## What `inspect` Should Show

Healthy baseline should include:

- `adapter = "elasticsearch"`
- `capture_mode = "watermark"`
- explicit tracked index patterns
- tracked objects under `tracked_tables`
- `cdc.runtime.transport = "polling"`

## Validation Status

The current `Elasticsearch` implementation already confirms:

- env-gated live baseline validation
- local full-cycle proof export
- env-gated restart and short-soak validation harness

Reference:

- [wave2-elasticsearch-validation.md](wave2-elasticsearch-validation.md)
- [wave2-readiness-matrix.md](wave2-readiness-matrix.md)

## Operational Notes

- keep index patterns explicit and narrow
- treat the current adapter as proof of the indexed operational view, not necessarily the canonical source-of-truth record
- keep watermark and primary key fields stable across the tracked indices
- bound proof retention for high-volume search workloads
- prefer polling intervals that reflect actual document churn

## Current Limits

The current baseline does not yet provide:

- native change-stream style CDC
- delete tombstone reconstruction after a document disappears between polls
- full index-agnostic discovery
- mainnet `denotary` validation

For now, the strongest production posture is:

- explicit index-pattern coverage
- bounded polling
- regular `doctor`, `inspect`, and `report` checks
- env-gated restart and short-soak validation before promoting a pattern set to production
