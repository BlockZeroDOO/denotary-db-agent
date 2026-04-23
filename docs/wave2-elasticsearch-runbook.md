# Wave 2 Elasticsearch Runbook

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

This runbook describes the current release-oriented baseline for deploying
`denotary-db-agent` against `Elasticsearch`.

The supported source model today is:

- `Elasticsearch` remains the operational search and index layer
- `denotary-db-agent` polls explicit index patterns through the `elasticsearch`
  adapter
- notarization proofs are exported locally and anchored through the normal
  `deNotary` service stack
- checkpoint state survives process restarts in the local `state_db`

Native Elasticsearch CDC is not part of the current baseline.

## Security Baseline

For enterprise production use:

- keep `Ingress`, `Watcher`, `Receipt`, and `Audit` in the same trusted
  deployment boundary as the agent
- use a dedicated hot permission such as `dnanchor`
- do not use `owner` or `active` as the runtime signer permission
- keep the hot key in `env_file` or a secret mount
- treat `state_db`, `proof_dir`, and saved evidence snapshots as sensitive
  local artifacts

Reference:

- [security-baseline.md](security-baseline.md)
- [denotary-env-file-runbook.md](denotary-env-file-runbook.md)

## Recommended Fit

Use the `Elasticsearch` adapter when:

- the business workflow depends on the indexed representation exposed through
  search
- explicit index-pattern selection is acceptable
- bounded polling is acceptable
- the goal is to prove the search-visible record set rather than reconstruct a
  full source-of-truth transaction log

Good examples:

- operational search indices exposed to customers or counterparties
- searchable order or case views
- document metadata indices
- mirrored audit or reporting indices

## Reference Config

Starter config:

- [deploy/config/elasticsearch-agent.example.json](../deploy/config/elasticsearch-agent.example.json)

Reference docs:

- [elasticsearch-config-reference.md](elasticsearch-config-reference.md)
- [security-baseline.md](security-baseline.md)
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

- Elasticsearch endpoint: `https://elasticsearch.example.com:9200`
- agent state: `/var/lib/denotary-db-agent/elasticsearch-agent-state.sqlite3`
- proof export directory: `/var/lib/denotary-db-agent/proofs`
- env-file secret: `/etc/denotary-db-agent/agent.secrets.env`

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
- `cdc.runtime.effective_runtime_mode = "watermark"`

## Validation Status

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

Reference:

- [wave2-elasticsearch-validation.md](wave2-elasticsearch-validation.md)
- [wave2-mainnet-budget-validation-report.md](wave2-mainnet-budget-validation-report.md)
- [wave2-mainnet-service-outage-validation-report.md](wave2-mainnet-service-outage-validation-report.md)
- [wave2-readiness-matrix.md](wave2-readiness-matrix.md)

## Operational Notes

- keep index patterns explicit and narrow
- treat the current adapter as proof of the indexed operational view, not
  necessarily the canonical source-of-truth record
- keep watermark and primary key fields stable across the tracked indices
- bound proof retention for high-volume search workloads
- prefer polling intervals that reflect actual document churn

## Current Limits

The current baseline does not provide:

- native change-stream style CDC
- delete tombstone reconstruction after a document disappears between polls
- full index-agnostic discovery
- a production-scale replacement for mutation-stream capture

For the current release posture, prefer:

- explicit index-pattern coverage
- bounded polling
- regular `doctor`, `inspect`, and `report` checks
- release or rollout evidence based on restart, soak, and degraded-service runs
