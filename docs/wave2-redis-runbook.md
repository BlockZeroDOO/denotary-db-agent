# Wave 2 Redis Runbook

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

This runbook describes the recommended baseline for deploying `denotary-db-agent` against `Redis` in operational state and cache-adjacent scenarios.

The target model is:

- `Redis` remains the operational state store
- `denotary-db-agent` polls explicit key patterns through `SCAN`
- notarization proofs are exported locally and anchored through the normal `deNotary` service stack
- checkpoint state survives process restarts in the local `state_db`

## Recommended Fit

Use the `Redis` adapter when:

- the business workflow depends on specific Redis keys or namespaces
- explicit key-pattern selection is acceptable
- bounded polling is acceptable
- the goal is to prove the state exposed through Redis rather than reconstruct an internal event log

Good examples:

- order cache layers
- pricing or policy snapshots
- branch-local session or entitlement state
- operational keys mirrored from another system of record

## Reference Config

Starter config:

- [deploy/config/redis-agent.example.json](../deploy/config/redis-agent.example.json)

Reference docs:

- [redis-config-reference.md](redis-config-reference.md)
- [storage-config-reference.md](storage-config-reference.md)
- [denotary-service-config-reference.md](denotary-service-config-reference.md)
- [denotary-env-file-runbook.md](denotary-env-file-runbook.md)

## Preconditions

Before starting the agent, confirm:

1. the Redis instance is reachable
2. the tracked key patterns are explicitly defined
3. the selected patterns are narrow enough to poll predictably
4. the service user can write the configured `storage.state_db` and `proof_dir`
5. the `deNotary` service stack and signer are already reachable

## Recommended Layout

Example Linux layout:

- application Redis:
  - `127.0.0.1:6379`
- agent state:
  - `/var/lib/denotary-db-agent/redis-agent-state.sqlite3`
- proof export directory:
  - `/var/lib/denotary-db-agent/proofs`
- env-file secret:
  - `/etc/denotary-db-agent/agent.secrets.env`

## Recommended Source Settings

For the current baseline, use:

- `adapter = "redis"`
- `capture_mode = "scan"`
- explicit `include` key patterns
- bounded `row_limit`
- bounded `scan_count`

Typical source section:

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
  "checkpoint_policy": "after_ack",
  "backfill_mode": "full",
  "connection": {
    "host": "127.0.0.1",
    "port": 6379,
    "db": 0
  },
  "options": {
    "capture_mode": "scan",
    "row_limit": 250,
    "scan_count": 250
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
python -m denotary_db_agent --config /etc/denotary-db-agent/redis-agent.json doctor --source redis-cache-core --strict
python -m denotary_db_agent --config /etc/denotary-db-agent/redis-agent.json bootstrap --source redis-cache-core
python -m denotary_db_agent --config /etc/denotary-db-agent/redis-agent.json inspect --source redis-cache-core
python -m denotary_db_agent --config /etc/denotary-db-agent/redis-agent.json run --once
python -m denotary_db_agent --config /etc/denotary-db-agent/redis-agent.json run --interval-sec 15
```

## What `inspect` Should Show

Healthy baseline should include:

- `adapter = "redis"`
- `capture_mode = "scan"`
- explicit tracked key patterns
- per-database tracked keys under `tracked_keys`
- `cdc.runtime.transport = "polling"`

## Validation Status

The current `Redis` validation already confirms:

- Docker-backed live baseline validation
- local full-cycle proof export
- restart recovery validation
- short-soak validation

Reference:

- [wave2-redis-validation.md](wave2-redis-validation.md)
- [wave2-redis-validation-report.md](wave2-redis-validation-report.md)

## Operational Notes

- keep key patterns explicit and narrow
- avoid using the current baseline as an implicit “all keys” scraper
- size `row_limit` and `scan_count` to fit the expected key volume
- bound proof retention for cache-heavy workloads
- prefer polling intervals that reflect how fast the tracked keys actually change

## Current Limits

The current baseline does not yet provide:

- keyspace notification CDC
- Redis Streams consumption
- wildcard full-database discovery
- delete tombstone reconstruction after a key disappears between polls

For now, the strongest production posture is:

- explicit key-pattern coverage
- bounded polling
- regular `doctor`, `inspect`, and `report` checks
- restart and short-soak validation before promoting a pattern set to production
