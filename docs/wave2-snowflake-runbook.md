# Wave 2 Snowflake Runbook

This runbook describes the recommended baseline for deploying `denotary-db-agent` against `Snowflake`.

The target model is:

- `Snowflake` remains the analytics warehouse
- `denotary-db-agent` polls explicitly tracked tables or views through the `Snowflake` adapter
- notarization proofs are exported locally and anchored through the normal `deNotary` service stack
- checkpoint state survives process restarts in the local `state_db`

## Recommended Fit

Use the `Snowflake` adapter when:

- the business workflow depends on warehouse-visible records
- explicit object selection is acceptable
- bounded snapshot polling is acceptable
- the goal is to prove analytical or reporting datasets exposed through Snowflake

Good examples:

- finance or compliance reporting tables
- curated analytical views
- customer-visible reconciliation datasets
- warehouse-backed evidence for partners or auditors

## Reference Config

Starter config:

- [../deploy/config/snowflake-agent.example.json](../deploy/config/snowflake-agent.example.json)

Reference docs:

- [snowflake-config-reference.md](snowflake-config-reference.md)
- [storage-config-reference.md](storage-config-reference.md)
- [denotary-service-config-reference.md](denotary-service-config-reference.md)
- [denotary-env-file-runbook.md](denotary-env-file-runbook.md)

## Preconditions

Before starting the agent, confirm:

1. the Snowflake account, warehouse, database, and schema are reachable
2. the tracked tables or views are explicitly defined
3. the chosen watermark field is present and stable
4. the service user can write the configured `storage.state_db` and `proof_dir`
5. the `deNotary` service stack and signer are already reachable

## Recommended Source Settings

For the current baseline, use:

- `adapter = "snowflake"`
- `capture_mode = "watermark"`
- `tracked_object_type = "table"` or `"view"`
- explicit `include` object names
- bounded `row_limit`

## First-Time Startup

Recommended order:

1. `doctor`
2. `bootstrap`
3. `inspect`
4. one controlled `run --once`
5. verify proof export
6. start daemon mode

## Validation Status

The current `Snowflake` implementation already confirms:

- live warehouse ping
- configured-object introspection
- local full-cycle proof export
- env-gated live integration and full-cycle harnesses

Current gap:

- deeper restart and soak validation still depend on a real Snowflake environment
