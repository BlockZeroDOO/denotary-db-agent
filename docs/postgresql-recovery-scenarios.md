# PostgreSQL Recovery Scenarios

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

This runbook describes the current PostgreSQL recovery policy for `denotary-db-agent`.

It is focused on logical PostgreSQL sources that use:

- `capture_mode = "logical"`
- `output_plugin = "pgoutput"`
- `logical_runtime_mode = "stream"` or `logical_runtime_mode = "peek"`

## Scope

The agent already handles these PostgreSQL recovery cases:

- missing logical slot
- missing `pgoutput` publication
- publication drift
- REPLICA IDENTITY drift
- stream reconnect after connection loss
- temporary fallback from `stream` to `peek`
- return from `peek` back to `stream` after fallback

The goal of the operator is not to repair PostgreSQL objects by hand first, but to:

1. inspect the source
2. run the built-in repair command
3. confirm the repaired runtime state
4. only then resume or keep daemon mode running

## Fast Triage

Run:

```bash
python -m denotary_db_agent --config /etc/denotary-db-agent/agent.json inspect --source <source_id>
python -m denotary_db_agent --config /etc/denotary-db-agent/agent.json health --source <source_id>
python -m denotary_db_agent --config /etc/denotary-db-agent/agent.json diagnostics --source <source_id>
```

Focus on these PostgreSQL fields:

- `slot_exists`
- `publication_exists`
- `publication_in_sync`
- `replica_identity_in_sync`
- `effective_runtime_mode`
- `stream_session_active`
- `stream_reconnect_count`
- `stream_failure_streak`
- `stream_backoff_active`
- `stream_fallback_active`
- `stream_probation_active`
- `stream_last_error_kind`
- `stream_last_error`

## Scenario: Missing Logical Slot

Typical symptoms:

- `inspect` shows `slot_exists = false`
- `health` becomes `critical` or `error`
- logical reads stop advancing

Recovery:

```bash
python -m denotary_db_agent --config /etc/denotary-db-agent/agent.json refresh --source <source_id>
python -m denotary_db_agent --config /etc/denotary-db-agent/agent.json inspect --source <source_id>
```

Expected repaired state:

- `slot_exists = true`
- `plugin = "pgoutput"` for the logical source
- source severity no longer reports missing slot

Notes:

- this repair path is already covered by live PostgreSQL integration tests
- it depends on auto-create being enabled for the source

## Scenario: Missing Publication

Typical symptoms:

- `inspect` shows `publication_exists = false`
- `publication_in_sync = false`
- `pgoutput` runtime cannot safely continue

Recovery:

```bash
python -m denotary_db_agent --config /etc/denotary-db-agent/agent.json refresh --source <source_id>
python -m denotary_db_agent --config /etc/denotary-db-agent/agent.json inspect --source <source_id>
```

Expected repaired state:

- `publication_exists = true`
- `publication_in_sync = true`
- `publication_tables` matches the tracked PostgreSQL tables

Notes:

- this repair path is also covered by live PostgreSQL integration tests
- use unique publication names per source to avoid cross-source collisions

## Scenario: Publication Drift

Typical symptoms:

- `publication_exists = true`
- `publication_in_sync = false`
- `publication_tables` differs from the tracked table list

Recovery:

```bash
python -m denotary_db_agent --config /etc/denotary-db-agent/agent.json refresh --source <source_id>
```

Expected repaired state:

- `publication_in_sync = true`
- `publication_tables` includes all tracked tables and no unexpected extra tables

## Scenario: REPLICA IDENTITY Drift

Typical symptoms:

- `replica_identity_in_sync = false`
- logical `update` or `delete` capture becomes unsafe or incomplete

Recovery:

```bash
python -m denotary_db_agent --config /etc/denotary-db-agent/agent.json refresh --source <source_id>
python -m denotary_db_agent --config /etc/denotary-db-agent/agent.json inspect --source <source_id>
```

Expected repaired state:

- `replica_identity_in_sync = true`

## Scenario: Stream Connection Loss

Typical symptoms:

- `stream_session_active = false`
- `stream_reconnect_count` increases
- `stream_last_error_kind` becomes something like:
  - `connection_lost`
  - `timeout`
  - `protocol_error`

Built-in behavior:

- the agent closes the broken replication session
- reopens it from the last safe logical position
- keeps retrying with bounded reconnect backoff

Operator action:

- first observe whether the stream recovers on its own
- if reconnect count grows but the source becomes healthy again, no manual repair is required
- if the source stays degraded or critical, run:

```bash
python -m denotary_db_agent --config /etc/denotary-db-agent/agent.json diagnostics --source <source_id>
python -m denotary_db_agent --config /etc/denotary-db-agent/agent.json report --source <source_id> --save-snapshot
```

## Scenario: Stream Fallback to Peek

Typical symptoms:

- `effective_runtime_mode = "peek"`
- `stream_fallback_active = true`
- `health.severity = degraded`

Meaning:

- the source was configured for `stream`
- repeated stream failures triggered a temporary fallback to `peek`
- delivery can continue, but the source is no longer in the preferred runtime mode

Operator action:

- monitor whether fallback expires and the source returns to `stream`
- if fallback persists or repeats often:
  - inspect PostgreSQL connectivity
  - inspect recent stream errors
  - collect a `report --save-snapshot`

## Scenario: Probation After Return to Stream

Typical symptoms:

- `effective_runtime_mode = "stream"`
- `stream_probation_active = true`
- `health.severity = degraded`

Meaning:

- fallback already expired
- the source returned to `stream`
- the agent is intentionally surfacing a short recovery observation window

Operator action:

- usually no manual repair is needed
- just confirm that:
  - `stream_failure_streak` returns to `0`
  - `stream_session_active = true`
  - no new critical warnings appear

## Recommended Recovery Sequence

When PostgreSQL-side runtime objects are lost or drifted:

1. `pause` the source if daemon mode is actively failing and you want a quiet maintenance window
2. run `inspect`
3. run `health`
4. run `refresh`
5. run `inspect` again
6. run `doctor --strict` if signer or service topology also changed
7. run one controlled `run --once`
8. confirm:
   - checkpoint advances
   - proofs export
   - no unexpected DLQ growth
9. `resume` the source or leave daemon mode running

## Evidence Capture

Before and after a meaningful recovery action, save evidence:

```bash
python -m denotary_db_agent --config /etc/denotary-db-agent/agent.json diagnostics --source <source_id> --save-snapshot
python -m denotary_db_agent --config /etc/denotary-db-agent/agent.json doctor --source <source_id> --save-snapshot
python -m denotary_db_agent --config /etc/denotary-db-agent/agent.json report --source <source_id> --save-snapshot
python -m denotary_db_agent --config /etc/denotary-db-agent/agent.json artifacts --source <source_id> --latest 10
```

This keeps the recovery trail in the local evidence manifest without manual file tracking.
