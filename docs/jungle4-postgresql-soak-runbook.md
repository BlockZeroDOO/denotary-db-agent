# Jungle4 PostgreSQL Soak Runbook

Use this runbook to execute a long-running PostgreSQL soak test against the
`Jungle4` validation deployment before promoting operational changes to `denotary`.

This runbook is intentionally focused on the enterprise path:

- PostgreSQL
- `denotary-db-agent`
- local deNotary services
- `verifbill::submit` / `verifbill::submitroot`
- inline `verif::billsubmit` / `verif::billbatch`

## Goal

Validate long-run behavior under sustained PostgreSQL change load, including:

- logical `pgoutput` capture
- stream mode stability
- batch anchoring under ongoing writes
- reconnect / fallback / probation behavior
- local retention and diagnostics snapshot flow
- on-chain proof creation on `Jungle4`

## Current Jungle4 Assumptions

If you are reusing the currently validated Jungle4 contract layout from prior work:

- `verif` account: `decentrfstor`
- `verifbill` account: `vadim1111111`

If your Jungle4 deployment differs, replace those values in the config and
verification commands below.

Current stable short-soak signing baseline:

- `broadcast_backend = "private_key_env"`
- `submitter = "newtestactio"`
- `submitter_permission = "owner"`

That is a testnet validation shortcut, not a production recommendation.

## Files

Starter files:

- [deploy/config/jungle4-soak-agent.example.json](../deploy/config/jungle4-soak-agent.example.json)
- [scripts/postgres-soak-load.py](../scripts/postgres-soak-load.py)
- [deploy/postgres-live/docker-compose.yml](../deploy/postgres-live/docker-compose.yml)
- [deploy/postgres-live/init.sql](../deploy/postgres-live/init.sql)

## Recommended Soak Phases

### Phase 1: Short Validation

Run for `2-4` hours to confirm the setup is healthy.

### Phase 2: Long Soak

Run for `12-24` hours after the short validation looks stable.

## Preparation

### 1. Prepare PostgreSQL

Start the live PostgreSQL harness:

```bash
docker compose -f deploy/postgres-live/docker-compose.yml up -d
```

Confirm it is reachable on `127.0.0.1:55432`.

### 2. Prepare Agent Config

Copy the starter config:

```bash
cp deploy/config/jungle4-soak-agent.example.json data/jungle4-soak/agent.json
```

Then set:

- `watcher_auth_token`
- `chain_rpc_url`
- `submitter`
- `schema_id`
- `policy_id`
- PostgreSQL password
- local `env_file`

Recommended unique runtime identity:

- `source_id = pg-jungle4-soak`
- `slot_name = denotary_jungle4_soak_slot`
- `publication_name = denotary_jungle4_soak_pub`

Important policy rule:

- if `batch_enabled = false`, use a policy with `allow_single = 1`
- if `batch_enabled = true`, use a policy with `allow_batch = 1`

Do not reuse a single-only policy for batch soak or a batch-only policy for single soak.

### 3. Prepare Hot Key

Create a local secret file outside git, for example:

```dotenv
DENOTARY_SUBMITTER_PRIVATE_KEY=<HOT_WIF>
```

On POSIX:

```bash
chmod 600 data/jungle4-soak/agent.secrets.env
```

For the current validated Jungle4 baseline, using `owner` is acceptable for
testnet validation if no dedicated hot permission exists yet. For mainnet or
customer-facing production rollout, prefer a dedicated hot permission such as
`dnanchor`.

### 4. Confirm Entitlement

Before starting a long soak, confirm the Jungle4 submitter already has enough
remaining enterprise entitlement for the whole window.

## Preflight

Run:

```bash
python -m denotary_db_agent --config data/jungle4-soak/agent.json doctor --source pg-jungle4-soak --strict
python -m denotary_db_agent --config data/jungle4-soak/agent.json bootstrap --source pg-jungle4-soak
python -m denotary_db_agent --config data/jungle4-soak/agent.json inspect --source pg-jungle4-soak
python -m denotary_db_agent --config data/jungle4-soak/agent.json metrics --source pg-jungle4-soak
python -m denotary_db_agent --config data/jungle4-soak/agent.json diagnostics --source pg-jungle4-soak --save-snapshot
```

`doctor` should confirm:

- `broadcast_ready = true`
- `private_key_matches_permission = true`
- `permission_is_minimal_hot_key = true`
- no `critical` or `error`

## Controlled First Event

Before the soak loop, do a single controlled run:

1. Insert one known row manually into PostgreSQL.
2. Run:

```bash
python -m denotary_db_agent --config data/jungle4-soak/agent.json run --once
```

3. Confirm:

- `processed = 1`
- `failed = 0`
- a proof bundle was exported
- the on-chain action landed in `Jungle4`

## Start the Soak

### Terminal 1: Agent Daemon

```bash
python -m denotary_db_agent --config data/jungle4-soak/agent.json run --interval-sec 5
```

### Terminal 2: PostgreSQL Load Generator

Example short validation load:

```bash
python scripts/postgres-soak-load.py --duration-sec 14400 --interval-sec 1.0 --burst-every 30 --burst-size 10 --delete-every 5
```

Example long soak load:

```bash
python scripts/postgres-soak-load.py --duration-sec 86400 --interval-sec 1.0 --burst-every 30 --burst-size 10 --delete-every 5
```

What the generator does:

- inserts invoices
- inserts payments
- updates recent invoices
- updates recent payments
- deletes some recent payments
- periodically emits multi-row transactions

This gives coverage for:

- `insert`
- `update`
- `delete`
- burst transactions
- multi-row commit handling

## During the Soak

Periodically collect operator evidence:

```bash
python -m denotary_db_agent --config data/jungle4-soak/agent.json health --source pg-jungle4-soak
python -m denotary_db_agent --config data/jungle4-soak/agent.json metrics --source pg-jungle4-soak
python -m denotary_db_agent --config data/jungle4-soak/agent.json diagnostics --source pg-jungle4-soak --save-snapshot
python -m denotary_db_agent --config data/jungle4-soak/agent.json report --source pg-jungle4-soak --save-snapshot
```

Watch especially:

- `stream_reconnect_count`
- `stream_failure_streak`
- `stream_backoff_active`
- `stream_fallback_active`
- `stream_probation_active`
- `logical_slot_retained_wal_bytes`
- `logical_slot_flush_lag_bytes`
- `dlq_count`

## Recommended Fault Injection

Do these only during the short validation phase first.

### Agent Restart

Restart the daemon and confirm it resumes from checkpoint.

### PostgreSQL Restart

Restart the Docker PostgreSQL service and confirm:

- stream reconnect occurs
- no uncontrolled DLQ growth starts
- delivery resumes

### Publication / Slot Recovery

Use controlled maintenance to remove the slot or publication, then confirm:

```bash
python -m denotary_db_agent --config data/jungle4-soak/agent.json inspect --source pg-jungle4-soak
python -m denotary_db_agent --config data/jungle4-soak/agent.json refresh --source pg-jungle4-soak
python -m denotary_db_agent --config data/jungle4-soak/agent.json inspect --source pg-jungle4-soak
```

## On-Chain Verification

During or after the soak, verify that `Jungle4` continues to receive anchors.

Useful commands for the current validated layout:

```bash
cleos -u <JUNGLE4_RPC> get table decentrfstor decentrfstor commitments --limit 10 --reverse
cleos -u <JUNGLE4_RPC> get table decentrfstor decentrfstor batches --limit 10 --reverse
cleos -u <JUNGLE4_RPC> get table vadim1111111 vadim1111111 entitlements --limit 10
```

Confirm:

- new `commitments` or `batches` appear
- entitlement decreases as expected
- proof bundles continue exporting locally

## Exit Criteria

Short validation is successful when:

- daemon stays healthy for `2-4` hours
- `processed` continues increasing
- `failed` stays low and explainable
- no uncontrolled `dlq` growth occurs
- no unrecovered slot/publication drift remains

Long soak is successful when:

- the full `12-24` hour window completes
- anchors continue reaching `Jungle4`
- proof export remains healthy
- reconnect/fallback behavior stays bounded and recoverable
- no persistent `critical` state remains at the end

## Final Evidence Capture

At the end of the run:

```bash
python -m denotary_db_agent --config data/jungle4-soak/agent.json doctor --source pg-jungle4-soak --save-snapshot
python -m denotary_db_agent --config data/jungle4-soak/agent.json diagnostics --source pg-jungle4-soak --save-snapshot
python -m denotary_db_agent --config data/jungle4-soak/agent.json report --source pg-jungle4-soak --save-snapshot
python -m denotary_db_agent --config data/jungle4-soak/agent.json artifacts --source pg-jungle4-soak --latest 20
```

This gives one bounded local evidence trail for the full soak window.
