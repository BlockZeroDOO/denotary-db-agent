# deNotary PostgreSQL Rollout Checklist

Use this checklist when promoting a PostgreSQL source to `denotary` mainnet.

## Before Host Setup

- Confirm the enterprise submitter account exists.
- Confirm a dedicated hot permission exists:
  - recommended: `dnanchor`
- Confirm `dnanchor` is linked only to:
  - `verifbill::submit`
  - `verifbill::submitroot`
- Confirm the submitter account has active `verifbill` entitlement with enough remaining KiB.
- Confirm the target `schema_id` and `policy_id` are correct for mainnet.

## Host Preparation

- Install Python runtime and dependencies.
- Place `denotary-db-agent` on the target host.
- Create config and runtime directories.
- Create the hot-key secret file outside git.
- Restrict secret-file permissions:
  - Linux / POSIX: `chmod 600`
- Ensure the local service user can read:
  - config JSON
  - secret env file
  - state/proof directories

## Service Dependencies

- Confirm local `Ingress API` is reachable.
- Confirm local `Finality Watcher` is reachable.
- Confirm local `Receipt Service` is reachable.
- Confirm local `Audit API` is reachable.
- Confirm `chain_rpc_url` points to the intended `denotary` RPC.
- Confirm PostgreSQL is reachable from the agent host.

## Source Configuration

- Use a unique `source_id`.
- Use a unique logical `slot_name`.
- Use a unique `publication_name`.
- Do not reuse old test slot/publication names for rollout.
- Confirm tracked schemas/tables are correct.
- Confirm `capture_mode`.
- Confirm `output_plugin`:
  - recommended baseline: `pgoutput`
- Confirm `logical_runtime_mode`:
  - recommended baseline: `stream`

## Signer Configuration

- Set:
  - `broadcast_backend = "private_key_env"`
- Set:
  - `submitter_private_key_env = "DENOTARY_SUBMITTER_PRIVATE_KEY"`
- Set:
  - `env_file = "/path/to/agent.secrets.env"`
- Keep `submitter_private_key` empty in production config.

## Preflight Commands

Run:

```bash
python -m denotary_db_agent --config /etc/denotary-db-agent/agent.json doctor --source <source_id> --strict
python -m denotary_db_agent --config /etc/denotary-db-agent/agent.json bootstrap --source <source_id>
python -m denotary_db_agent --config /etc/denotary-db-agent/agent.json inspect --source <source_id>
python -m denotary_db_agent --config /etc/denotary-db-agent/agent.json metrics --source <source_id>
python -m denotary_db_agent --config /etc/denotary-db-agent/agent.json diagnostics --source <source_id>
```

## `doctor` Must Confirm

- `broadcast_ready = true`
- `private_key_matches_permission = true`
- `permission_exists = true`
- `billing_account_exists = true`
- source connectivity is healthy
- no `critical` or `error` severity

On POSIX hosts also expect:

- `env_file_permissions_checked = true`
- `env_file_permissions_ok = true`

## Controlled First Run

1. Insert one known test row into a tracked PostgreSQL table.
2. Run:

```bash
python -m denotary_db_agent --config /etc/denotary-db-agent/agent.json run --once
```

3. Confirm:
  - `processed = 1`
  - `failed = 0`
4. Confirm proof bundle export under `storage.proof_dir`.

## On-Chain Verification

Verify:

- `verifbill::submit` or `verifbill::submitroot` used the expected submitter permission
- inline `verif::billsubmit` or `verif::billbatch` exists
- `verif.commitments` or `verif.batches` contains the new anchor
- entitlement balance decreased

Useful commands:

```bash
cleos -u https://history.denotary.io get transaction <tx_id>
cleos -u https://history.denotary.io get table verif verif commitments --limit 5 --reverse
cleos -u https://history.denotary.io get table verifbill verifbill entitlements --limit 10
```

## Rollout Evidence

Save rollout evidence:

```bash
python -m denotary_db_agent --config /etc/denotary-db-agent/agent.json doctor --source <source_id> --save-snapshot
python -m denotary_db_agent --config /etc/denotary-db-agent/agent.json report --source <source_id> --save-snapshot
```

Optionally inspect later:

```bash
python -m denotary_db_agent --config /etc/denotary-db-agent/agent.json artifacts --source <source_id>
python -m denotary_db_agent --config /etc/denotary-db-agent/agent.json artifacts --source <source_id> --latest 5
```

## Daemon Enablement

Only after the controlled run is verified:

- enable `systemd` / Windows service / Docker daemon mode
- keep `doctor --strict` in pre-start
- monitor:
  - `health`
  - `metrics`
  - `diagnostics`

## After Enablement

- confirm checkpoint advances
- confirm proof bundles keep exporting
- confirm no unexpected `dlq` growth
- confirm logical slot lag stays within thresholds
- confirm reconnect/fallback state stays healthy over time
