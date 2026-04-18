# deNotary Mainnet Env-File Runbook

This runbook describes the recommended production baseline for running `denotary-db-agent` against `denotary` mainnet with an env-file-backed hot key.

The target model is:

- enterprise account signs through `submitter@dnanchor`
- the hot key is loaded from a local `env_file`
- `owner` stays offline
- `active` stays off the application host
- no dependency on `cleos wallet` in daemon mode

## Preconditions

Before starting the agent, confirm:

1. `submitter_permission` exists on chain and is linked only to:
   - `verifbill::submit`
   - `verifbill::submitroot`
2. the enterprise account has an active `verifbill` entitlement with remaining KiB
3. local deNotary services are reachable:
   - `Ingress`
   - `Finality Watcher`
   - `Receipt`
   - `Audit`
4. PostgreSQL source connectivity is already working
5. the hot key is **not** committed to git

Reference:

- [verifbill-permission-model.md](verifbill-permission-model.md)
- [verifbill-permission-commands.md](verifbill-permission-commands.md)

## Recommended Config

Use:

- `broadcast_backend = "private_key_env"`
- `submitter_permission = "dnanchor"`
- `submitter_private_key_env = "DENOTARY_SUBMITTER_PRIVATE_KEY"`
- `env_file = "/path/to/agent.secrets.env"`

Minimal `denotary` section:

```json
"denotary": {
  "ingress_url": "http://127.0.0.1:8080",
  "watcher_url": "http://127.0.0.1:8081",
  "watcher_auth_token": "REPLACE_WITH_WATCHER_TOKEN",
  "receipt_url": "http://127.0.0.1:8082",
  "audit_url": "http://127.0.0.1:8083",
  "chain_rpc_url": "https://history.denotary.io",
  "submitter": "dbagentstest",
  "submitter_permission": "dnanchor",
  "broadcast_backend": "private_key_env",
  "submitter_private_key_env": "DENOTARY_SUBMITTER_PRIVATE_KEY",
  "env_file": "/etc/denotary-db-agent/agent.secrets.env",
  "submitter_private_key": "",
  "schema_id": 1,
  "policy_id": 1,
  "billing_account": "verifbill",
  "wait_for_finality": true,
  "finality_timeout_sec": 180,
  "finality_poll_interval_sec": 3.0
}
```

## Secret File

Example secret file:

```dotenv
DENOTARY_SUBMITTER_PRIVATE_KEY=REPLACE_WITH_DNANCHOR_WIF
```

Recommended placement:

- Linux: `/etc/denotary-db-agent/agent.secrets.env`
- Windows: `C:\deNotary\denotary-db-agent\config\agent.secrets.env`
- Docker secret mount: `/run/secrets/agent.secrets.env`

Recommended file permissions:

- Linux / POSIX: `chmod 600 /path/to/agent.secrets.env`
- readable only by the service user
- not world-readable
- not group-writable
- not included in backups unless that is explicitly intended

## Fresh Source Naming

For a clean rollout, do **not** reuse test slot/publication names.

Use source-specific unique names, for example:

- `slot_name = denotary_prod_pg_ledger_slot`
- `publication_name = denotary_prod_pg_ledger_pub`

When testing repeatedly on the same host, unique names avoid confusion from stale logical state.

## First-Time Startup

Recommended order:

1. `doctor`
2. `bootstrap`
3. `inspect`
4. one controlled `run --once`
5. verify on-chain result
6. start daemon mode

Example:

```bash
python -m denotary_db_agent --config /etc/denotary-db-agent/agent.json doctor --source pg-core-ledger --strict
python -m denotary_db_agent --config /etc/denotary-db-agent/agent.json bootstrap --source pg-core-ledger
python -m denotary_db_agent --config /etc/denotary-db-agent/agent.json inspect --source pg-core-ledger
python -m denotary_db_agent --config /etc/denotary-db-agent/agent.json run --once
python -m denotary_db_agent --config /etc/denotary-db-agent/agent.json run --interval-sec 5
```

## What `doctor` Must Show

Healthy preflight should include:

- `broadcast_backend = "private_key_env"`
- `effective_broadcast_backend = "private_key_env"`
- `private_key_source = "env"`
- `env_file_exists = true`
- `env_file_permissions_checked = true` on Linux / POSIX
- `env_file_permissions_ok = true`
- `private_key_matches_permission = true`
- `broadcast_ready = true`
- `permission_exists = true`
- `billing_account_exists = true`

On Linux / POSIX, `doctor` should stay `healthy` when the secret file is `0600` or stricter.
If the file is group/world-readable it should become `degraded`.
If the file is group/world-writable it should become `critical`.

## On-Chain Verification

After a controlled `run --once`, verify:

1. `verifbill::submit` was signed by:
   - `submitter@dnanchor`
2. inline `verif::billsubmit` or `verif::billbatch` exists
3. `verif.commitments` or `verif.batches` contains the new anchor
4. entitlement KiB decreased as expected

Useful checks:

```bash
cleos -u https://history.denotary.io get transaction <tx_id>
cleos -u https://history.denotary.io get table verif verif commitments --limit 5 --reverse
cleos -u https://history.denotary.io get table verifbill verifbill entitlements --limit 10
```

## Rotation Procedure

Safe hot-key rotation:

1. create a new key pair
2. update `submitter@dnanchor` on chain to the new public key
3. update the local `env_file`
4. run:
   - `doctor --strict`
5. perform one controlled `run --once`
6. only then restart daemon mode if needed

If you rotate while the daemon is running, restart the process after updating the env file so the new key is reloaded.

For the full PostgreSQL rotation sequence, see:

- [postgresql-hot-key-rotation.md](postgresql-hot-key-rotation.md)

## If Broadcast Fails

Check in this order:

1. `doctor`
   - signer section
2. `metrics`
   - source severity and counters
3. `diagnostics`
   - slot/runtime state
4. local proof/delivery state
5. on-chain entitlement balance

Typical causes:

- wrong `env_file` path
- wrong variable name in `submitter_private_key_env`
- old key still present in file after permission rotation
- missing `dnanchor` permission on chain
- no active enterprise entitlement

## Operational Notes

- keep one dedicated hot key per enterprise submitter, not a shared admin key
- do not put `owner` or `active` into the agent config
- do not use `cleos wallet` for daemon mode
- keep `doctor --strict` in service pre-start hooks
- archive `doctor` and `report` snapshots for rollout evidence when promoting to new environments
