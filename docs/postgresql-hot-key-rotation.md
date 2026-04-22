# PostgreSQL Hot Key Rotation

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

This runbook describes how to rotate the PostgreSQL enterprise hot key used by
`denotary-db-agent` for:

- `verifbill::submit`
- `verifbill::submitroot`

It assumes the recommended model:

- enterprise account, for example `dbagentstest`
- dedicated runtime permission, for example `dnanchor`
- `broadcast_backend = "private_key_env"`
- hot key stored in a local `env_file`

## Goal

Rotate the runtime key without exposing `owner`, and without moving the agent to
an overly broad permission.

## Preconditions

Before rotation, confirm:

- the account still has a dedicated permission such as `dnanchor`
- `dnanchor` is linked only to:
  - `verifbill::submit`
  - `verifbill::submitroot`
- the current agent config uses:
  - `submitter = <enterprise account>`
  - `submitter_permission = dnanchor`
- the current hot key is loaded from an external `env_file`

## Recommended Safe Sequence

### 1. Generate a New Key Pair

Example:

```bash
cleos create key --to-console
```

Keep the new private key out of git and out of shell history if possible.

### 2. Update the On-Chain Hot Permission

Replace the key on the existing dedicated permission.

Example:

```bash
cleos -u https://history.denotary.io push action eosio updateauth '[
  "dbagentstest",
  "dnanchor",
  "active",
  {
    "threshold": 1,
    "keys": [
      {
        "key": "PUB_OR_EOS_NEW_PUBLIC_KEY",
        "weight": 1
      }
    ],
    "accounts": [],
    "waits": []
  }
]' -p dbagentstest@active
```

Do not add extra linked accounts, extra keys, or waits unless there is a very
specific operational reason.

### 3. Update the Local Secret File

Replace the old value in the agent secret file:

```dotenv
DENOTARY_SUBMITTER_PRIVATE_KEY=<NEW_PRIVATE_WIF>
```

On POSIX hosts, keep:

```bash
chmod 600 /etc/denotary-db-agent/agent.secrets.env
```

### 4. Run Preflight Before Restart

Run:

```bash
python -m denotary_db_agent --config /etc/denotary-db-agent/agent.json doctor --source <source_id> --strict
```

Expected signer results:

- `broadcast_backend = "private_key_env"`
- `private_key_source = "env"`
- `permission_exists = true`
- `private_key_matches_permission = true`
- `broadcast_ready = true`

Additional healthy signals:

- `permission_is_minimal_hot_key = true`
- `permission_threshold = 1`
- `permission_public_keys` contains only the new runtime key
- `permission_account_links = []`
- `permission_waits = []`

If `doctor` reports:

- `private_key_matches_permission = false`
- `permission_is_minimal_hot_key = false`
- broad permission warnings

stop and fix the permission before restarting the daemon.

### 5. Run a Controlled Delivery

Before fully trusting the rotated key, do one controlled run:

```bash
python -m denotary_db_agent --config /etc/denotary-db-agent/agent.json run --once
```

Confirm:

- the source can still broadcast
- no DLQ growth occurs
- the new on-chain transaction is signed through the expected permission

### 6. Resume Normal Daemon Operation

If preflight and the controlled run succeed, restart or continue daemon mode.

## Rollback

If the new key fails:

1. stop daemon mode
2. restore the previous on-chain public key on `dnanchor`
3. restore the previous private key in the local `env_file`
4. rerun:

```bash
python -m denotary_db_agent --config /etc/denotary-db-agent/agent.json doctor --source <source_id> --strict
```

5. only then resume daemon mode

## Evidence Capture

Before and after rotation, save snapshots:

```bash
python -m denotary_db_agent --config /etc/denotary-db-agent/agent.json doctor --source <source_id> --save-snapshot
python -m denotary_db_agent --config /etc/denotary-db-agent/agent.json report --source <source_id> --save-snapshot
python -m denotary_db_agent --config /etc/denotary-db-agent/agent.json artifacts --source <source_id> --latest 10
```

This gives you a local evidence trail for the rotation event.
