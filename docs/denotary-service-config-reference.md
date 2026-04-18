# deNotary Service Config Reference

This document describes the `denotary` section of the agent config.

## Example

```json
"denotary": {
  "ingress_url": "http://127.0.0.1:8080",
  "watcher_url": "http://127.0.0.1:8081",
  "watcher_auth_token": "integration-shared-token",
  "receipt_url": "http://127.0.0.1:8082",
  "audit_url": "http://127.0.0.1:8083",
  "chain_rpc_url": "https://history.denotary.io",
  "submitter": "enterpriseac1",
  "submitter_permission": "dnanchor",
  "broadcast_backend": "private_key_env",
  "submitter_private_key_env": "DENOTARY_SUBMITTER_PRIVATE_KEY",
  "env_file": "./examples/agent.secrets.env",
  "wallet_command": ["cleos"],
  "submitter_private_key": "",
  "schema_id": 1,
  "policy_id": 1,
  "billing_account": "verifbill",
  "wait_for_finality": true,
  "finality_timeout_sec": 180,
  "finality_poll_interval_sec": 3.0
}
```

### `ingress_url`

- Type: `string`
- Required: yes
- Purpose: base URL of the deNotary Ingress API

### `watcher_url`

- Type: `string`
- Required: yes
- Purpose: base URL of Finality Watcher

### `watcher_auth_token`

- Type: `string`
- Required: no
- Default: `""`

### `receipt_url`

- Type: `string`
- Required: no
- Default: `""`

### `audit_url`

- Type: `string`
- Required: no
- Default: `""`

### `chain_rpc_url`

- Type: `string`
- Required: no
- Default: `""`

### `submitter`

- Type: `string`
- Required: yes
- Purpose: enterprise payer account used to sign `verifbill::submit` and `verifbill::submitroot`

### `submitter_permission`

- Type: `string`
- Required: no
- Default: `"dnanchor"`
- Recommendation:
  - use a dedicated custom permission such as `dnanchor`
  - avoid `owner` and `active` in production

### `broadcast_backend`

- Type: `string`
- Required: no
- Default: `"auto"`
- Allowed values:
  - `"auto"`
  - `"private_key"`
  - `"private_key_env"`
  - `"cleos_wallet"`

### `submitter_private_key_env`

- Type: `string`
- Required: no
- Default: `"DENOTARY_SUBMITTER_PRIVATE_KEY"`
- Used when:
  - `broadcast_backend = "private_key_env"`
  - or `broadcast_backend = "auto"` and no inline key is configured

### `env_file`

- Type: `string`
- Required: no
- Default: `""`
- Purpose: optional dotenv-style file loaded before resolving `submitter_private_key_env`
- Recommendation:
  - keep it outside git
  - on POSIX hosts keep it at `0600` or stricter

### `wallet_command`

- Type: `string[]`
- Required: no
- Default: `[]`
- Used only when:
  - `broadcast_backend = "cleos_wallet"`

### `submitter_private_key`

- Type: `string`
- Required: no
- Default: `""`
- Recommendation:
  - debug/bootstrap only
  - prefer `private_key_env` + `env_file`

### `schema_id`

- Type: `integer`
- Required: yes

### `policy_id`

- Type: `integer`
- Required: yes

### `billing_account`

- Type: `string`
- Required: no
- Default: `"verifbill"`

### `wait_for_finality`

- Type: `boolean`
- Required: no
- Default: `false`

### `finality_timeout_sec`

- Type: `integer`
- Required: no
- Default: `120`

### `finality_poll_interval_sec`

- Type: `number`
- Required: no
- Default: `2.0`

## Security Notes

- Keep `owner` offline in production.
- Do not place `active` on the DB Agent host.
- Prefer a minimal hot permission such as `dnanchor`.
- Prefer `private_key_env` with a restricted `env_file`.
