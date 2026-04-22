# Storage Config Reference

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

This document describes the `storage` section of the agent config.

## Example

```json
"storage": {
  "state_db": "./data/agent-state.sqlite3",
  "proof_dir": "./data/proofs",
  "proof_retention": 1000,
  "delivery_retention": 5000,
  "dlq_retention": 1000,
  "diagnostics_snapshot_interval_sec": 900,
  "diagnostics_snapshot_retention": 20,
  "evidence_manifest_retention": 200
}
```

### `state_db`

- Type: `string`
- Required: yes
- Purpose: SQLite file used for checkpoints, deliveries, proofs, and control state

### `proof_dir`

- Type: `string`
- Required: no
- Default: `"runtime/proofs"`

### `proof_retention`

- Type: `integer`
- Required: no
- Default: `0`
- Meaning:
  - `0` disables pruning
  - positive values keep only the newest N proof artifacts per source

### `delivery_retention`

- Type: `integer`
- Required: no
- Default: `0`

### `dlq_retention`

- Type: `integer`
- Required: no
- Default: `0`

### `diagnostics_snapshot_interval_sec`

- Type: `number`
- Required: no
- Default: `0.0`
- Meaning:
  - `0` disables periodic diagnostics snapshots in daemon mode

### `diagnostics_snapshot_retention`

- Type: `integer`
- Required: no
- Default: `20`
- Must be at least: `1`

### `evidence_manifest_retention`

- Type: `integer`
- Required: no
- Default: `200`
- Must be at least: `1`

## Notes

- proof bundle files are pruned together with their SQLite metadata rows
- diagnostics snapshots and evidence manifest retention are separate controls
