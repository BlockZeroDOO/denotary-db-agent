# Wave 2 Jungle4 Validation

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

This document describes the current Jungle4 validation path for `Wave 2` adapters that can be executed with local source harnesses today.

Current scope:

- `SQLite`
- `Redis`

The validation uses:

- the real adapter
- a real local source (`SQLite` file or Docker-backed `Redis`)
- the local `deNotary` off-chain stack on dynamic `127.0.0.1` ports
- the real `Jungle4` chain RPC
- real transaction broadcast through `cleos_wallet`

## Current Jungle4 Baseline

Validated submitter path:

- `submitter = "newtestactio"`
- `submitter_permission = "owner"`
- `broadcast_backend = "cleos_wallet"`
- `wallet_command = ["wsl", "cleos"]`

Current contract routing:

- verification account: `decentrfstor`
- billing account: `vadim1111111`

Current baseline IDs:

- `schema_id = 1776342316`
- `policy_id = 1776343316`

## Commands

PowerShell:

```powershell
scripts/run-wave2-jungle4-validation.ps1 -Adapter all
scripts/run-wave2-jungle4-validation.ps1 -Adapter sqlite
scripts/run-wave2-jungle4-validation.ps1 -Adapter redis
```

Shell:

```bash
scripts/run-wave2-jungle4-validation.sh all
scripts/run-wave2-jungle4-validation.sh sqlite
scripts/run-wave2-jungle4-validation.sh redis
```

Python:

```bash
python scripts/run-wave2-jungle4-validation.py --adapter all
python scripts/run-wave2-jungle4-validation.py --adapter sqlite
python scripts/run-wave2-jungle4-validation.py --adapter redis
```

## Expected Result

For each adapter:

1. `doctor --strict` succeeds against the Jungle4 signer and billing path
2. baseline `run_once()` processes `0` events
3. one source event is inserted
4. the next `run_once()` returns:
   - `processed = 1`
   - `failed = 0`
5. one delivery record is persisted locally
6. the result includes:
   - `request_id`
   - `tx_id`
   - `block_num`
   - optional proof bundle path when finality export is available

## Current Interpretation

Passing this validation means:

- the adapter works end-to-end with the real Jungle4 broadcast path
- the current Wave 2 signer model can use an unlocked wallet on testnet
- the local off-chain services accept the on-chain inclusion handoff for this testnet path

It does not yet mean:

- every Wave 2 adapter is Jungle4-validated
- the same adapter is already mainnet-validated
- long soak on Jungle4 is already complete for Wave 2
- the current Jungle4 history endpoints are sufficient for finalized proof export
