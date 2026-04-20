# Wave 2 denotary Mainnet Validation

This runbook defines the current real `denotary` mainnet validation path for the `Wave 2` adapters that can already be exercised with local source harnesses.

Current scope:

- `SQLite`
- `Redis`

The validation uses:

- the real adapter
- a real local source (`SQLite` file or Docker-backed `Redis`)
- the local `deNotary` off-chain stack on dynamic `127.0.0.1` ports
- the real `denotary` chain RPC
- finalized receipt and audit export
- the validated mainnet signer path:
  - `submitter = "dbagentstest"`
  - `submitter_permission = "dnanchor"`
  - `broadcast_backend = "private_key_env"`

## Current Mainnet Baseline

Current contract routing:

- verification account: `verif`
- billing account: `verifbill`

Current mainnet policy path:

- `schema_id = 1`
- `policy_id = 1`

Current signer secret path:

- `env_file = data/denotary-live-wallet/agent.secrets.env`
- `submitter_private_key_env = "DENOTARY_SUBMITTER_PRIVATE_KEY"`

## Commands

PowerShell:

```powershell
scripts/run-wave2-denotary-validation.ps1 -Adapter all
scripts/run-wave2-denotary-validation.ps1 -Adapter sqlite
scripts/run-wave2-denotary-validation.ps1 -Adapter redis
```

Shell:

```bash
scripts/run-wave2-denotary-validation.sh all
scripts/run-wave2-denotary-validation.sh sqlite
scripts/run-wave2-denotary-validation.sh redis
```

Python:

```bash
python scripts/run-wave2-denotary-validation.py --adapter all
python scripts/run-wave2-denotary-validation.py --adapter sqlite
python scripts/run-wave2-denotary-validation.py --adapter redis
```

By default the script stores persistent run artifacts under:

- `data/wave2-denotary-validation-<timestamp>/summary.json`

## Expected Result

For each adapter:

1. `doctor` succeeds against the mainnet signer and billing path
2. baseline `run_once()` processes `0` events
3. one source event is inserted
4. the next `run_once()` returns:
   - `processed = 1`
   - `failed = 0`
5. one finalized proof bundle is exported locally
6. the result includes:
   - `request_id`
   - `tx_id`
   - `block_num`
   - `proof_path`

## Interpretation

Passing this validation means:

- the adapter works end-to-end against the real `denotary` mainnet path
- the current `Wave 2` signer model matches the production-style `private_key_env` baseline
- the local off-chain services, the real chain, and the finality/receipt/audit path agree enough to export a finalized proof bundle

It does not yet mean:

- every `Wave 2` adapter is mainnet-validated
- restart, soak, and outage depth already matches `Wave 1`
- `Db2`, `Cassandra`, or `Elasticsearch` are already confirmed on mainnet
