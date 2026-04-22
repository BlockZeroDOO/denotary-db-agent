# Wave 2 denotary Mainnet Budget Validation

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

This runbook defines the bounded real-mainnet batch validation path for the `Wave 2` adapters that are already confirmed on `denotary` mainnet.

Current scope:

- `SQLite`
- `Redis`
- `IBM Db2`
- `Apache Cassandra`
- `ScyllaDB`
- `Elasticsearch`

The budget validation uses:

- the real adapter
- a real local source (`SQLite` file or Docker-backed `Redis`)
- the local `deNotary` off-chain stack on dynamic `127.0.0.1` ports
- the real `denotary` chain RPC
- finalized receipt and audit export
- batch anchoring through `policy_id = 2`
- the validated mainnet signer path:
  - `submitter = "dbagentstest"`
  - `submitter_permission = "dnanchor"`
  - `broadcast_backend = "private_key_env"`

## Current Budget Profile

Current bounded profile:

- `target_kib_per_adapter = 25`
- `batch_size = 100`
- `approx_batch_kib = 9`
- `approx_cycles = 3`
- `approx_total_kib = 27`

Current signer secret path:

- `env_file = data/denotary-live-wallet/agent.secrets.env`
- `submitter_private_key_env = "DENOTARY_SUBMITTER_PRIVATE_KEY"`

## Commands

PowerShell:

```powershell
python scripts/run-wave2-mainnet-budget-validation.py --adapter all --target-kib 25 --batch-size 100
python scripts/run-wave2-mainnet-budget-validation.py --adapter sqlite --target-kib 25 --batch-size 100
python scripts/run-wave2-mainnet-budget-validation.py --adapter redis --target-kib 25 --batch-size 100
python scripts/run-wave2-mainnet-budget-validation.py --adapter db2 --target-kib 25 --batch-size 100
python scripts/run-wave2-mainnet-budget-validation.py --adapter cassandra --target-kib 25 --batch-size 100
python scripts/run-wave2-mainnet-budget-validation.py --adapter scylladb --target-kib 25 --batch-size 100
python scripts/run-wave2-mainnet-budget-validation.py --adapter elasticsearch --target-kib 25 --batch-size 100
```

Shell:

```bash
python scripts/run-wave2-mainnet-budget-validation.py --adapter all --target-kib 25 --batch-size 100
python scripts/run-wave2-mainnet-budget-validation.py --adapter sqlite --target-kib 25 --batch-size 100
python scripts/run-wave2-mainnet-budget-validation.py --adapter redis --target-kib 25 --batch-size 100
python scripts/run-wave2-mainnet-budget-validation.py --adapter db2 --target-kib 25 --batch-size 100
python scripts/run-wave2-mainnet-budget-validation.py --adapter cassandra --target-kib 25 --batch-size 100
python scripts/run-wave2-mainnet-budget-validation.py --adapter scylladb --target-kib 25 --batch-size 100
python scripts/run-wave2-mainnet-budget-validation.py --adapter elasticsearch --target-kib 25 --batch-size 100
```

To persist a stable artifact root:

```bash
python scripts/run-wave2-mainnet-budget-validation.py \
  --target-kib 25 \
  --batch-size 100 \
  --output-root data/wave2-mainnet-budget-latest
```

## Expected Result

For each adapter:

1. `doctor` succeeds against the mainnet signer and billing path
2. baseline `run_once()` processes `0` events
3. `3` batch cycles of `100` events are inserted locally
4. each cycle exports one finalized batch proof
5. the final result includes:
   - `delivery_count = 3`
   - `proof_count = 3`
   - `dlq_count = 0`
   - `request_id`
   - `tx_id`
   - `block_num`
   - `proof_path`

## Interpretation

Passing this validation means:

- the adapter is not only mainnet-happy-path-ready, but also supports bounded real-mainnet batch volume
- the current `Wave 2` signer model is good enough for repeated batch deliveries on `denotary`
- finality, receipt, and audit export remain stable across a small mainnet budget profile

It does not yet mean:

- `Wave 2` has full `Wave 1`-depth soak coverage
- every `Wave 2` adapter is mainnet-batch-validated
