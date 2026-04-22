# Wave 2 denotary Mainnet Service-Outage Validation

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

This runbook defines the controlled degraded-service drill for the `Wave 2` adapters already confirmed on real `denotary` mainnet.

Current scope:

- `SQLite`
- `Redis`
- `Apache Cassandra`
- `ScyllaDB`
- `IBM Db2`
- `Elasticsearch`

The validation keeps:

- the real adapter
- a real local source (`SQLite` file or Docker-backed `Redis`)
- the local `deNotary` off-chain stack on dynamic `127.0.0.1` ports
- the real `denotary` chain RPC
- the real mainnet signer path through `private_key_env`

The drill intentionally breaks one local off-chain service URL in config while keeping the rest of the pipeline healthy, so it validates recovery on the real mainnet path without needing to corrupt the chain or signer layer.

## Scenarios

- temporary `ingress` outage
- temporary `watcher` outage
- temporary `receipt` outage
- temporary `audit` outage

## Commands

PowerShell:

```powershell
scripts/run-wave2-mainnet-service-outage-validation.ps1 all
scripts/run-wave2-mainnet-service-outage-validation.ps1 sqlite ingress
scripts/run-wave2-mainnet-service-outage-validation.ps1 redis watcher
scripts/run-wave2-mainnet-service-outage-validation.ps1 cassandra receipt
scripts/run-wave2-mainnet-service-outage-validation.ps1 scylladb receipt
scripts/run-wave2-mainnet-service-outage-validation.ps1 db2 audit
scripts/run-wave2-mainnet-service-outage-validation.ps1 elasticsearch audit
```

Shell:

```bash
scripts/run-wave2-mainnet-service-outage-validation.sh all
scripts/run-wave2-mainnet-service-outage-validation.sh sqlite ingress
scripts/run-wave2-mainnet-service-outage-validation.sh redis watcher
scripts/run-wave2-mainnet-service-outage-validation.sh cassandra receipt
scripts/run-wave2-mainnet-service-outage-validation.sh scylladb receipt
scripts/run-wave2-mainnet-service-outage-validation.sh db2 audit
scripts/run-wave2-mainnet-service-outage-validation.sh elasticsearch audit
```

Python:

```bash
python scripts/run-wave2-mainnet-service-outage-validation.py --adapter all
python scripts/run-wave2-mainnet-service-outage-validation.py --adapter sqlite --scenario ingress
python scripts/run-wave2-mainnet-service-outage-validation.py --adapter redis --scenario watcher
python scripts/run-wave2-mainnet-service-outage-validation.py --adapter cassandra --scenario receipt
python scripts/run-wave2-mainnet-service-outage-validation.py --adapter scylladb --scenario receipt
python scripts/run-wave2-mainnet-service-outage-validation.py --adapter db2 --scenario audit
python scripts/run-wave2-mainnet-service-outage-validation.py --adapter elasticsearch --scenario audit
```

To persist a stable artifact root:

```bash
python scripts/run-wave2-mainnet-service-outage-validation.py \
  --adapter all \
  --output-root data/wave2-mainnet-service-outage-latest
```

For `IBM Db2`, use a dedicated artifact root so the persisted summary reflects the `Db2` run set directly:

```bash
python scripts/run-wave2-mainnet-service-outage-validation.py \
  --adapter db2 \
  --output-root data/wave2-mainnet-service-outage-db2-latest
```

For `Apache Cassandra`, use a dedicated artifact root so the persisted summary reflects the `Cassandra` run set directly:

```bash
python scripts/run-wave2-mainnet-service-outage-validation.py \
  --adapter cassandra \
  --output-root data/wave2-mainnet-service-outage-cassandra-latest
```

For `Elasticsearch`, use a dedicated artifact root so the persisted summary reflects the `Elasticsearch` run set directly:

```bash
python scripts/run-wave2-mainnet-service-outage-validation.py \
  --adapter elasticsearch \
  --output-root data/wave2-mainnet-service-outage-elasticsearch-latest
```

## Expected Recovery Pattern

For each adapter and scenario:

1. baseline `run_once()` processes `0` events
2. one source event is inserted while one service URL is pointed at an unreachable port
3. the first `run_once()` returns:
   - `processed = 0`
   - `failed = 1`
4. the agent is rerun with the healthy config and the same local state
5. the second `run_once()` returns:
   - `processed = 1`
   - `failed = 0`
6. one finalized proof bundle is exported locally

The current artifact pattern also shows:

- `delivery_count = 2`
- `proof_count = 1`
- `dlq_count = 1`

This is expected for the current drill because the first failed attempt is retained in local history while the second attempt completes successfully.

## Interpretation

Passing this validation means:

- `SQLite`, `Redis`, `Apache Cassandra`, `ScyllaDB`, `IBM Db2`, and `Elasticsearch` recover cleanly from temporary off-chain pipeline outages on the real `denotary` mainnet path
- the event is not lost after the first failed attempt
- a second run with healthy service URLs still reaches finalized proof export

It does not yet mean:

- every `Wave 2` adapter has mainnet degraded-service coverage
- `Wave 2` already matches full `Wave 1` validation depth across every adapter
- long outage behavior is fully characterized
