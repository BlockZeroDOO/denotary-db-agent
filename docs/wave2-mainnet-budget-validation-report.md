# Wave 2 denotary Mainnet Budget Validation Report

This report captures the current persisted bounded real-mainnet batch validation runs for the `Wave 2` adapters already exercised against the production `denotary` chain path.

Common validation baseline:

- local off-chain services on dynamic `127.0.0.1` ports
- chain RPC:
  - `https://history.denotary.io`
- signer path:
  - `broadcast_backend = "private_key_env"`
  - `submitter = "dbagentstest"`
  - `submitter_permission = "dnanchor"`
- billing path:
  - `billing_account = "verifbill"`
- batch policy path:
  - `schema_id = 1`
  - `policy_id = 2`
- bounded profile:
  - `target_kib_per_adapter = 25`
  - `batch_size = 100`
  - `approx_batch_kib = 9`
  - `approx_cycles = 3`
  - `approx_total_kib = 27`
- persisted run roots:
  - `data/wave2-mainnet-budget-latest`
  - `data/wave2-mainnet-budget-scylladb-latest`
- summaries:
  - [wave2-mainnet-budget-latest/summary.json](/c:/projects/denotary-db-agent/data/wave2-mainnet-budget-latest/summary.json)
  - [wave2-mainnet-budget-scylladb-latest/summary.json](/c:/projects/denotary-db-agent/data/wave2-mainnet-budget-scylladb-latest/summary.json)

## SQLite

- result:
  - `delivery_count = 3`
  - `proof_count = 3`
  - `dlq_count = 0`
- request:
  - `request_id = 66c8c582242e67ae29c8be49bf85f7d9bfdaa136a88dfbc011c96138b8a6d40a`
- chain:
  - `tx_id = 431191cb5c4b5bb3c37b2b8f6f8c1be555fae98a207926689fdc86b8bd018f5b`
  - `block_num = 1110244`
- proof:
  - [66c8c582242e67ae29c8be49bf85f7d9bfdaa136a88dfbc011c96138b8a6d40a.json](/c:/projects/denotary-db-agent/data/wave2-mainnet-budget-latest/sqlite/proofs/sqlite-wave2-denotary-budget/66c8c582242e67ae29c8be49bf85f7d9bfdaa136a88dfbc011c96138b8a6d40a.json)

## Redis

- result:
  - `delivery_count = 3`
  - `proof_count = 3`
  - `dlq_count = 0`
- request:
  - `request_id = 49aeed8197a3d0b9bb21df361b8bd98e1eca9396e04af916580a8a988ff1e361`
- chain:
  - `tx_id = 46110b5be352a1881db528a6bd122cb7077e735e387100eaf7322b20333eeeaf`
  - `block_num = 1110535`
- proof:
  - [49aeed8197a3d0b9bb21df361b8bd98e1eca9396e04af916580a8a988ff1e361.json](/c:/projects/denotary-db-agent/data/wave2-mainnet-budget-latest/redis/proofs/redis-wave2-denotary-budget/49aeed8197a3d0b9bb21df361b8bd98e1eca9396e04af916580a8a988ff1e361.json)

## ScyllaDB

- result:
  - `delivery_count = 3`
  - `proof_count = 3`
  - `dlq_count = 0`
- request:
  - `request_id = f694b401414fb84df9384a810fb75d1bf4a4a7bbf28d7df250149ce2e30d5411`
- chain:
  - `tx_id = 4cc5fef652fb7320498848af16ebe02c4caae7d23da013b09a70bcfcbfe5dc18`
  - `block_num = 1157301`
- proof:
  - [f694b401414fb84df9384a810fb75d1bf4a4a7bbf28d7df250149ce2e30d5411.json](/c:/projects/denotary-db-agent/data/wave2-mainnet-budget-scylladb-latest/scylladb/cycle-03/proofs/scylladb-wave2-denotary-budget-03/f694b401414fb84df9384a810fb75d1bf4a4a7bbf28d7df250149ce2e30d5411.json)

## Interpretation

Current `Wave 2` bounded real-mainnet budget confirmation now exists for:

- `SQLite`
- `Redis`
- `ScyllaDB`

That means the current `Wave 2` set already includes:

- real mainnet single-event validation
- real mainnet bounded batch validation
- finalized proof export through the same production-style signer model used by `Wave 1`
