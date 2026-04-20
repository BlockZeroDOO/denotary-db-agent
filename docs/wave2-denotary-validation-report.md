# Wave 2 denotary Mainnet Validation Report

This report captures the current persisted real mainnet `denotary` validation runs for the `Wave 2` adapters already exercised against the production chain path.

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
- policy path:
  - `schema_id = 1`
  - `policy_id = 1`
- persisted run root:
  - `data/wave2-denotary-latest`
- summary:
  - [summary.json](/c:/projects/denotary-db-agent/data/wave2-denotary-latest/summary.json)

## SQLite

- result:
  - `processed = 1`
  - `failed = 0`
- request:
  - `request_id = 8a32e19c0c41521e46b11e72daaf7dcf51967dcfd513742d9781d6d98d44bb8d`
- chain:
  - `tx_id = 811e3b2e14a69766887238076f359feaf426a1d1aca0683fa24dea1ed601ec08`
  - `block_num = 1109233`
- proof:
  - [8a32e19c0c41521e46b11e72daaf7dcf51967dcfd513742d9781d6d98d44bb8d.json](/c:/projects/denotary-db-agent/data/wave2-denotary-latest/sqlite/proofs/sqlite-wave2-denotary/8a32e19c0c41521e46b11e72daaf7dcf51967dcfd513742d9781d6d98d44bb8d.json)

## Redis

- result:
  - `processed = 1`
  - `failed = 0`
- request:
  - `request_id = da62a8dcd832f726d361898e3d0d601fe0314d2c6733fd6d64f3eacf6d6fd34e`
- chain:
  - `tx_id = 6093cd08bcb29892f43d45f2670ef576e6593083f67efef5d79df4feebc01988`
  - `block_num = 1109331`
- proof:
  - [da62a8dcd832f726d361898e3d0d601fe0314d2c6733fd6d64f3eacf6d6fd34e.json](/c:/projects/denotary-db-agent/data/wave2-denotary-latest/redis/proofs/redis-wave2-denotary/da62a8dcd832f726d361898e3d0d601fe0314d2c6733fd6d64f3eacf6d6fd34e.json)

## Interpretation

Current `Wave 2` real-mainnet confirmation now exists for:

- `SQLite`
- `Redis`
- `ScyllaDB`

That means the current `Wave 2` set is no longer only local-validation-ready. It already includes:

- edge/local-first notarization on real mainnet
- operational key-value state notarization on real mainnet
- distributed wide-column notarization on real mainnet
- finalized proof export through the same production-style signer model used by `Wave 1`

## ScyllaDB

- result:
  - `processed = 1`
  - `failed = 0`
- request:
  - `request_id = 5fa1675f2d688df34b3612b75d34abf892f8509936144174222c92ce2e5d869a`
- chain:
  - `tx_id = 991abdb365530db670a3f735e8083a2013dea77c609e52acd301d59dd441cb90`
  - `block_num = 1134971`
- proof:
  - [5fa1675f2d688df34b3612b75d34abf892f8509936144174222c92ce2e5d869a.json](/c:/projects/denotary-db-agent/data/wave2-denotary-live-validation-scylladb/scylladb/proofs/scylladb-wave2-denotary-live/5fa1675f2d688df34b3612b75d34abf892f8509936144174222c92ce2e5d869a.json)
