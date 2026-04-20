# Wave 2 denotary Mainnet Service-Outage Validation Report

This report captures the current persisted degraded-service validation state for the `Wave 2` adapters already exercised on the real `denotary` mainnet path.

Common validation baseline:

- local off-chain services on dynamic `127.0.0.1` ports
- chain RPC:
  - `https://history.denotary.io`
- signer path:
  - `broadcast_backend = "private_key_env"`
  - `submitter = "dbagentstest"`
  - `submitter_permission = "dnanchor"`
- single-event policy path:
  - `schema_id = 1`
  - `policy_id = 1`
- persisted run root:
  - `data/wave2-mainnet-service-outage-latest`
- summary:
  - [summary.json](/c:/projects/denotary-db-agent/data/wave2-mainnet-service-outage-latest/summary.json)

## Verified Recovery Pattern

For every passing scenario, the observed recovery contract is:

- baseline `run_once()` processes `0` events
- first attempt returns:
  - `processed = 0`
  - `failed = 1`
- second attempt returns:
  - `processed = 1`
  - `failed = 0`
- finalized proof export succeeds after the temporary outage is removed

Current artifact counts per passing scenario:

- `delivery_count = 2`
- `proof_count = 1`
- `dlq_count = 1`

## Current Result Summary

`SQLite`

- `ingress`: passed
  - `request_id = 62d921a4aceb03643c7188d00d7ce5a47183457e27146d3bbd3184160f791a17`
  - `tx_id = 5748d6cd4f969b5b7536bdef6b992f9e6f0a38889e6b9f9b2c3e12eb6cf0936d`
  - `block_num = 1112127`
- `watcher`: passed
  - `request_id = 8061d7e72d4037982e67a9a3ae4bba657b74b73d9ba2f4ecef19f9f12605d98f`
  - `tx_id = 448661e1e92fd58833b0b6860a355feb9086ad64fba42520d6b66d6e5300c4a1`
  - `block_num = 1112251`
- `receipt`: passed
  - `request_id = 23f8917016ef5a29280174654f6f9928014a8df72aa9a1261a713e7e37c6ec83`
  - `tx_id = c9bf09849807523ae76a9bb273de3164f2072a1b59647dbab217a8c570e61f1f`
  - `block_num = 1112346`
- `audit`: passed
  - `request_id = 117a8f25cceecea3eca61bc5bf9c7591bf0a4a3ddca24fe31f12c467be10c546`
  - `tx_id = 602cdba310b0de053520f46622170b1727079e4791481572fe6b1f2c80aa791d`
  - `block_num = 1112469`

`Redis`

- `ingress`: passed
  - `request_id = f36c49fb7e68de5d821e25d93b13c6118d6db6f3b7eeb8a7f9f9f09f0d47692e`
  - `tx_id = abefe348ee3987d5ce6a9b12cca747dc3f3157116cb04e5be1a0a31d6e4dd86e`
  - `block_num = 1112609`
- `watcher`: passed
  - `request_id = 098760e80c545624141ac65a5a407c5758210fa2f7f303f575bcb824def43729`
  - `tx_id = 826aecd43c496dbbd53f216306f5c78e20bcc3fdf1842254458922933b44d95d`
  - `block_num = 1112735`
- `receipt`: passed
  - `request_id = 84e4bf60635d9efd8c0ec4be6decf0fc12fe4687561b82562e706319b8b3dbf1`
  - `tx_id = f7c9d149b948acfb454fb002f41022bbe32e4d9ca78d568cccb1d1217a40a4fe`
  - `block_num = 1112867`
- `audit`: passed
  - `request_id = 0b914857becaf90481788fefa04d4e0027b01a9fd178b2ec146cc5da56569496`
  - `tx_id = f02e404260c2c403ced2a933a77b5e463007489fc63e4d46ca942a1c8eee76b6`
  - `block_num = 1113000`

## Interpretation

This closes the first real-mainnet degraded-service recovery layer for:

- file-backed edge / embedded `SQLite`
- operational-state `Redis`

Together with real mainnet happy-path and bounded budget validation, these adapters now have a stronger `Wave 2` readiness story than baseline proof export alone.
