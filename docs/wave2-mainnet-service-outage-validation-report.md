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
  - `request_id = 64dcbea99ff59c70a08bd5654d77c4f2e2798eb5eb5c422b68779f167abdaedf`
  - `tx_id = 87b963e7b86ac60456db30296c0175a91cc3ee8adf4c225cbb065e7244b2a55f`
  - `block_num = 1150892`
- `watcher`: passed
  - `request_id = 0cf7db9ca6ab7093d1b85c7dfda66dd3939c8acf95456aa334bf7f307a9ccd54`
  - `tx_id = 877604836fa91426e8cc6112e127dbf745da24f331ddbc15e07fadf2b6fed1e1`
  - `block_num = 1151012`
- `receipt`: passed
  - `request_id = ce308fe8591585280df62ccd9c4dc8ab849682c0b7e274e5672ef2ffd9587e4a`
  - `tx_id = f3982df7510f059cd3e4b4a0bcc1ef5da186a62ba90d3570b186ab98e7249acd`
  - `block_num = 1151106`
- `audit`: passed
  - `request_id = 8108b7d82ba3e992a0827338297af0bd1a7e1f51aedd40847050af8567c97c47`
  - `tx_id = 11972afe797048e87d557aa50ce1d5b9d947542217f84aa052de78fe1d9fe2e8`
  - `block_num = 1151227`

`Redis`

- `ingress`: passed
  - `request_id = 036f25e1b952650a621ea5ef4381cf28b5605f906c5d6012a4d1cd4b35fd3d4c`
  - `tx_id = aa56c16a353f4e8cf5261cdf2c2ec4719312dd9aec69db54f746d2ea4ff2cf5b`
  - `block_num = 1151363`
- `watcher`: passed
  - `request_id = eb50dfe4f3f6c76eabdce3a27c19edc13317a0d685c2a130f6c173355b7759b6`
  - `tx_id = 5638d242c626b592101fc4b8184577b0b37c95d7a7fe727c8ae26ffd2f79847f`
  - `block_num = 1151497`
- `receipt`: passed
  - `request_id = f0c7db5b7fbb66f4ec5a62da854c980183f28d1ee08384c0b8fd92be5ae65519`
  - `tx_id = a6a3ad2dc49e8b2ef099bfcf9a90bcb4de6f5240b0a34d5325314cbb135c8e21`
  - `block_num = 1151603`
- `audit`: passed
  - `request_id = fcddf47c72b5d0cf5e71b35d6a6155f5fb9b4f37519013b2e4f503a9a026ff91`
  - `tx_id = b6dad4c3748dc10dffaf88b85aaefafaa9f79ec60cb58013e4510342f71efc5e`
  - `block_num = 1151736`

`ScyllaDB`

- `ingress`: passed
  - `request_id = 4ff8f1beda91d315c43eccdcfe235db62aa02278d22b7283c61395d77a6809ae`
  - `tx_id = e5ef90e1a42cb4f86c38a365c30cdb377ac9bd663c2f912e128948ff22b184d3`
  - `block_num = 1152005`
- `watcher`: passed
  - `request_id = ca9a423a591936482c174fc61982c6e07f7941c1916365ba9190db8869539781`
  - `tx_id = 0cdfff8cc152480aeb0d7cc4c74f86d6e3d89af983deff44eef40647ec412827`
  - `block_num = 1152267`
- `receipt`: passed
  - `request_id = 5febb73e2f5345438cb429272cc22f3c6074c0e0ffde64ee3273c888b246276b`
  - `tx_id = d1b103d170c8e048dc6d90c5606aab6c668e57102543113e1f7ba0e103f3994d`
  - `block_num = 1152486`
- `audit`: passed
  - `request_id = fdeb238cb1b9fc56f4831706a4e1617ca16c7c27fd88c08aced7cbb48b828d3a`
  - `tx_id = 23fc858d336f9f73b7f0d370e0bb77a4cb508fc881b971257c1ea9cf672a8f11`
  - `block_num = 1152767`

## Interpretation

This closes the first real-mainnet degraded-service recovery layer for:

- file-backed edge / embedded `SQLite`
- operational-state `Redis`
- wide-column `ScyllaDB`

Together with real mainnet happy-path validation, these adapters now have a stronger `Wave 2` readiness story than baseline proof export alone. For `SQLite` and `Redis`, this layer also stacks on top of bounded mainnet budget validation; for `ScyllaDB`, the next step is still the bounded budget layer.
