# denotary Wave 1 Mainnet Validation Report

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

This report captures the real mainnet `denotary` validation runs for the Wave 1 database adapters beyond the already documented PostgreSQL path.

Common validation baseline:

- local off-chain services:
  - `Ingress API` on `127.0.0.1:8080`
  - `Finality Watcher` on `127.0.0.1:8081`
  - `Receipt Service` on `127.0.0.1:8082`
  - `Audit API` on `127.0.0.1:8083`
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
- validation sequence:
  - `doctor`
  - `bootstrap`
  - insert one fresh source record
  - `run --once`
  - `report --save-snapshot`

## MySQL

- source id:
  - `mysql-denotary-mainnet-1776557788`
- result:
  - `processed = 1`
  - `failed = 0`
- request:
  - `request_id = d652ffb95e1407656c1c292aab37b3fcba4ef9c8830d17a01553b53bbf5b8e94`
- chain:
  - `tx_id = 07e2c1c404e88a33a80f2d0a989606399a3bb4442801050278e6c635cdfde653`
  - `block_num = 816383`
- artifacts:
  - [config](</c:/projects/denotary-db-agent/data/denotary-mysql-mainnet-1776557788/agent.1776557788.json>)
  - [proof bundle](</c:/projects/denotary-db-agent/data/denotary-mysql-mainnet-1776557788/proofs/mysql-denotary-mainnet-1776557788/d652ffb95e1407656c1c292aab37b3fcba4ef9c8830d17a01553b53bbf5b8e94.json>)
  - [report snapshot](</c:/projects/denotary-db-agent/data/denotary-mysql-mainnet-1776557788/diagnostics/report-mysql-denotary-mainnet-1776557788-20260419T001734Z.json>)

## MariaDB

- source id:
  - `mariadb-denotary-mainnet-1776557967`
- result:
  - `processed = 1`
  - `failed = 0`
- request:
  - `request_id = b5a5d47c02383d66d52247e75f1591e049f70ce5bdf33492d548f605f0264782`
- chain:
  - `tx_id = 8291b6b23f83c0ddac61e6178c7a33ce595656caf2f4a6462ef6d46adb2a30c6`
  - `block_num = 816755`
- artifacts:
  - [config](</c:/projects/denotary-db-agent/data/denotary-mariadb-mainnet-1776557967/agent.1776557967.json>)
  - [proof bundle](</c:/projects/denotary-db-agent/data/denotary-mariadb-mainnet-1776557967/proofs/mariadb-denotary-mainnet-1776557967/b5a5d47c02383d66d52247e75f1591e049f70ce5bdf33492d548f605f0264782.json>)
  - [report snapshot](</c:/projects/denotary-db-agent/data/denotary-mariadb-mainnet-1776557967/diagnostics/report-mariadb-denotary-mainnet-1776557967-20260419T002033Z.json>)

## SQL Server

- source id:
  - `sqlserver-denotary-mainnet-1776558033`
- result:
  - `processed = 1`
  - `failed = 0`
- request:
  - `request_id = 5d45a3ba55084723a517e7b6771cd83bc1088398163025d58c7e02fdf21459ca`
- chain:
  - `tx_id = 316d29db275b0ab09d2704a7d60764b939b38e1a95f2354e0a3821ca235b8b4d`
  - `block_num = 816917`
- artifacts:
  - [config](</c:/projects/denotary-db-agent/data/denotary-sqlserver-mainnet-1776558033/agent.1776558033.json>)
  - [proof bundle](</c:/projects/denotary-db-agent/data/denotary-sqlserver-mainnet-1776558033/proofs/sqlserver-denotary-mainnet-1776558033/5d45a3ba55084723a517e7b6771cd83bc1088398163025d58c7e02fdf21459ca.json>)
  - [report snapshot](</c:/projects/denotary-db-agent/data/denotary-sqlserver-mainnet-1776558033/diagnostics/report-sqlserver-denotary-mainnet-1776558033-20260419T002144Z.json>)

## Oracle

- source id:
  - `oracle-denotary-mainnet-1776558211`
- result:
  - `processed = 1`
  - `failed = 0`
- request:
  - `request_id = 107e01c2cb0067373622bde0586fead39c6a09ddb1b24719aae8ad605d44e40f`
- chain:
  - `tx_id = cc6fe33f2fa9e57252e25ce933c8a1732a4034a92f050b4185e78358992f4836`
  - `block_num = 817239`
- artifacts:
  - [config](</c:/projects/denotary-db-agent/data/denotary-oracle-mainnet-1776558211/agent.1776558211.json>)
  - [proof bundle](</c:/projects/denotary-db-agent/data/denotary-oracle-mainnet-1776558211/proofs/oracle-denotary-mainnet-1776558211/107e01c2cb0067373622bde0586fead39c6a09ddb1b24719aae8ad605d44e40f.json>)
  - [report snapshot](</c:/projects/denotary-db-agent/data/denotary-oracle-mainnet-1776558211/diagnostics/report-oracle-denotary-mainnet-1776558211-20260419T002438Z.json>)

## MongoDB

- source id:
  - `mongodb-denotary-mainnet-1776558278`
- result:
  - `processed = 1`
  - `failed = 0`
- request:
  - `request_id = 89ca672c0dfdb5dcea2963a5cc4766af4d9547dee9afed73a3053d1de675d557`
- chain:
  - `tx_id = a54ba661d45d17b8458ee8b7fb852b6767f90bd21ca55d937665f08eaab46818`
  - `block_num = 817378`
- artifacts:
  - [config](</c:/projects/denotary-db-agent/data/denotary-mongodb-mainnet-1776558278/agent.1776558278.json>)
  - [proof bundle](</c:/projects/denotary-db-agent/data/denotary-mongodb-mainnet-1776558278/proofs/mongodb-denotary-mainnet-1776558278/89ca672c0dfdb5dcea2963a5cc4766af4d9547dee9afed73a3053d1de675d557.json>)
  - [report snapshot](</c:/projects/denotary-db-agent/data/denotary-mongodb-mainnet-1776558278/diagnostics/report-mongodb-denotary-mainnet-1776558278-20260419T002538Z.json>)

## Outcome

Wave 1 mainnet validation is now confirmed for:

- PostgreSQL
- MySQL
- MariaDB
- SQL Server
- Oracle
- MongoDB

This closes the real `denotary` happy-path validation milestone for all currently supported Wave 1 adapters.

The next recommended validation layers are:

- short soak runs for non-PostgreSQL adapters
- reconnect / source restart / backlog recovery drills
- per-adapter recovery runbooks where native CDC is used
