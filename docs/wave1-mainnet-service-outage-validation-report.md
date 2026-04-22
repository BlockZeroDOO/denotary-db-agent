# Wave 1 Mainnet Service Outage Validation Report

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

This report captures the first live degraded-service drill against the real `denotary` mainnet path.

Validation command:

```powershell
C:\Python39\python.exe scripts\run-wave1-mainnet-service-outage-validation.py --config data/denotary-live-env-e2e/agent.1776475214.json
```

Validation baseline:

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
- source path:
  - synthetic `dry_run_events` source for pipeline-only validation

Results:

- `ingress` outage:
  - first run: `processed = 0`, `failed = 1`
  - second run: `processed = 1`, `failed = 0`
  - `delivery_count = 2`
  - `proof_count = 1`
  - `dlq_count = 1`
  - `request_id = ca3a1897e153c359ff1fc0b18e0b61088c4ed6c701b6ac5c7b42be834229f34d`
- `watcher` outage:
  - first run: `processed = 0`, `failed = 1`
  - second run: `processed = 1`, `failed = 0`
  - `delivery_count = 2`
  - `proof_count = 1`
  - `dlq_count = 1`
  - `request_id = e1c9829a099eaeac2ae53770180b81f7a8cf50ec18154bf45e098caf26ba3a6c`
- `receipt` outage:
  - first run: `processed = 0`, `failed = 1`
  - second run: `processed = 1`, `failed = 0`
  - `delivery_count = 3`
  - `proof_count = 1`
  - `dlq_count = 1`
  - `request_id = 12849fc7741abc00913b3ebe0ca78b2be8e159da3411353faa31991a2a621005`
- `audit` outage:
  - first run: `processed = 0`, `failed = 1`
  - second run: `processed = 1`, `failed = 0`
  - `delivery_count = 3`
  - `proof_count = 1`
  - `dlq_count = 1`
  - `request_id = 881781b4859793507ee81410ba063c95953e0b8bed8479e37c727f253c68bda5`

Conclusion:

- the real `denotary` mainnet path now has a validated degraded-service recovery drill for:
  - `ingress`
  - `watcher`
  - `receipt`
  - `audit`
- recovery remains consistent with the local outage model:
  - first run fails
  - next run succeeds
  - proof export reaches `finalized_exported`
