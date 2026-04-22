# Wave 1 Mainnet Service Outage Validation

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

This runbook defines a controlled degraded-service drill for the real `denotary` mainnet path.

It validates temporary outages in the local off-chain stack while keeping:

- real `denotary` chain RPC
- real signer path
- real `verifbill` broadcast path

The drill is intentionally pipeline-level and uses a synthetic `dry_run_events` source, so it validates service recovery without needing a live database mutation for each scenario.

## Scenarios

- temporary `ingress` outage
- temporary `watcher` outage
- temporary `receipt` outage
- temporary `audit` outage

## Drill Shape

For each scenario:

1. load a known-good live mainnet config
2. create a temporary config with one service URL pointed at an unreachable local port
3. run `engine.run_once()` and confirm one failed event
4. rerun with the healthy config and the same local state
5. confirm proof export succeeds and a delivery reaches `finalized_exported`

## Command

Use one of:

- [scripts/run-wave1-mainnet-service-outage-validation.ps1](../scripts/run-wave1-mainnet-service-outage-validation.ps1)
- [scripts/run-wave1-mainnet-service-outage-validation.sh](../scripts/run-wave1-mainnet-service-outage-validation.sh)

The underlying Python driver is:

- [scripts/run-wave1-mainnet-service-outage-validation.py](../scripts/run-wave1-mainnet-service-outage-validation.py)

Example:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/run-wave1-mainnet-service-outage-validation.ps1 --config data/denotary-live-env-e2e/agent.1776475214.json
```
