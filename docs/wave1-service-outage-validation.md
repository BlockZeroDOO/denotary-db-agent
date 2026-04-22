# Wave 1 Service Outage Validation

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

This runbook defines a bounded recovery drill for temporary off-chain service outages during the notarization pipeline.

It validates recovery for:

- `ingress` prepare outage
- `watcher` register outage
- `receipt` fetch outage
- `audit` fetch outage

The validation is intentionally pipeline-level and uses `dry_run_events`, so it focuses on service recovery rather than source-side CDC behavior.

## Drill Shape

For each scenario:

1. start local mocked off-chain services
2. inject one temporary failure into the target component
3. run `engine.run_once()` and confirm one failed event
4. run `engine.run_once()` again after the temporary fault is gone
5. confirm proof export succeeds and delivery reaches `finalized_exported`

## Command

Use one of:

- [scripts/run-wave1-service-outage-validation.ps1](../scripts/run-wave1-service-outage-validation.ps1)
- [scripts/run-wave1-service-outage-validation.sh](../scripts/run-wave1-service-outage-validation.sh)

The underlying Python driver is:

- [scripts/run-wave1-service-outage-validation.py](../scripts/run-wave1-service-outage-validation.py)
