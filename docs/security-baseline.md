# deNotary DB Agent Security Baseline

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

This document describes the current minimum security baseline for deploying
`denotary-db-agent` in enterprise environments.

It is written for operators, security reviewers, and infrastructure teams that
need a clear explanation of the agent trust boundary.

## Core Security Model

`denotary-db-agent` is not only a database reader.

In a live notarization deployment it also:

- sends canonical request payloads to `Ingress`
- receives a `prepared_action` back from `Ingress`
- signs and broadcasts that prepared action with an enterprise hot key
- relies on `Watcher`, `Receipt`, and `Audit` for finality and proof export

That means the security boundary is not limited to the agent process itself.

The agent and the following services should be treated as one trusted
operational zone:

- `Ingress`
- `Finality Watcher`
- `Receipt`
- `Audit`

## Why Local deNotary Services Matter

The current production baseline assumes that the deNotary off-chain services are
local to the host, local to the deployment stack, or at least private to the
same controlled network segment.

Why this matters:

- `Ingress` participates in constructing the request that the enterprise signer
  will later authorize
- `Watcher`, `Receipt`, and `Audit` are part of the proof-finalization path
- exposing these services broadly increases the chance of tampering,
  interception, or misrouting
- enterprise operators usually need these components to be inside the same
  operational trust boundary as the agent

Recommended baseline:

- run `Ingress`, `Watcher`, `Receipt`, and `Audit` on the same host, on the
  same private cluster, or behind private-only network access
- do not expose these services directly to the public internet unless a
  separate hardening layer exists
- treat chain RPC separately from the local deNotary backend; chain RPC can be
  remote, but the off-chain notarization services should remain private

## Enterprise Signer Baseline

The recommended signer model is:

- `submitter_permission = "dnanchor"`
- `broadcast_backend = "private_key_env"`
- hot key loaded from a local `env_file` or equivalent secret mount

This model exists to reduce blast radius.

## Why `owner` and `active` Must Not Be Used

Enterprise production deployments should not run the DB Agent with:

- `submitter_permission = "owner"`
- `submitter_permission = "active"`

Why this matters:

- `owner` is the account recovery and ultimate control permission
- `active` is usually much broader than notarization signing
- compromise of the DB Agent host would then become compromise of the
  enterprise account, not just the notarization function
- a dedicated permission such as `dnanchor` keeps the hot runtime key narrow
  and easier to audit

The DB Agent hot permission should be linked only to:

- `verifbill::submit`
- `verifbill::submitroot`

It should not be linked to:

- `eosio.token::transfer`
- account-management actions
- broader treasury or admin workflows

## Sensitive Local Artifacts

The following local artifacts should be treated as confidential operational
data:

- `storage.state_db`
- `storage.proof_dir`
- saved `doctor`, `diagnostics`, and `report` snapshots
- the local `env_file` or secret mount that contains the hot key

Why:

- local state contains delivery metadata and proof metadata
- local state may also contain event context used for delivery recovery and
  reconciliation
- proof bundles and evidence snapshots are useful for audit, but they still
  expose operational context and should not be published broadly

Recommended baseline:

- keep these paths under service-owned directories
- restrict filesystem access to the service account
- avoid putting them in shared temp directories or world-readable locations
- do not commit secret files to source control
- do not upload evidence snapshots to public artifact storage

## Minimum Production Checklist

Before first production start:

1. place `Ingress`, `Watcher`, `Receipt`, and `Audit` inside the same trusted
   deployment boundary as the agent
2. configure `submitter_permission = "dnanchor"`
3. keep `owner` offline and `active` off the application host
4. use `broadcast_backend = "private_key_env"`
5. keep the hot key in `env_file` or an equivalent secret mount
6. run `doctor --strict`
7. verify that local state and proof paths are service-owned and access
   controlled

## Related Documents

- [security-minimal-roadmap.md](security-minimal-roadmap.md)
- [architecture.md](architecture.md)
- [operator-guide.md](operator-guide.md)
- [denotary-env-file-runbook.md](denotary-env-file-runbook.md)
- [verifbill-permission-model.md](verifbill-permission-model.md)

