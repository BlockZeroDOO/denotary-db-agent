# Jungle4 PostgreSQL Short Soak Report

This report captures the current stable short soak baseline for PostgreSQL
against the `Jungle4` validation environment.

It supersedes the earlier `verification@dnanchor` short-run baseline that was
useful for debugging, but not yet stable under sustained load because of
watcher-side history timing on `Jungle4`.

## Scope

Validated path:

- PostgreSQL logical `pgoutput`
- `denotary-db-agent`
- local deNotary off-chain services
- `verifbill::submit`
- inline `verif::billsubmit`
- receipt / audit proof export

The goal of this run was to confirm that the PostgreSQL runtime and the
off-chain verification path remain stable under ongoing write load on `Jungle4`.

## Stable Runtime Used

Fresh single-mode source:

- `source_id = pg-jungle4-owner-rerun2-1776510325`
- `slot_name = denotary_jungle4_owner_rerun2_1776510325_slot`
- `publication_name = denotary_jungle4_owner_rerun2_1776510325_pub`

Signing model:

- `broadcast_backend = "private_key_env"`
- `submitter = "newtestactio"`
- `submitter_permission = "owner"`

Jungle4 contract routing:

- `verif = decentrfstor`
- `verifbill = vadim1111111`

Policy configuration for this run:

- `schema_id = 1776342316`
- `policy_id = 1776343316`
- `batch_enabled = false`

## Why This Run Matters

This rerun validated two important conditions together:

1. testnet signing can use the same `private_key_env` model as mainnet
2. the updated watcher behavior no longer produces the previous false-negative
   `indexed transaction does not match request anchor` failures during the
   `/included` phase

That makes this run the current stable Jungle4 single-event baseline.

## Preflight

`doctor --strict` result:

- overall severity: `healthy`
- signer: `broadcast_ready = true`
- `private_key_matches_permission = true`
- source connectivity: `healthy`
- off-chain services reachable on:
  - `127.0.0.1:18080`
  - `127.0.0.1:18081`
  - `127.0.0.1:18082`
  - `127.0.0.1:18083`

Expected warning profile:

- using `owner` on Jungle4 is acceptable for this validation path
- `doctor` may still mark `owner` as not recommended for production, but it is
  not a blocker for the testnet soak baseline

`bootstrap` result:

- logical slot created and present
- publication created and in sync
- tracked tables:
  - `public.invoices`
  - `public.payments`

## Controlled First Run

Controlled `run --once` result:

- `processed = 2`
- `failed = 0`

Observed after the first stabilization window:

- `proof_count = 2`
- `dlq_count = 0`

This confirmed that the single-event Jungle4 path was clean before leaving the
runtime in short soak mode.

## Stable Short Soak Result

After the fresh rerun and short sustained load window:

- `delivery_count = 70`
- `proof_count = 69`
- `dlq_count = 0`
- source severity remained `healthy`

Stream runtime stayed stable throughout the short soak:

- `effective_runtime_mode = "stream"`
- `stream_reconnect_count = 0`
- `stream_failure_streak = 0`
- `stream_backoff_active = false`
- `stream_fallback_active = false`
- `stream_probation_active = false`

This means the PostgreSQL runtime itself stayed calm, and the off-chain
delivery/finality path no longer produced the earlier Jungle4 indexing failure
pattern during this validation window.

## On-Chain / Proof Evidence

Local rollout evidence snapshot for the stable rerun:

- [report-pg-jungle4-owner-rerun2-1776510325-20260418T090958Z.json](</c:/projects/denotary-db-agent/data/jungle4-owner-rerun2-1776510325/diagnostics/report-pg-jungle4-owner-rerun2-1776510325-20260418T090958Z.json>)

Runtime config used for the stable rerun:

- [agent.1776510325.json](</c:/projects/denotary-db-agent/data/jungle4-owner-rerun2-1776510325/agent.1776510325.json>)

Example proof export directory:

- [pg-jungle4-owner-rerun2-1776510325](</c:/projects/denotary-db-agent/data/jungle4-owner-rerun2-1776510325/proofs/pg-jungle4-owner-rerun2-1776510325>)

## Current Conclusion

The current stable Jungle4 short soak validates that:

- PostgreSQL logical `pgoutput` single-event flow is stable under ongoing write
  load
- the agent continues producing finalized proofs on Jungle4
- the watcher-side inclusion timing issue that previously caused false DLQ
  growth is no longer reproduced in this short validation window
- the signer model is aligned with mainnet:
  - `broadcast_backend = "private_key_env"`

## Remaining Caveats

This report is intentionally about the stable single-event baseline.

It does **not** yet promote the following as stable Jungle4 outcomes:

- batch soak
- long `12-24h` soak
- production-style dedicated hot permission on testnet

Those should be tracked separately once the single-event baseline is extended to
longer runtime windows.
