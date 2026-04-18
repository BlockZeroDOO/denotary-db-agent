# deNotary PostgreSQL Validation Report

This report captures the current live PostgreSQL validation status against `denotary` mainnet.

## Scope

Validated path:

- PostgreSQL source
- `denotary-db-agent`
- local deNotary off-chain services:
  - `Ingress API`
  - `Finality Watcher`
  - `Receipt Service`
  - `Audit API`
- `verifbill::submit`
- inline `verif::billsubmit`
- receipt and audit proof-chain export

Runtime signing model:

- `broadcast_backend = "private_key_env"`
- `submitter = "dbagentstest"`
- `submitter_permission = "dnanchor"`
- hot key loaded from local `env_file`

## Preconditions Used

Confirmed before live run:

- `dbagentstest@dnanchor` exists on chain
- `dnanchor` is linked to:
  - `verifbill::submit`
  - `verifbill::submitroot`
- `dbagentstest` has active `verifbill` entitlement
- local off-chain services are healthy on:
  - `127.0.0.1:8080`
  - `127.0.0.1:8081`
  - `127.0.0.1:8082`
  - `127.0.0.1:8083`
- PostgreSQL live harness is reachable on:
  - `127.0.0.1:55432`

## Doctor Result

Live `doctor` confirmed:

- `broadcast_backend = "private_key_env"`
- `effective_broadcast_backend = "private_key_env"`
- `private_key_source = "env"`
- `permission_exists = true`
- `billing_account_exists = true`
- `private_key_matches_permission = true`
- `broadcast_ready = true`
- source connectivity = `healthy`

Observed signer key match:

- `permission_public_keys` included:
  - `EOS7PKswjmSgDDA7iQKEqZu5t3vrniHhQmNCPrQfwxYeGdYiUiq2N`
- derived public key from the hot WIF matched the same on-chain key

## Fresh Validation Source

Fresh live validation used a clean source identity to avoid stale logical state:

- `source_id = pg-denotary-env-verify-1776477432`
- `slot_name = denotary_verify_1776477432_slot`
- `publication_name = denotary_verify_1776477432_pub`

This run used:

- `capture_mode = logical`
- `output_plugin = pgoutput`
- `logical_runtime_mode = stream`

## Live Event

Inserted PostgreSQL row:

- table: `public.invoices`
- `id = 1776477432`
- `status = "issued"`
- `amount = 654.32`

Agent result:

- `run --once`
- `processed = 1`
- `failed = 0`

## On-Chain Result

Request:

- `request_id = 6726d21c686bd8fe74b479381eade18251cf0ff24265e92e6cf96a46602d9f45`
- `trace_id = 53bc3c17-472b-4905-829d-c462aa33176f`

Transaction:

- `tx_id = 0092e621e205c14a5a57ebea2900bc784a4bec64277a3e6611293db559a0db7c`
- `block_num = 655844`

Verified chain actions:

- `verifbill::submit`
  - authorized by `dbagentstest@dnanchor`
- inline `verif::billsubmit`

Verified registry record:

- `verif.commitments.id = 9`
- `submitter = dbagentstest`
- `schema_id = 1`
- `policy_id = 1`
- `billable_bytes = 88`
- `billable_kib = 1`

## Receipt / Audit Result

Receipt state confirmed:

- `trust_state = finalized_verified`
- `receipt_available = true`
- `provider_disagreement = false`
- `verification_policy = single-provider`
- `verification_min_success = 1`

Audit proof chain confirmed stages:

- `request_registered`
- `transaction_verified`
- `transaction_included`
- `block_finalized`

## Exported Evidence

Proof bundle exported to:

- [6726d21c686bd8fe74b479381eade18251cf0ff24265e92e6cf96a46602d9f45.json](</c:/projects/denotary-db-agent/data/denotary-live-verify-1776477432/proofs/pg-denotary-env-verify-1776477432/6726d21c686bd8fe74b479381eade18251cf0ff24265e92e6cf96a46602d9f45.json>)

Local runtime state used for this run:

- [agent.1776477432.json](</c:/projects/denotary-db-agent/data/denotary-live-verify-1776477432/agent.1776477432.json>)
- [agent-state.sqlite3](</c:/projects/denotary-db-agent/data/denotary-live-verify-1776477432/agent-state.sqlite3>)

## Notable Hardening Confirmed

This validation also confirmed two important production protections:

- env-file signer preflight now verifies that the hot key matches `submitter@permission`
- delivery retry logic now reuses the original prepared request, preventing `trace_id does not match existing request` conflicts on retry

## Current Conclusion

PostgreSQL is validated live on `denotary` for the enterprise single-event path with:

- logical `pgoutput`
- stream runtime mode
- env-file-backed hot key signing
- `verifbill -> verif` anchoring
- receipt + audit proof-chain export

This is a valid production baseline for controlled rollout.
