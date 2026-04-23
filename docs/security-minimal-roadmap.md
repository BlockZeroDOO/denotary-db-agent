# Minimal Security Roadmap

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

This roadmap defines the minimum security-hardening pass for
`denotary-db-agent`.

The goal is to improve operator safety and deployment correctness without
starting the larger hardening phase yet.

## Scope

This minimum phase focuses on:

- explaining the trust boundary clearly in documentation
- making unsafe deployment choices more visible to operators
- adding a small number of fail-closed runtime checks
- preserving the current architecture and delivery model

This phase does not yet include:

- encrypted local state
- mTLS or certificate pinning
- HSM or KMS-backed signing
- a full redesign of reconciliation storage
- deeper multi-tenant or host-hardening controls

## P0 Documentation Baseline

1. Add a dedicated security baseline document.
2. Explain why `Ingress`, `Watcher`, `Receipt`, and `Audit` should run in the
   same trusted environment as the agent.
3. Document that `owner` and `active` are not acceptable runtime permissions
   for enterprise production use.
4. Mark `state_db`, `proof_dir`, evidence snapshots, and local secret files as
   sensitive operator artifacts.
5. Link the security baseline from `README`, `architecture`, and operator
   runbooks.

## P1 Operator and Preflight Guardrails

1. Raise `owner` / `active` runtime usage from a soft recommendation to a
   policy violation in strict preflight.
2. Treat inline hot-key configuration as a non-recommended fallback and make
   that visible in `doctor`.
3. Make the trusted-backend expectation explicit in `doctor` and rollout docs.
4. Keep `doctor --strict` as the required pre-start gate for production
   rollouts and key rotation.

## P2 Minimal Runtime Hardening

1. Do not allow the agent to advance checkpoints through a live notarization
   path when the signer/broadcast backend is not ready.
2. Validate `prepared_action` against the locally expected payload before
   signing.
3. Sanitize proof export paths derived from `source_id` and `request_id`.

## P3 Local Artifact Handling

1. Treat local state and evidence files as confidential operational data.
2. Add or document filesystem permission expectations for:
   - `storage.state_db`
   - `storage.proof_dir`
   - evidence snapshot directories
   - `env_file`
3. Keep proof and state paths in controlled service-owned directories.

## P4 Minimal Security Validation

1. Add unit coverage for strict rejection of `owner` / `active`.
2. Add tests for rejecting mismatched `prepared_action` payloads.
3. Add tests for safe proof-export path handling.
4. Add tests that confirm a missing live signer prevents checkpoint advance.

## Exit Criteria

The minimum security phase is complete when:

- the documentation explains the trust boundary in plain operator language
- enterprise operators are clearly told not to use `owner` or `active`
- production-like examples use `dnanchor` and `private_key_env`
- strict preflight blocks obviously unsafe signer setups
- runtime does not silently progress through a live flow without a ready
  broadcaster
- proof export paths are constrained to the configured proof directory

