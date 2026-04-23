# deNotary Enterprise Permission Model for `verifbill`

[BlockZero DOO, Serbia https://blockzero.rs](https://blockzero.rs)
Telegram group: [DeNotaryGroup](https://t.me/DeNotaryGroup)

This document describes the recommended Antelope permission layout for enterprise broadcasting through `verifbill` without exposing the account `owner` key.

## Goal

Allow the DB Agent or another enterprise broadcaster to sign only:

- `verifbill::submit`
- `verifbill::submitroot`

while keeping:

- `owner` fully offline
- `active` out of the application server
- token transfers outside the hot broadcaster key

## Why `owner` and `active` Must Not Be Used

Enterprise DB Agent deployments should not run with:

- `submitter_permission = "owner"`
- `submitter_permission = "active"`

Why:

- `owner` is the recovery and ultimate-control permission
- `active` is usually broad enough to affect unrelated enterprise account
  operations
- compromise of the application host would then become compromise of the
  enterprise account, not only the notarization path
- a dedicated permission such as `dnanchor` keeps the runtime blast radius
  narrow and auditable

## Recommended Enterprise Account Layout

For an enterprise payer account such as `enterpriseac1`:

- `owner`
  - cold recovery key or multisig only
  - never imported into the DB Agent host
- `active`
  - cold operations key
  - used only to manage lower permissions
  - not stored in the DB Agent host
- `dnanchor`
  - dedicated hot key for the DB Agent
  - used only for:
    - `verifbill::submit`
    - `verifbill::submitroot`

## Why This Is Safer

`verifbill::submit` and `submitroot` require `require_auth(payer)`.

That means the enterprise account can authorize those actions with a custom permission instead of `active` or `owner`.

This reduces blast radius:

- the DB Agent key cannot change account permissions
- the DB Agent key should not be able to transfer tokens if you do not link it to `eosio.token::transfer`
- plan purchases and treasury operations stay separate from notarization signing

## Strong Recommendation

Do **not** give the DB Agent a permission linked to `eosio.token::transfer`.

Reason:

- `linkauth` on `eosio.token::transfer` cannot restrict the destination account
- a hot transfer key can move funds elsewhere, not only to `verifbill`

Recommended split:

- manual or finance-controlled plan/pack purchase
- automated notarization with prepaid entitlements

## Suggested Setup

Enterprise account:

- create a new permission: `dnanchor`
- parent: `active`
- threshold: `1`
- one dedicated public key

Link it only to:

- `verifbill::submit`
- `verifbill::submitroot`

## Optional Contract-Account Separation

For the `verifbill` contract account itself, a safer long-term model is:

- `owner`: cold
- `active`: cold + `eosio.code`
- `ops`: hot admin permission for pricing/config actions
- `treasury`: separate withdrawal permission

That is separate from the enterprise payer account used by the DB Agent.

## Agent Config

The DB Agent config includes:

- `submitter`
- `submitter_permission`

Recommended value:

- `submitter_permission = "dnanchor"`

This makes the intended runtime permission explicit even before full on-chain signing is implemented inside the agent.

For the broader deployment trust boundary, see:

- [security-baseline.md](security-baseline.md)
