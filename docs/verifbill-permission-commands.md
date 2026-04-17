# `verifbill` Enterprise Permission Commands

These commands set up a dedicated enterprise broadcaster permission without exposing the `owner` key in application infrastructure.

Example account:

- enterprise payer account: `enterpriseac1`
- hot broadcaster permission: `dnanchor`
- target billing contract: `verifbill`

## 1. Generate a new broadcaster key

Run this on a secure operator machine with `cleos` available:

```bash
cleos create key --to-console
```

Store:

- the **private key** in your secure secret manager or HSM-backed workflow
- the **public key** for the permission setup below

## 2. Create the custom permission

Using the enterprise account `active` key:

```bash
cleos -u https://history.denotary.io push action eosio updateauth '[
  "enterpriseac1",
  "dnanchor",
  "active",
  {
    "threshold": 1,
    "keys": [
      {
        "key": "PUB_K1_REPLACE_ME",
        "weight": 1
      }
    ],
    "accounts": [],
    "waits": []
  }
]' -p enterpriseac1@active
```

## 3. Link the custom permission only to notarization actions

```bash
cleos -u https://history.denotary.io push action eosio linkauth '[
  "enterpriseac1",
  "verifbill",
  "submit",
  "dnanchor"
]' -p enterpriseac1@active
```

```bash
cleos -u https://history.denotary.io push action eosio linkauth '[
  "enterpriseac1",
  "verifbill",
  "submitroot",
  "dnanchor"
]' -p enterpriseac1@active
```

## 4. Verify

```bash
cleos -u https://history.denotary.io get account enterpriseac1
```

Check:

- permission `dnanchor` exists
- linked actions include:
  - `verifbill::submit`
  - `verifbill::submitroot`

## 5. Agent runtime target

When the DB Agent later signs on-chain transactions, it should use:

- `enterpriseac1@dnanchor`

not:

- `enterpriseac1@owner`
- `enterpriseac1@active`

## 6. Operational policy

- keep `owner` offline
- keep `active` off the application server
- do not link `dnanchor` to `eosio.token::transfer`
- buy plans and packs through a separate finance workflow
- periodically rotate the `dnanchor` key with `updateauth`

