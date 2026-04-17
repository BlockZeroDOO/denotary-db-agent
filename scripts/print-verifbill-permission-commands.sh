#!/usr/bin/env bash
set -euo pipefail

account="${1:?usage: print-verifbill-permission-commands.sh <account> <public_key> [permission_name] [rpc_url] [billing_account]}"
public_key="${2:?usage: print-verifbill-permission-commands.sh <account> <public_key> [permission_name] [rpc_url] [billing_account]}"
permission_name="${3:-dnanchor}"
rpc_url="${4:-https://history.denotary.io}"
billing_account="${5:-verifbill}"

cat <<EOF
cleos -u ${rpc_url} push action eosio updateauth '[
  "${account}",
  "${permission_name}",
  "active",
  {
    "threshold": 1,
    "keys": [
      {
        "key": "${public_key}",
        "weight": 1
      }
    ],
    "accounts": [],
    "waits": []
  }
]' -p ${account}@active

cleos -u ${rpc_url} push action eosio linkauth '["${account}","${billing_account}","submit","${permission_name}"]' -p ${account}@active
cleos -u ${rpc_url} push action eosio linkauth '["${account}","${billing_account}","submitroot","${permission_name}"]' -p ${account}@active
EOF

