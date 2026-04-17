param(
    [Parameter(Mandatory = $true)]
    [string]$Account,

    [Parameter(Mandatory = $true)]
    [string]$PublicKey,

    [string]$PermissionName = "dnanchor",
    [string]$RpcUrl = "https://history.denotary.io",
    [string]$BillingAccount = "verifbill"
)

Write-Output "cleos -u $RpcUrl push action eosio updateauth '["
Write-Output "  `"$Account`","
Write-Output "  `"$PermissionName`","
Write-Output "  `"active`","
Write-Output "  {"
Write-Output "    `"threshold`": 1,"
Write-Output "    `"keys`": ["
Write-Output "      {"
Write-Output "        `"key`": `"$PublicKey`","
Write-Output "        `"weight`": 1"
Write-Output "      }"
Write-Output "    ],"
Write-Output "    `"accounts`": [],"
Write-Output "    `"waits`": []"
Write-Output "  }"
Write-Output "]' -p ${Account}@active"
Write-Output ""
Write-Output "cleos -u $RpcUrl push action eosio linkauth '[`"$Account`",`"$BillingAccount`",`"submit`",`"$PermissionName`"]' -p ${Account}@active"
Write-Output "cleos -u $RpcUrl push action eosio linkauth '[`"$Account`",`"$BillingAccount`",`"submitroot`",`"$PermissionName`"]' -p ${Account}@active"

