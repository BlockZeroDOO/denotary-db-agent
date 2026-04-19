$projectRoot = Split-Path -Parent $PSScriptRoot
Push-Location $projectRoot
try {
    & "C:\Python39\python.exe" (Join-Path $projectRoot "scripts\run-wave1-mainnet-service-outage-validation.py") @args
}
finally {
    Pop-Location
}
