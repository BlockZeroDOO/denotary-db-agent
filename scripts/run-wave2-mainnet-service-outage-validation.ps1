param(
    [ValidateSet("sqlite", "redis", "scylladb", "all")]
    [string]$Adapter = "all",
    [ValidateSet("ingress", "watcher", "receipt", "audit")]
    [string]$Scenario = "",
    [string]$OutputRoot = ""
)

$pythonExe = "C:\Python39\python.exe"
$scriptPath = Join-Path $PSScriptRoot "run-wave2-mainnet-service-outage-validation.py"

$arguments = @($scriptPath, "--adapter", $Adapter)
if ($Scenario -ne "") {
    $arguments += @("--scenario", $Scenario)
}
if ($OutputRoot -ne "") {
    $arguments += @("--output-root", $OutputRoot)
}

& $pythonExe @arguments
exit $LASTEXITCODE
