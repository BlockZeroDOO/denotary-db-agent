param(
    [ValidateSet("db2", "cassandra", "elasticsearch", "all")]
    [string]$Adapter = "all",
    [string]$EnvFile = "",
    [switch]$ListRequiredEnv,
    [ValidateSet("json", "text")]
    [string]$ListFormat = "json",
    [string]$WriteEnvTemplate = "",
    [switch]$CheckEnvOnly,
    [switch]$StrictEnv,
    [string]$OutputRoot = ""
)

$pythonExe = "C:\Python39\python.exe"
$scriptPath = Join-Path $PSScriptRoot "run-wave2-live-validation.py"

$arguments = @($scriptPath, "--adapter", $Adapter)
if ($EnvFile -ne "") {
    $arguments += @("--env-file", $EnvFile)
}
if ($ListRequiredEnv) {
    $arguments += "--list-required-env"
    $arguments += @("--list-format", $ListFormat)
}
if ($WriteEnvTemplate -ne "") {
    $arguments += @("--write-env-template", $WriteEnvTemplate)
}
if ($CheckEnvOnly) {
    $arguments += "--check-env-only"
}
if ($StrictEnv) {
    $arguments += "--strict-env"
}
if ($OutputRoot -ne "") {
    $arguments += @("--output-root", $OutputRoot)
}

& $pythonExe @arguments
exit $LASTEXITCODE
