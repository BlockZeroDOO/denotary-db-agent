param(
    [ValidateSet("snowflake", "db2", "cassandra", "elasticsearch", "all")]
    [string]$Adapter = "all",
    [string]$EnvFile = "",
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
