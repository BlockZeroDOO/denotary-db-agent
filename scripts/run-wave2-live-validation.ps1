param(
    [ValidateSet("snowflake", "db2", "cassandra", "elasticsearch", "all")]
    [string]$Adapter = "all",
    [switch]$StrictEnv
)

$pythonExe = "C:\Python39\python.exe"
$scriptPath = Join-Path $PSScriptRoot "run-wave2-live-validation.py"

$arguments = @($scriptPath, "--adapter", $Adapter)
if ($StrictEnv) {
    $arguments += "--strict-env"
}

& $pythonExe @arguments
exit $LASTEXITCODE
