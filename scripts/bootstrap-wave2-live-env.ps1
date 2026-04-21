param(
    [ValidateSet("db2", "cassandra", "elasticsearch")]
    [string]$Adapter,
    [string]$Output,
    [switch]$Force
)

$scriptPath = Join-Path $PSScriptRoot "bootstrap-wave2-live-env.py"
$arguments = @($scriptPath, "--adapter", $Adapter, "--output", $Output)
if ($Force) {
    $arguments += "--force"
}

& python @arguments
exit $LASTEXITCODE
