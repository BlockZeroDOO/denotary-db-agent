param(
    [ValidateSet("restart", "short-soak")]
    [string]$Mode = "restart",
    [int]$Cycles = 5,
    [int]$EventsPerCycle = 3
)

$pythonExe = "C:\Python39\python.exe"
$scriptPath = Join-Path $PSScriptRoot "run-wave2-cassandra-validation.py"

$arguments = @($scriptPath, "--mode", $Mode)
if ($Mode -eq "short-soak") {
    $arguments += @("--cycles", $Cycles, "--events-per-cycle", $EventsPerCycle)
}

& $pythonExe @arguments
exit $LASTEXITCODE
