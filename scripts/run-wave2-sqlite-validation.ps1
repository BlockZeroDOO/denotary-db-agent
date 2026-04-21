param(
  [ValidateSet("restart", "short-soak")]
  [string]$Mode = "restart",
  [int]$Cycles = 5,
  [int]$EventsPerCycle = 3
)

$projectRoot = Split-Path -Parent $PSScriptRoot

C:\Python39\python.exe (Join-Path $projectRoot "scripts\\run-wave2-sqlite-validation.py") --mode $Mode --cycles $Cycles --events-per-cycle $EventsPerCycle
exit $LASTEXITCODE
