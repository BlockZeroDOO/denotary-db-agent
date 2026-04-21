param(
  [string]$Adapter = "all",
  [int]$Cycles = 10,
  [int]$EventsPerCycle = 5,
  [string]$OutputRoot = "data/wave2-long-soak-validation-latest"
)

C:\Python39\python.exe "$PSScriptRoot\run-wave2-long-soak-validation.py" `
  --adapter $Adapter `
  --cycles $Cycles `
  --events-per-cycle $EventsPerCycle `
  --output-root $OutputRoot

exit $LASTEXITCODE
