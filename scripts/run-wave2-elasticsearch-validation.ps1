param(
    [ValidateSet("restart", "short-soak")]
    [string]$Mode,
    [int]$Cycles = 5,
    [int]$EventsPerCycle = 3
)

$ErrorActionPreference = "Stop"

python scripts/run-wave2-elasticsearch-validation.py --mode $Mode --cycles $Cycles --events-per-cycle $EventsPerCycle
