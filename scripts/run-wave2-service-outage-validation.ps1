param(
    [ValidateSet("sqlite", "redis", "all")]
    [string]$Adapter = "all"
)

$ErrorActionPreference = "Stop"

python scripts/run-wave2-service-outage-validation.py --adapter $Adapter
