param(
    [ValidateSet("sqlite", "redis", "all")]
    [string]$Adapter = "all"
)

$ErrorActionPreference = "Stop"

C:\Python39\python.exe scripts/run-wave2-jungle4-validation.py --adapter $Adapter
