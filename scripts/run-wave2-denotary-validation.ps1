param(
    [ValidateSet("all", "sqlite", "redis")]
    [string]$Adapter = "all",
    [string]$OutputRoot = ""
)

$projectRoot = Split-Path -Parent $PSScriptRoot
$python = "C:\Python39\python.exe"
$args = @((Join-Path $projectRoot "scripts\run-wave2-denotary-validation.py"), "--adapter", $Adapter)
if ($OutputRoot) {
    $args += @("--output-root", $OutputRoot)
}
& $python @args
