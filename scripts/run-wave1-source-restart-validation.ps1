param(
  [string]$Adapter = ""
)

$projectRoot = Split-Path -Parent $PSScriptRoot
$command = @((Join-Path $projectRoot "scripts\\run-wave1-source-restart-validation.py"))
if ($Adapter) {
  $command += "--adapter"
  $command += $Adapter
}
python @command
