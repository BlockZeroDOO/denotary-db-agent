param(
    [Parameter(Mandatory = $true)]
    [string]$ConfigPath,

    [string]$PythonExe = "C:\Python39\python.exe",
    [double]$IntervalSec = 5,
    [string]$DoctorSource = ""
)

$projectRoot = Split-Path -Parent $PSScriptRoot
$mainModule = Join-Path $projectRoot "denotary_db_agent\__main__.py"

$doctorArgs = @($mainModule, "--config", $ConfigPath, "doctor", "--strict")
if ($DoctorSource) {
    $doctorArgs += @("--source", $DoctorSource)
}

& $PythonExe @doctorArgs
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

& $PythonExe $mainModule "--config" $ConfigPath "run" "--interval-sec" $IntervalSec
exit $LASTEXITCODE
