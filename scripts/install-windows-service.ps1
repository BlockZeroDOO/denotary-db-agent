param(
    [Parameter(Mandatory = $true)]
    [string]$ConfigPath,

    [string]$PythonExe = "C:\Python39\python.exe",
    [string]$ServiceName = "deNotaryDbAgent",
    [string]$DisplayName = "deNotary DB Agent",
    [string]$Description = "deNotary DB Agent enterprise database notarization service",
    [double]$IntervalSec = 5,
    [string]$DoctorSource = ""
)

$projectRoot = Split-Path -Parent $PSScriptRoot
$runnerScript = Join-Path $projectRoot "scripts\run-windows-service.ps1"
$quotedPython = '"' + $PythonExe + '"'
$quotedRunner = '"' + $runnerScript + '"'
$quotedConfig = '"' + $ConfigPath + '"'
$binPath = "$quotedPython -ExecutionPolicy Bypass -File $quotedRunner -ConfigPath $quotedConfig -IntervalSec $IntervalSec"
if ($DoctorSource) {
    $quotedDoctorSource = '"' + $DoctorSource + '"'
    $binPath = "$binPath -DoctorSource $quotedDoctorSource"
}

if (Get-Service -Name $ServiceName -ErrorAction SilentlyContinue) {
    Write-Host "Service $ServiceName already exists; updating binPath via sc.exe config"
    sc.exe config $ServiceName binPath= $binPath | Out-Null
} else {
    New-Service -Name $ServiceName -BinaryPathName $binPath -DisplayName $DisplayName -Description $Description -StartupType Automatic
}

Write-Host "Configured service $ServiceName"
Write-Host "Pre-start gate: doctor --strict"
Write-Host "Start with: Start-Service $ServiceName"
