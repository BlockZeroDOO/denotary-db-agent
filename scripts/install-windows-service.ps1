param(
    [Parameter(Mandatory = $true)]
    [string]$ConfigPath,

    [string]$PythonExe = "C:\Python39\python.exe",
    [string]$ServiceName = "deNotaryDbAgent",
    [string]$DisplayName = "deNotary DB Agent",
    [string]$Description = "deNotary DB Agent enterprise database notarization service",
    [double]$IntervalSec = 5
)

$projectRoot = Split-Path -Parent $PSScriptRoot
$mainModule = Join-Path $projectRoot "denotary_db_agent\__main__.py"
$quotedPython = '"' + $PythonExe + '"'
$quotedMain = '"' + $mainModule + '"'
$quotedConfig = '"' + $ConfigPath + '"'
$binPath = "$quotedPython $quotedMain --config $quotedConfig run --interval-sec $IntervalSec"

if (Get-Service -Name $ServiceName -ErrorAction SilentlyContinue) {
    Write-Host "Service $ServiceName already exists; updating binPath via sc.exe config"
    sc.exe config $ServiceName binPath= $binPath | Out-Null
} else {
    New-Service -Name $ServiceName -BinaryPathName $binPath -DisplayName $DisplayName -Description $Description -StartupType Automatic
}

Write-Host "Configured service $ServiceName"
Write-Host "Start with: Start-Service $ServiceName"
