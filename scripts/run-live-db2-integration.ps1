param()

$projectRoot = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $projectRoot "deploy\\db2-live\\docker-compose.yml"

docker compose -f $composeFile up -d
if ($LASTEXITCODE -ne 0) {
  exit $LASTEXITCODE
}

try {
  $env:DENOTARY_DB2_HOST = "127.0.0.1"
  $env:DENOTARY_DB2_PORT = "55000"
  $env:DENOTARY_DB2_USERNAME = "db2inst1"
  $env:DENOTARY_DB2_PASSWORD = "password"
  $env:DENOTARY_DB2_DATABASE = "DENOTARY"
  $env:DENOTARY_DB2_SCHEMA = "DB2INST1"

  $ready = $false
  for ($i = 0; $i -lt 90; $i++) {
    @'
from tests.db2_live_support import create_connection
connection = create_connection()
connection.close()
'@ | C:\Python39\python.exe - *> $null
    if ($LASTEXITCODE -eq 0) {
      $ready = $true
      break
    }
    Start-Sleep -Seconds 10
  }
  if (-not $ready) {
    throw "Db2 live container did not become ready in time."
  }

  C:\Python39\python.exe -m unittest discover -s tests -p test_db2*_integration.py -v
  exit $LASTEXITCODE
}
finally {
  docker compose -f $composeFile down -v
}
