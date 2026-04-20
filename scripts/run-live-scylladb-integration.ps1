param()

$projectRoot = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $projectRoot "deploy\\scylladb-live\\docker-compose.yml"

docker compose -f $composeFile up -d
if ($LASTEXITCODE -ne 0) {
  exit $LASTEXITCODE
}

try {
  $ready = $false
  for ($i = 0; $i -lt 60; $i++) {
    docker exec denotary-db-agent-scylladb-live cqlsh -e "describe keyspaces" *> $null
    if ($LASTEXITCODE -eq 0) {
      $ready = $true
      break
    }
    Start-Sleep -Seconds 5
  }
  if (-not $ready) {
    throw "ScyllaDB live container did not become ready in time."
  }

  docker exec denotary-db-agent-scylladb-live cqlsh -e "create keyspace if not exists denotary_agent with replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
  if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
  }

  $env:DENOTARY_SCYLLADB_HOST = "127.0.0.1"
  $env:DENOTARY_SCYLLADB_PORT = "59043"
  $env:DENOTARY_SCYLLADB_KEYSPACE = "denotary_agent"
  $env:DENOTARY_SCYLLADB_USERNAME = ""
  $env:DENOTARY_SCYLLADB_PASSWORD = ""

  C:\Python39\python.exe -m unittest discover -s tests -p test_scylladb*_integration.py -v
  exit $LASTEXITCODE
}
finally {
  docker compose -f $composeFile down -v
}
