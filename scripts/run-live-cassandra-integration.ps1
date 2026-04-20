param()

$projectRoot = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $projectRoot "deploy\\cassandra-live\\docker-compose.yml"

docker compose -f $composeFile up -d
if ($LASTEXITCODE -ne 0) {
  exit $LASTEXITCODE
}

try {
  $ready = $false
  for ($i = 0; $i -lt 60; $i++) {
    docker exec denotary-db-agent-cassandra-live cqlsh -e "describe keyspaces" *> $null
    if ($LASTEXITCODE -eq 0) {
      $ready = $true
      break
    }
    Start-Sleep -Seconds 5
  }
  if (-not $ready) {
    throw "Cassandra live container did not become ready in time."
  }

  docker exec denotary-db-agent-cassandra-live cqlsh -e "create keyspace if not exists denotary_agent with replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
  if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
  }

  $env:DENOTARY_CASSANDRA_HOST = "127.0.0.1"
  $env:DENOTARY_CASSANDRA_PORT = "59042"
  $env:DENOTARY_CASSANDRA_KEYSPACE = "denotary_agent"
  $env:DENOTARY_CASSANDRA_USERNAME = ""
  $env:DENOTARY_CASSANDRA_PASSWORD = ""

  C:\Python39\python.exe -m unittest discover -s tests -p test_cassandra*_integration.py -v
  exit $LASTEXITCODE
}
finally {
  docker compose -f $composeFile down -v
}
