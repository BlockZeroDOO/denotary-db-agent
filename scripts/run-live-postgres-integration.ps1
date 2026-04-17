param()

$projectRoot = Split-Path -Parent $PSScriptRoot

docker compose -f (Join-Path $projectRoot "deploy\\postgres-live\\docker-compose.yml") up -d
if ($LASTEXITCODE -ne 0) {
  exit $LASTEXITCODE
}

try {
  python -m unittest -v tests.test_postgres_live_integration
  exit $LASTEXITCODE
}
finally {
  docker compose -f (Join-Path $projectRoot "deploy\\postgres-live\\docker-compose.yml") down -v
}

