param()

$projectRoot = Split-Path -Parent $PSScriptRoot

docker compose -f (Join-Path $projectRoot "deploy\\mariadb-live\\docker-compose.yml") up -d
if ($LASTEXITCODE -ne 0) {
  exit $LASTEXITCODE
}

try {
  python -m unittest discover -s tests -p test_mariadb_live_integration.py -v
  exit $LASTEXITCODE
}
finally {
  docker compose -f (Join-Path $projectRoot "deploy\\mariadb-live\\docker-compose.yml") down -v
}
