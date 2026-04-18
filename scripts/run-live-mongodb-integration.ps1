param()

$projectRoot = Split-Path -Parent $PSScriptRoot

docker compose -f (Join-Path $projectRoot "deploy\\mongodb-live\\docker-compose.yml") up -d
if ($LASTEXITCODE -ne 0) {
  exit $LASTEXITCODE
}

try {
  C:\Python39\python.exe -m unittest discover -s tests -p test_mongodb*_integration.py -v
  exit $LASTEXITCODE
}
finally {
  docker compose -f (Join-Path $projectRoot "deploy\\mongodb-live\\docker-compose.yml") down -v
}
