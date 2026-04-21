param()

$projectRoot = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $projectRoot "deploy\\elasticsearch-live\\docker-compose.yml"

docker compose -f $composeFile up -d
if ($LASTEXITCODE -ne 0) {
  exit $LASTEXITCODE
}

$env:DENOTARY_ELASTICSEARCH_URL = "http://127.0.0.1:59200"
$env:DENOTARY_ELASTICSEARCH_USERNAME = ""
$env:DENOTARY_ELASTICSEARCH_PASSWORD = ""
$env:DENOTARY_ELASTICSEARCH_VERIFY_CERTS = "false"

try {
  C:\Python39\python.exe -m unittest discover -s tests -p test_elasticsearch*_integration.py -v
  exit $LASTEXITCODE
}
finally {
  docker compose -f $composeFile down -v
}
