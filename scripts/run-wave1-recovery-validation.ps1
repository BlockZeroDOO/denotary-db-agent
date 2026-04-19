param()

$projectRoot = Split-Path -Parent $PSScriptRoot

$patterns = @(
  "test_mysql_live_integration.py",
  "test_mariadb_live_integration.py",
  "test_sqlserver_live_integration.py",
  "test_oracle_live_integration.py",
  "test_mongodb_live_integration.py"
)

foreach ($pattern in $patterns) {
  python -m unittest discover -s tests -p $pattern -v
  if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
  }
}
