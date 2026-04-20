param()

$projectRoot = Split-Path -Parent $PSScriptRoot

C:\Python39\python.exe -m unittest discover -s tests -p test_cassandra*_integration.py -v
exit $LASTEXITCODE
