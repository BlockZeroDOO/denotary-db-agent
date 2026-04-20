param()

$projectRoot = Split-Path -Parent $PSScriptRoot

C:\Python39\python.exe -m unittest discover -s tests -p test_elasticsearch*_integration.py -v
exit $LASTEXITCODE
