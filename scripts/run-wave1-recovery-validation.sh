#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
cd "$PROJECT_ROOT"

patterns=(
  "test_mysql_live_integration.py"
  "test_mariadb_live_integration.py"
  "test_sqlserver_live_integration.py"
  "test_oracle_live_integration.py"
  "test_mongodb_live_integration.py"
)

for pattern in "${patterns[@]}"; do
  python -m unittest discover -s tests -p "$pattern" -v
done
