#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="$project_root/deploy/db2-live/docker-compose.yml"

docker compose -f "$compose_file" up -d
trap 'docker compose -f "$compose_file" down -v' EXIT

export DENOTARY_DB2_HOST="127.0.0.1"
export DENOTARY_DB2_PORT="55000"
export DENOTARY_DB2_USERNAME="db2inst1"
export DENOTARY_DB2_PASSWORD="password"
export DENOTARY_DB2_DATABASE="DENOTARY"
export DENOTARY_DB2_SCHEMA="DB2INST1"

ready=0
for _ in $(seq 1 90); do
  if python - <<'PY' >/dev/null 2>&1
from tests.db2_live_support import create_connection
connection = create_connection()
connection.close()
PY
  then
    ready=1
    break
  fi
  sleep 10
done

if [[ "$ready" -ne 1 ]]; then
  echo "Db2 live container did not become ready in time." >&2
  exit 1
fi

python -m unittest discover -s tests -p 'test_db2*_integration.py' -v
