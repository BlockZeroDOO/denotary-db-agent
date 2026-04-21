#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")"/.. && pwd)"
compose_file="$project_root/deploy/scylladb-live/docker-compose.yml"

docker compose -f "$compose_file" up -d
cleanup() {
  docker compose -f "$compose_file" down -v
}
trap cleanup EXIT

ready=false
for _ in $(seq 1 60); do
  if docker exec denotary-db-agent-scylladb-live cqlsh -e "describe keyspaces" >/dev/null 2>&1; then
    ready=true
    break
  fi
  sleep 5
done

if [[ "$ready" != true ]]; then
  echo "ScyllaDB live container did not become ready in time." >&2
  exit 1
fi

docker exec denotary-db-agent-scylladb-live cqlsh -e "create keyspace if not exists denotary_agent with replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"

export DENOTARY_SCYLLADB_HOST=127.0.0.1
export DENOTARY_SCYLLADB_PORT=59043
export DENOTARY_SCYLLADB_KEYSPACE=denotary_agent
export DENOTARY_SCYLLADB_USERNAME=
export DENOTARY_SCYLLADB_PASSWORD=

python -m unittest discover -s tests -p 'test_scylladb*_integration.py' -v
