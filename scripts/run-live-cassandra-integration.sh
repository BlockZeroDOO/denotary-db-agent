#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="$project_root/deploy/cassandra-live/docker-compose.yml"

docker compose -f "$compose_file" up -d
trap 'docker compose -f "$compose_file" down -v' EXIT

ready=0
for _ in $(seq 1 60); do
  if docker exec denotary-db-agent-cassandra-live cqlsh -e "describe keyspaces" >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 5
done

if [[ "$ready" -ne 1 ]]; then
  echo "Cassandra live container did not become ready in time." >&2
  exit 1
fi

docker exec denotary-db-agent-cassandra-live cqlsh -e "create keyspace if not exists denotary_agent with replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"

export DENOTARY_CASSANDRA_HOST="127.0.0.1"
export DENOTARY_CASSANDRA_PORT="59042"
export DENOTARY_CASSANDRA_KEYSPACE="denotary_agent"
export DENOTARY_CASSANDRA_USERNAME=""
export DENOTARY_CASSANDRA_PASSWORD=""

python -m unittest discover -s tests -p 'test_cassandra*_integration.py' -v
