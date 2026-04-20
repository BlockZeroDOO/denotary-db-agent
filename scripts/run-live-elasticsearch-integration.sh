#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

docker compose -f "$project_root/deploy/elasticsearch-live/docker-compose.yml" up -d
trap 'docker compose -f "$project_root/deploy/elasticsearch-live/docker-compose.yml" down -v' EXIT

export DENOTARY_ELASTICSEARCH_URL="http://127.0.0.1:59200"
export DENOTARY_ELASTICSEARCH_USERNAME=""
export DENOTARY_ELASTICSEARCH_PASSWORD=""
export DENOTARY_ELASTICSEARCH_VERIFY_CERTS="false"

python -m unittest discover -s tests -p 'test_elasticsearch*_integration.py' -v
