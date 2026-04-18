#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

docker compose -f "$project_root/deploy/mongodb-live/docker-compose.yml" up -d
trap 'docker compose -f "$project_root/deploy/mongodb-live/docker-compose.yml" down -v' EXIT

python -m unittest discover -s "$project_root/tests" -p 'test_mongodb*_integration.py' -v
