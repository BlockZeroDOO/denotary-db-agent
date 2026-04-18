#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

docker compose -f "$project_root/deploy/sqlserver-live/docker-compose.yml" up -d
trap 'docker compose -f "$project_root/deploy/sqlserver-live/docker-compose.yml" down -v' EXIT

python -m unittest discover -s tests -p 'test_sqlserver*_integration.py' -v
