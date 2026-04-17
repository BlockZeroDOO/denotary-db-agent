#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="${project_root}/deploy/postgres-live/docker-compose.yml"

docker compose -f "${compose_file}" up -d
trap 'docker compose -f "${compose_file}" down -v' EXIT

python -m unittest -v tests.test_postgres_live_integration

