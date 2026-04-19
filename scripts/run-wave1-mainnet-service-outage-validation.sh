#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
python "$PROJECT_ROOT/scripts/run-wave1-mainnet-service-outage-validation.py" "$@"
