#!/usr/bin/env bash
set -euo pipefail

ADAPTER="${1:-all}"
STRICT_ENV="${2:-}"

ARGS=( "scripts/run-wave2-live-validation.py" "--adapter" "$ADAPTER" )
if [[ "$STRICT_ENV" == "--strict-env" ]]; then
  ARGS+=( "--strict-env" )
fi

python "${ARGS[@]}"
