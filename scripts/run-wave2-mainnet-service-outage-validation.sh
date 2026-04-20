#!/usr/bin/env bash
set -euo pipefail

ADAPTER="${1:-all}"
SCENARIO="${2:-}"
OUTPUT_ROOT="${3:-}"

ARGS=( "scripts/run-wave2-mainnet-service-outage-validation.py" "--adapter" "$ADAPTER" )
if [[ -n "$SCENARIO" ]]; then
  ARGS+=( "--scenario" "$SCENARIO" )
fi
if [[ -n "$OUTPUT_ROOT" ]]; then
  ARGS+=( "--output-root" "$OUTPUT_ROOT" )
fi

python "${ARGS[@]}"
