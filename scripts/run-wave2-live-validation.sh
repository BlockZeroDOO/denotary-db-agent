#!/usr/bin/env bash
set -euo pipefail

ADAPTER="${1:-all}"
STRICT_ENV="${2:-}"
OUTPUT_ROOT="${3:-}"

ARGS=( "scripts/run-wave2-live-validation.py" "--adapter" "$ADAPTER" )
if [[ "$STRICT_ENV" == "--strict-env" ]]; then
  ARGS+=( "--strict-env" )
fi
if [[ -n "$OUTPUT_ROOT" ]]; then
  ARGS+=( "--output-root" "$OUTPUT_ROOT" )
fi

python "${ARGS[@]}"
