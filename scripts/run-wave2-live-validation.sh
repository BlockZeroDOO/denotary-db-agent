#!/usr/bin/env bash
set -euo pipefail

ADAPTER="${1:-all}"
ENV_FILE="${2:-}"
MODE="${3:-}"
OUTPUT_ROOT="${4:-}"
LIST_FORMAT="${5:-json}"

ARGS=( "scripts/run-wave2-live-validation.py" "--adapter" "$ADAPTER" )
if [[ -n "$ENV_FILE" ]]; then
  ARGS+=( "--env-file" "$ENV_FILE" )
fi
if [[ "$MODE" == "--list-required-env" ]]; then
  ARGS+=( "--list-required-env" "--list-format" "$LIST_FORMAT" )
fi
if [[ "$MODE" == "--check-env-only" ]]; then
  ARGS+=( "--check-env-only" )
fi
if [[ "$MODE" == "--strict-env" ]]; then
  ARGS+=( "--strict-env" )
fi
if [[ -n "$OUTPUT_ROOT" ]]; then
  ARGS+=( "--output-root" "$OUTPUT_ROOT" )
fi

python "${ARGS[@]}"
