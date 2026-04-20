#!/usr/bin/env bash
set -euo pipefail

ADAPTER="${1:-}"
OUTPUT="${2:-}"
FORCE="${3:-}"

ARGS=( "scripts/bootstrap-wave2-live-env.py" "--adapter" "$ADAPTER" "--output" "$OUTPUT" )
if [[ "$FORCE" == "--force" ]]; then
  ARGS+=( "--force" )
fi

python "${ARGS[@]}"
