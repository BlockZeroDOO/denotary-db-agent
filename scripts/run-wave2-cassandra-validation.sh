#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-restart}"
CYCLES="${2:-5}"
EVENTS_PER_CYCLE="${3:-3}"

ARGS=( "scripts/run-wave2-cassandra-validation.py" "--mode" "$MODE" )
if [[ "$MODE" == "short-soak" ]]; then
  ARGS+=( "--cycles" "$CYCLES" "--events-per-cycle" "$EVENTS_PER_CYCLE" )
fi

python "${ARGS[@]}"
