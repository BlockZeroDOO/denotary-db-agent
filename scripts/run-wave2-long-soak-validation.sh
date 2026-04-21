#!/usr/bin/env bash
set -euo pipefail

ADAPTER="${1:-all}"
OUTPUT_ROOT="${OUTPUT_ROOT:-data/wave2-long-soak-validation-latest}"
CYCLES="${CYCLES:-10}"
EVENTS_PER_CYCLE="${EVENTS_PER_CYCLE:-5}"

python "$(dirname "$0")/run-wave2-long-soak-validation.py" \
  --adapter "$ADAPTER" \
  --cycles "$CYCLES" \
  --events-per-cycle "$EVENTS_PER_CYCLE" \
  --output-root "$OUTPUT_ROOT"
