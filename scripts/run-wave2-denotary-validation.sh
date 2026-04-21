#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ADAPTER="${1:-all}"
OUTPUT_ROOT="${2:-}"

ARGS=("$PROJECT_ROOT/scripts/run-wave2-denotary-validation.py" --adapter "$ADAPTER")
if [[ -n "$OUTPUT_ROOT" ]]; then
  ARGS+=(--output-root "$OUTPUT_ROOT")
fi

python "${ARGS[@]}"
