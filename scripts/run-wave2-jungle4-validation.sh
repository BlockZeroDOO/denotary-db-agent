#!/usr/bin/env bash
set -euo pipefail

adapter="${1:-all}"

python scripts/run-wave2-jungle4-validation.py --adapter "$adapter"
