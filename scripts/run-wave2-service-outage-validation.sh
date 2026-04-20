#!/usr/bin/env bash
set -euo pipefail

adapter="${1:-all}"

python scripts/run-wave2-service-outage-validation.py --adapter "$adapter"
