#!/usr/bin/env bash
set -euo pipefail

mode="${1:?mode is required}"
cycles="${2:-5}"
events_per_cycle="${3:-3}"

python scripts/run-wave2-elasticsearch-validation.py --mode "$mode" --cycles "$cycles" --events-per-cycle "$events_per_cycle"
