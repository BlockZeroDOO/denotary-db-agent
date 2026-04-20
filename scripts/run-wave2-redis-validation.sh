#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

mode="${1:-restart}"
cycles="${2:-5}"
events_per_cycle="${3:-3}"

python "$project_root/scripts/run-wave2-redis-validation.py" --mode "$mode" --cycles "$cycles" --events-per-cycle "$events_per_cycle"
