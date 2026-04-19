from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SHORT_SOAK_SCRIPT = PROJECT_ROOT / "scripts" / "run-wave1-short-soak-validation.py"
DEFAULT_CYCLES = 10
DEFAULT_EVENTS_PER_CYCLE = 5
DEFAULT_MONGODB_SLEEP_AFTER_WRITE = 2.0


def _load_short_soak_module():
    spec = importlib.util.spec_from_file_location("wave1_short_soak_validation", SHORT_SOAK_SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError("could not load wave1 short soak validation helpers")
    module = importlib.util.module_from_spec(spec)
    sys.modules["wave1_short_soak_validation"] = module
    spec.loader.exec_module(module)
    return module


SHORT_SOAK = _load_short_soak_module()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Wave 1 long-soak validation.")
    parser.add_argument("--cycles", type=int, default=DEFAULT_CYCLES)
    parser.add_argument("--events-per-cycle", type=int, default=DEFAULT_EVENTS_PER_CYCLE)
    parser.add_argument("--adapter", choices=["mysql", "mariadb", "sqlserver", "oracle", "mongodb"])
    parser.add_argument("--mongodb-sleep-after-write", type=float, default=DEFAULT_MONGODB_SLEEP_AFTER_WRITE)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    payload = SHORT_SOAK.run_validation(
        cycles=args.cycles,
        events_per_cycle=args.events_per_cycle,
        soak_name="long soak",
        adapter=args.adapter,
        mongodb_sleep_after_write=args.mongodb_sleep_after_write,
    )
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
