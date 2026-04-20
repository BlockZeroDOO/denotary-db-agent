from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = PROJECT_ROOT / "data"
PYTHON_EXE = Path(sys.executable)

ADAPTERS: dict[str, dict[str, Any]] = {
    "snowflake": {
        "pattern": "test_snowflake*_integration.py",
        "required_env": [
            "DENOTARY_SNOWFLAKE_ACCOUNT",
            "DENOTARY_SNOWFLAKE_USERNAME",
            "DENOTARY_SNOWFLAKE_PASSWORD",
            "DENOTARY_SNOWFLAKE_DATABASE",
            "DENOTARY_SNOWFLAKE_SCHEMA",
            "DENOTARY_SNOWFLAKE_WAREHOUSE",
        ],
    },
    "db2": {
        "pattern": "test_db2*_integration.py",
        "required_env": [
            "DENOTARY_DB2_HOST",
            "DENOTARY_DB2_PORT",
            "DENOTARY_DB2_USERNAME",
            "DENOTARY_DB2_PASSWORD",
            "DENOTARY_DB2_DATABASE",
            "DENOTARY_DB2_SCHEMA",
        ],
    },
    "cassandra": {
        "pattern": "test_cassandra*_integration.py",
        "required_env": [
            "DENOTARY_CASSANDRA_HOST",
            "DENOTARY_CASSANDRA_PORT",
            "DENOTARY_CASSANDRA_KEYSPACE",
        ],
    },
    "elasticsearch": {
        "pattern": "test_elasticsearch*_integration.py",
        "required_env": [
            "DENOTARY_ELASTICSEARCH_URL",
        ],
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run unified Wave 2 live validation suites.")
    parser.add_argument("--adapter", choices=tuple(list(ADAPTERS) + ["all"]), default="all")
    parser.add_argument("--output-root", default="", help="Optional directory for persistent summary output.")
    parser.add_argument(
        "--strict-env",
        action="store_true",
        help="Fail adapters with missing required environment instead of marking them skipped.",
    )
    return parser.parse_args()


def _selected_adapters(name: str) -> list[str]:
    return list(ADAPTERS) if name == "all" else [name]


def _missing_env(required_env: list[str]) -> list[str]:
    return [env_name for env_name in required_env if not os.environ.get(env_name, "").strip()]


def _run_suite(pattern: str) -> subprocess.CompletedProcess[str]:
    command = [
        str(PYTHON_EXE),
        "-m",
        "unittest",
        "discover",
        "-s",
        "tests",
        "-p",
        pattern,
        "-v",
    ]
    return subprocess.run(  # noqa: S603
        command,
        cwd=str(PROJECT_ROOT),
        text=True,
        capture_output=True,
        check=False,
    )


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def main() -> None:
    args = parse_args()
    run_root = (
        Path(args.output_root).resolve()
        if args.output_root
        else (DATA_ROOT / f"wave2-live-validation-{utc_stamp().lower()}").resolve()
    )
    run_root.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, Any]] = []
    overall_exit = 0
    for adapter in _selected_adapters(args.adapter):
        spec = ADAPTERS[adapter]
        missing_env = _missing_env(spec["required_env"])
        if missing_env and not args.strict_env:
            results.append(
                {
                    "adapter": adapter,
                    "status": "skipped",
                    "reason": "missing_env",
                    "missing_env": missing_env,
                    "pattern": spec["pattern"],
                }
            )
            continue

        process = _run_suite(spec["pattern"])
        status = "passed" if process.returncode == 0 else "failed"
        if process.returncode != 0:
            overall_exit = process.returncode
        results.append(
            {
                "adapter": adapter,
                "status": status,
                "missing_env": missing_env,
                "pattern": spec["pattern"],
                "returncode": process.returncode,
                "stdout": process.stdout,
                "stderr": process.stderr,
            }
        )

    summary = {
        "run_root": str(run_root),
        "strict_env": bool(args.strict_env),
        "results": results,
    }
    (run_root / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    raise SystemExit(overall_exit)


if __name__ == "__main__":
    main()
