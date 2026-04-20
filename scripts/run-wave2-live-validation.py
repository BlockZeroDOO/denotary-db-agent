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
    parser.add_argument("--env-file", default="", help="Optional dotenv-style file with adapter credentials.")
    parser.add_argument("--output-root", default="", help="Optional directory for persistent summary output.")
    parser.add_argument(
        "--list-required-env",
        action="store_true",
        help="Print required env variables for the selected adapters and exit.",
    )
    parser.add_argument(
        "--list-format",
        choices=("json", "text"),
        default="json",
        help="Output format for --list-required-env.",
    )
    parser.add_argument(
        "--check-env-only",
        action="store_true",
        help="Only validate environment readiness; do not execute live test suites.",
    )
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


def _load_env_file(env_file: str) -> dict[str, str]:
    if not env_file:
        return {}
    payload: dict[str, str] = {}
    path = Path(env_file).resolve()
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip().lstrip("\ufeff")
        value = value.strip()
        if not key:
            continue
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        payload[key] = value
    return payload


def _missing_env_from(required_env: list[str], env_map: dict[str, str]) -> list[str]:
    return [env_name for env_name in required_env if not env_map.get(env_name, "").strip()]


def _run_suite(pattern: str, env_map: dict[str, str]) -> subprocess.CompletedProcess[str]:
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
        env=env_map,
        check=False,
    )


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _required_env_payload(adapters: list[str]) -> dict[str, Any]:
    return {
        "adapters": [
            {
                "adapter": adapter,
                "required_env": list(ADAPTERS[adapter]["required_env"]),
                "pattern": ADAPTERS[adapter]["pattern"],
            }
            for adapter in adapters
        ]
    }


def _print_required_env(payload: dict[str, Any], output_format: str) -> None:
    if output_format == "text":
        for item in payload["adapters"]:
            print(f"{item['adapter']}:")
            for env_name in item["required_env"]:
                print(f"  {env_name}")
        return
    print(json.dumps(payload, indent=2))


def main() -> None:
    args = parse_args()
    env_file_values = _load_env_file(args.env_file)
    effective_env = dict(os.environ)
    effective_env.update(env_file_values)
    selected_adapters = _selected_adapters(args.adapter)
    if args.list_required_env:
        _print_required_env(_required_env_payload(selected_adapters), args.list_format)
        raise SystemExit(0)
    run_root = (
        Path(args.output_root).resolve()
        if args.output_root
        else (DATA_ROOT / f"wave2-live-validation-{utc_stamp().lower()}").resolve()
    )
    run_root.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, Any]] = []
    overall_exit = 0
    for adapter in selected_adapters:
        spec = ADAPTERS[adapter]
        missing_env = _missing_env_from(spec["required_env"], effective_env)
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

        if args.check_env_only:
            status = "ready" if not missing_env else "failed"
            if missing_env:
                overall_exit = overall_exit or 1
            results.append(
                {
                    "adapter": adapter,
                    "status": status,
                    "missing_env": missing_env,
                    "pattern": spec["pattern"],
                    "checked_only": True,
                }
            )
            continue

        process = _run_suite(spec["pattern"], effective_env)
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
        "env_file": str(Path(args.env_file).resolve()) if args.env_file else "",
        "check_env_only": bool(args.check_env_only),
        "strict_env": bool(args.strict_env),
        "results": results,
    }
    (run_root / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    raise SystemExit(overall_exit)


if __name__ == "__main__":
    main()
