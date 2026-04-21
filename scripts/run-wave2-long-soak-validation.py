from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import urlopen


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_ROOT = PROJECT_ROOT / "scripts"
PYTHON = sys.executable or "python"

DEFAULT_CYCLES = 10
DEFAULT_EVENTS_PER_CYCLE = 5
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "data" / "wave2-long-soak-validation-latest"


def _run_json(command: list[str], *, env: dict[str, str] | None = None) -> dict[str, Any]:
    result = subprocess.run(
        command,
        cwd=str(PROJECT_ROOT),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    stdout = result.stdout.strip()
    stderr = result.stderr.strip()
    if result.returncode != 0:
        raise RuntimeError(
            f"command failed with exit code {result.returncode}: {' '.join(command)}\nstdout:\n{stdout}\nstderr:\n{stderr}"
        )
    if not stdout:
        raise RuntimeError(f"command produced no JSON output: {' '.join(command)}")
    try:
        return json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"command output was not valid JSON: {' '.join(command)}\nstdout:\n{stdout}\nstderr:\n{stderr}"
        ) from exc


def _docker_compose(compose_file: Path, *args: str) -> None:
    subprocess.run(
        ["docker", "compose", "-f", str(compose_file), *args],
        cwd=str(PROJECT_ROOT),
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _wait_for_scylladb_ready(timeout_sec: float = 300.0) -> None:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        result = subprocess.run(
            ["docker", "exec", "denotary-db-agent-scylladb-live", "cqlsh", "-e", "describe keyspaces"],
            cwd=str(PROJECT_ROOT),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        if result.returncode == 0:
            subprocess.run(
                [
                    "docker",
                    "exec",
                    "denotary-db-agent-scylladb-live",
                    "cqlsh",
                    "-e",
                    "create keyspace if not exists denotary_agent with replication = {'class': 'SimpleStrategy', 'replication_factor': 1};",
                ],
                cwd=str(PROJECT_ROOT),
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            return
        time.sleep(5)
    raise RuntimeError("ScyllaDB live container did not become ready in time")


def _wait_for_elasticsearch_ready(timeout_sec: float = 180.0) -> None:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            with urlopen("http://127.0.0.1:59200", timeout=5) as response:  # noqa: S310
                if response.status == 200:
                    return
        except URLError:
            pass
        except Exception:
            pass
        time.sleep(2)
    raise RuntimeError("Elasticsearch live container did not become ready in time")


def _run_with_docker_env(
    *,
    adapter: str,
    compose_file: Path,
    env_updates: dict[str, str],
    command: list[str],
    readiness: callable | None = None,
) -> dict[str, Any]:
    _docker_compose(compose_file, "down", "-v")
    _docker_compose(compose_file, "up", "-d")
    try:
        if readiness is not None:
            readiness()
        env = os.environ.copy()
        env.update(env_updates)
        payload = _run_json(command, env=env)
        payload.setdefault("adapter", adapter)
        return payload
    finally:
        _docker_compose(compose_file, "down", "-v")


def _script_command(script_name: str, *, cycles: int, events_per_cycle: int) -> list[str]:
    return [
        PYTHON,
        str(SCRIPTS_ROOT / script_name),
        "--mode",
        "short-soak",
        "--cycles",
        str(cycles),
        "--events-per-cycle",
        str(events_per_cycle),
    ]


def run_sqlite(*, cycles: int, events_per_cycle: int) -> dict[str, Any]:
    payload = _run_json(_script_command("run-wave2-sqlite-validation.py", cycles=cycles, events_per_cycle=events_per_cycle))
    payload["adapter"] = "sqlite"
    return payload


def run_redis(*, cycles: int, events_per_cycle: int) -> dict[str, Any]:
    payload = _run_json(_script_command("run-wave2-redis-validation.py", cycles=cycles, events_per_cycle=events_per_cycle))
    payload["adapter"] = "redis"
    return payload


def run_db2(*, cycles: int, events_per_cycle: int) -> dict[str, Any]:
    payload = _run_json(_script_command("run-wave2-db2-validation.py", cycles=cycles, events_per_cycle=events_per_cycle))
    payload["adapter"] = "db2"
    return payload


def run_cassandra(*, cycles: int, events_per_cycle: int) -> dict[str, Any]:
    payload = _run_json(_script_command("run-wave2-cassandra-validation.py", cycles=cycles, events_per_cycle=events_per_cycle))
    payload["adapter"] = "cassandra"
    return payload


def run_scylladb(*, cycles: int, events_per_cycle: int) -> dict[str, Any]:
    return _run_with_docker_env(
        adapter="scylladb",
        compose_file=PROJECT_ROOT / "deploy" / "scylladb-live" / "docker-compose.yml",
        env_updates={
            "DENOTARY_SCYLLADB_HOST": "127.0.0.1",
            "DENOTARY_SCYLLADB_PORT": "59043",
            "DENOTARY_SCYLLADB_KEYSPACE": "denotary_agent",
            "DENOTARY_SCYLLADB_USERNAME": "",
            "DENOTARY_SCYLLADB_PASSWORD": "",
        },
        command=_script_command("run-wave2-scylladb-validation.py", cycles=cycles, events_per_cycle=events_per_cycle),
        readiness=_wait_for_scylladb_ready,
    )


def run_elasticsearch(*, cycles: int, events_per_cycle: int) -> dict[str, Any]:
    return _run_with_docker_env(
        adapter="elasticsearch",
        compose_file=PROJECT_ROOT / "deploy" / "elasticsearch-live" / "docker-compose.yml",
        env_updates={
            "DENOTARY_ELASTICSEARCH_URL": "http://127.0.0.1:59200",
            "DENOTARY_ELASTICSEARCH_USERNAME": "",
            "DENOTARY_ELASTICSEARCH_PASSWORD": "",
            "DENOTARY_ELASTICSEARCH_VERIFY_CERTS": "false",
        },
        command=_script_command("run-wave2-elasticsearch-validation.py", cycles=cycles, events_per_cycle=events_per_cycle),
        readiness=_wait_for_elasticsearch_ready,
    )


RUNNERS = {
    "sqlite": run_sqlite,
    "redis": run_redis,
    "scylladb": run_scylladb,
    "db2": run_db2,
    "cassandra": run_cassandra,
    "elasticsearch": run_elasticsearch,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Wave 2 long-soak validation.")
    parser.add_argument(
        "--adapter",
        choices=["all", *RUNNERS.keys()],
        default="all",
        help="Run one adapter or all supported Wave 2 adapters.",
    )
    parser.add_argument("--cycles", type=int, default=DEFAULT_CYCLES)
    parser.add_argument("--events-per-cycle", type=int, default=DEFAULT_EVENTS_PER_CYCLE)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    return parser.parse_args()


def _normalize_status(payload: dict[str, Any]) -> str:
    if payload.get("status") in {"skipped", "failed", "passed"}:
        return str(payload["status"])
    return "passed"


def main() -> None:
    args = parse_args()
    adapters = list(RUNNERS) if args.adapter == "all" else [args.adapter]
    output_root = args.output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    results: list[dict[str, Any]] = []
    for adapter in adapters:
        started_at = time.time()
        try:
            payload = RUNNERS[adapter](cycles=args.cycles, events_per_cycle=args.events_per_cycle)
            payload["status"] = _normalize_status(payload)
        except Exception as exc:  # noqa: BLE001
            payload = {
                "adapter": adapter,
                "validation": "long-soak",
                "status": "failed",
                "error": str(exc),
            }
        payload["elapsed_sec"] = round(time.time() - started_at, 3)
        results.append(payload)

    status_counts = dict(Counter(item["status"] for item in results))
    summary = {
        "validation": "wave2-long-soak",
        "cycles": args.cycles,
        "events_per_cycle": args.events_per_cycle,
        "adapters": adapters,
        "status_counts": status_counts,
        "results": results,
    }
    summary_path = output_root / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
