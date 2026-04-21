from __future__ import annotations

import socket
import subprocess
import time
from pathlib import Path

try:
    import redis
except ImportError:  # pragma: no cover - live test dependency gate
    redis = None  # type: ignore[assignment]


PROJECT_ROOT = Path(__file__).resolve().parents[1]
REDIS_COMPOSE_FILE = PROJECT_ROOT / "deploy" / "redis-live" / "docker-compose.yml"
REDIS_PORT = 56379
REDIS_URL = f"redis://127.0.0.1:{REDIS_PORT}/0"


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as handle:
        handle.bind(("127.0.0.1", 0))
        return int(handle.getsockname()[1])


def wait_for_redis(timeout_sec: float = 60.0) -> None:
    if redis is None:
        raise RuntimeError("redis package is required for live Redis validation")
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        client = None
        try:
            client = redis.Redis.from_url(REDIS_URL, decode_responses=False, socket_timeout=5)
            if client.ping():
                return
        except Exception:
            time.sleep(1)
        finally:
            if client is not None:
                close = getattr(client, "close", None)
                if callable(close):
                    close()
    raise RuntimeError("redis live container did not become ready in time")


def run_redis_compose(*args: str, retries: int = 3) -> None:
    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            subprocess.run(
                ["docker", "compose", "-f", str(REDIS_COMPOSE_FILE), *args],
                check=True,
                cwd=str(PROJECT_ROOT),
                capture_output=True,
                text=True,
            )
            return
        except subprocess.CalledProcessError as exc:
            last_error = exc
            if attempt == retries - 1:
                raise
            time.sleep(1)
    if last_error is not None:
        raise last_error
