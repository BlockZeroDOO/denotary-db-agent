from __future__ import annotations

import socket
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MONGODB_COMPOSE_FILE = PROJECT_ROOT / "deploy" / "mongodb-live" / "docker-compose.yml"
MONGODB_PORT = 27018


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as handle:
        handle.bind(("127.0.0.1", 0))
        return int(handle.getsockname()[1])
