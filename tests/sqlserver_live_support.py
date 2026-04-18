from __future__ import annotations

import socket
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SQLSERVER_COMPOSE_FILE = PROJECT_ROOT / "deploy" / "sqlserver-live" / "docker-compose.yml"
SQLSERVER_INIT_SQL = PROJECT_ROOT / "deploy" / "sqlserver-live" / "init.sql"
SQLSERVER_PORT = 51433


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as handle:
        handle.bind(("127.0.0.1", 0))
        return int(handle.getsockname()[1])


def split_tsql_batches(script: str) -> list[str]:
    batches: list[str] = []
    current: list[str] = []
    for line in script.splitlines():
        if line.strip().lower() == "go":
            batch = "\n".join(current).strip()
            if batch:
                batches.append(batch)
            current = []
            continue
        current.append(line)
    batch = "\n".join(current).strip()
    if batch:
        batches.append(batch)
    return batches
