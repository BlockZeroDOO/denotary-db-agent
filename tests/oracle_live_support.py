from __future__ import annotations

import socket
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ORACLE_COMPOSE_FILE = PROJECT_ROOT / "deploy" / "oracle-live" / "docker-compose.yml"
ORACLE_INIT_SQL = PROJECT_ROOT / "deploy" / "oracle-live" / "init.sql"
ORACLE_PORT = 51521
ORACLE_SERVICE_NAME = "FREEPDB1"
ORACLE_USERNAME = "denotary"
ORACLE_PASSWORD = "denotarypw"


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as handle:
        handle.bind(("127.0.0.1", 0))
        return int(handle.getsockname()[1])


def split_oracle_blocks(script: str) -> list[str]:
    blocks: list[str] = []
    current: list[str] = []
    for line in script.splitlines():
        if line.strip() == "/":
            block = "\n".join(current).strip()
            if block:
                blocks.append(block)
            current = []
            continue
        current.append(line)
    block = "\n".join(current).strip()
    if block:
        blocks.append(block)
    return blocks
