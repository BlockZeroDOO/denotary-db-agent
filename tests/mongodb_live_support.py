from __future__ import annotations

import socket
import subprocess
from pathlib import Path

from pymongo import MongoClient


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MONGODB_COMPOSE_FILE = PROJECT_ROOT / "deploy" / "mongodb-live" / "docker-compose.yml"
MONGODB_CONTAINER_NAME = "denotary-db-agent-mongodb-live"
MONGODB_PORT = 27018
MONGODB_URI = f"mongodb://127.0.0.1:{MONGODB_PORT}/?directConnection=true"


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as handle:
        handle.bind(("127.0.0.1", 0))
        return int(handle.getsockname()[1])


def wait_for_mongodb_replica_set(timeout_sec: float = 90.0) -> None:
    import time

    deadline = time.time() + timeout_sec
    initiated = False
    while time.time() < deadline:
        client = None
        try:
            client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=2000)
            client.admin.command("ping")
            try:
                hello = client.admin.command("hello")
                if hello.get("setName") == "rs0":
                    return
            except Exception:
                pass
            if not initiated:
                try:
                    subprocess.run(
                        [
                            "docker",
                            "exec",
                            MONGODB_CONTAINER_NAME,
                            "mongosh",
                            "--quiet",
                            "--eval",
                            "rs.initiate({_id:'rs0',members:[{_id:0,host:'localhost:27017'}]})",
                        ],
                        check=True,
                        cwd=str(PROJECT_ROOT),
                        capture_output=True,
                        text=True,
                    )
                    initiated = True
                except Exception as exc:
                    if "already initialized" not in str(exc).lower():
                        raise
            time.sleep(1)
        except Exception:
            time.sleep(1)
        finally:
            if client is not None:
                client.close()
    raise RuntimeError("mongodb replica set did not become ready in time")
