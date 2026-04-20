from __future__ import annotations

import os
import socket
import time
import uuid
from pathlib import Path

try:
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.cluster import Cluster
except ImportError:  # pragma: no cover - live test dependency gate
    Cluster = None
    PlainTextAuthProvider = None


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CASSANDRA_REQUIRED_ENV_VARS = (
    "DENOTARY_CASSANDRA_HOST",
    "DENOTARY_CASSANDRA_PORT",
    "DENOTARY_CASSANDRA_KEYSPACE",
)


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as handle:
        handle.bind(("127.0.0.1", 0))
        return int(handle.getsockname()[1])


def missing_cassandra_env() -> list[str]:
    missing = [name for name in CASSANDRA_REQUIRED_ENV_VARS if not os.environ.get(name)]
    if Cluster is None:
        missing.append("cassandra-driver")
    return missing


def cassandra_keyspace() -> str:
    return os.environ["DENOTARY_CASSANDRA_KEYSPACE"].strip()


def create_session():
    if Cluster is None:
        raise RuntimeError("cassandra-driver is required for live Cassandra validation")
    cluster_kwargs: dict[str, object] = {
        "contact_points": [os.environ["DENOTARY_CASSANDRA_HOST"]],
        "port": int(os.environ["DENOTARY_CASSANDRA_PORT"]),
    }
    username = os.environ.get("DENOTARY_CASSANDRA_USERNAME")
    password = os.environ.get("DENOTARY_CASSANDRA_PASSWORD")
    if username and password and PlainTextAuthProvider is not None:
        cluster_kwargs["auth_provider"] = PlainTextAuthProvider(username=username, password=password)
    cluster = Cluster(**cluster_kwargs)
    session = cluster.connect()
    return cluster, session


def agent_connection_config() -> dict[str, object]:
    payload: dict[str, object] = {
        "host": os.environ["DENOTARY_CASSANDRA_HOST"],
        "port": int(os.environ["DENOTARY_CASSANDRA_PORT"]),
    }
    if os.environ.get("DENOTARY_CASSANDRA_USERNAME"):
        payload["username"] = os.environ["DENOTARY_CASSANDRA_USERNAME"]
    if os.environ.get("DENOTARY_CASSANDRA_PASSWORD"):
        payload["password"] = os.environ["DENOTARY_CASSANDRA_PASSWORD"]
    return payload


def unique_table_name(prefix: str = "denotary_agent_live") -> str:
    token = uuid.uuid4().hex[:12].lower()
    return f"{prefix}_{token}"


def wait_for_table_visibility(table_name: str, timeout_sec: float = 30.0) -> None:
    deadline = time.time() + timeout_sec
    keyspace_name = cassandra_keyspace()
    while time.time() < deadline:
        cluster, session = create_session()
        try:
            rows = session.execute(
                """
                select table_name
                from system_schema.tables
                where keyspace_name = %s
                  and table_name = %s
                """,
                (keyspace_name, table_name),
            )
            if list(rows):
                return
        finally:
            cluster.shutdown()
        time.sleep(1)
    raise RuntimeError(f"cassandra table {keyspace_name}.{table_name} did not become visible in time")
