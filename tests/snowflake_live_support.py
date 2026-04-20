from __future__ import annotations

import os
import socket
import time
import uuid
from pathlib import Path

try:
    import snowflake.connector
except ImportError:  # pragma: no cover - live test dependency gate
    snowflake = None  # type: ignore[assignment]


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SNOWFLAKE_REQUIRED_ENV_VARS = (
    "DENOTARY_SNOWFLAKE_ACCOUNT",
    "DENOTARY_SNOWFLAKE_USERNAME",
    "DENOTARY_SNOWFLAKE_PASSWORD",
    "DENOTARY_SNOWFLAKE_DATABASE",
    "DENOTARY_SNOWFLAKE_SCHEMA",
    "DENOTARY_SNOWFLAKE_WAREHOUSE",
)


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as handle:
        handle.bind(("127.0.0.1", 0))
        return int(handle.getsockname()[1])


def missing_snowflake_env() -> list[str]:
    missing = [name for name in SNOWFLAKE_REQUIRED_ENV_VARS if not os.environ.get(name)]
    if snowflake is None:
        missing.append("snowflake-connector-python")
    return missing


def snowflake_connection_kwargs() -> dict[str, object]:
    return {
        "account": os.environ["DENOTARY_SNOWFLAKE_ACCOUNT"],
        "user": os.environ["DENOTARY_SNOWFLAKE_USERNAME"],
        "password": os.environ["DENOTARY_SNOWFLAKE_PASSWORD"],
        "database": os.environ["DENOTARY_SNOWFLAKE_DATABASE"],
        "schema": os.environ["DENOTARY_SNOWFLAKE_SCHEMA"],
        "warehouse": os.environ["DENOTARY_SNOWFLAKE_WAREHOUSE"],
        "role": os.environ.get("DENOTARY_SNOWFLAKE_ROLE") or None,
    }


def agent_connection_config() -> dict[str, object]:
    payload = {
        "account": os.environ["DENOTARY_SNOWFLAKE_ACCOUNT"],
        "username": os.environ["DENOTARY_SNOWFLAKE_USERNAME"],
        "password": os.environ["DENOTARY_SNOWFLAKE_PASSWORD"],
        "database": os.environ["DENOTARY_SNOWFLAKE_DATABASE"],
        "schema": os.environ["DENOTARY_SNOWFLAKE_SCHEMA"],
        "warehouse": os.environ["DENOTARY_SNOWFLAKE_WAREHOUSE"],
    }
    if os.environ.get("DENOTARY_SNOWFLAKE_ROLE"):
        payload["role"] = os.environ["DENOTARY_SNOWFLAKE_ROLE"]
    return payload


def create_connection():
    if snowflake is None:
        raise RuntimeError("snowflake-connector-python is required for live Snowflake validation")
    return snowflake.connector.connect(**snowflake_connection_kwargs())


def unique_table_name(prefix: str = "DENOTARY_AGENT_LIVE") -> str:
    token = uuid.uuid4().hex[:12].upper()
    return f"{prefix}_{token}"


def wait_for_table_visibility(table_name: str, timeout_sec: float = 30.0) -> None:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        connection = create_connection()
        try:
            with connection.cursor(snowflake.connector.DictCursor) as cursor:
                cursor.execute(
                    """
                    select table_name
                    from information_schema.tables
                    where table_catalog = %s
                      and table_schema = %s
                      and table_name = %s
                    """,
                    (
                        os.environ["DENOTARY_SNOWFLAKE_DATABASE"],
                        os.environ["DENOTARY_SNOWFLAKE_SCHEMA"],
                        table_name,
                    ),
                )
                if cursor.fetchone():
                    return
        finally:
            connection.close()
        time.sleep(1)
    raise RuntimeError(f"snowflake table {table_name} did not become visible in time")
