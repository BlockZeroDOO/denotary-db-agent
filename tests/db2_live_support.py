from __future__ import annotations

import os
import socket
import time
import uuid
from pathlib import Path

try:
    import ibm_db_dbi
except ImportError:  # pragma: no cover - live test dependency gate
    ibm_db_dbi = None


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB2_REQUIRED_ENV_VARS = (
    "DENOTARY_DB2_HOST",
    "DENOTARY_DB2_PORT",
    "DENOTARY_DB2_USERNAME",
    "DENOTARY_DB2_PASSWORD",
    "DENOTARY_DB2_DATABASE",
    "DENOTARY_DB2_SCHEMA",
)


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as handle:
        handle.bind(("127.0.0.1", 0))
        return int(handle.getsockname()[1])


def missing_db2_env() -> list[str]:
    missing = [name for name in DB2_REQUIRED_ENV_VARS if not os.environ.get(name)]
    if ibm_db_dbi is None:
        missing.append("ibm_db")
    return missing


def db2_schema() -> str:
    return os.environ["DENOTARY_DB2_SCHEMA"].strip().upper()


def db2_dsn() -> str:
    return (
        f"DATABASE={os.environ['DENOTARY_DB2_DATABASE']};"
        f"HOSTNAME={os.environ['DENOTARY_DB2_HOST']};"
        f"PORT={int(os.environ['DENOTARY_DB2_PORT'])};"
        "PROTOCOL=TCPIP;"
        f"UID={os.environ['DENOTARY_DB2_USERNAME']};"
        f"PWD={os.environ['DENOTARY_DB2_PASSWORD']};"
    )


def create_connection():
    if ibm_db_dbi is None:
        raise RuntimeError("ibm_db is required for live IBM Db2 validation")
    return ibm_db_dbi.connect(db2_dsn(), "", "")


def agent_connection_config() -> dict[str, object]:
    return {
        "host": os.environ["DENOTARY_DB2_HOST"],
        "port": int(os.environ["DENOTARY_DB2_PORT"]),
        "username": os.environ["DENOTARY_DB2_USERNAME"],
        "password": os.environ["DENOTARY_DB2_PASSWORD"],
        "database": os.environ["DENOTARY_DB2_DATABASE"],
    }


def unique_table_name(prefix: str = "DENOTARY_AGENT_LIVE") -> str:
    token = uuid.uuid4().hex[:12].upper()
    return f"{prefix}_{token}"


def wait_for_table_visibility(table_name: str, timeout_sec: float = 30.0) -> None:
    deadline = time.time() + timeout_sec
    schema_name = db2_schema()
    while time.time() < deadline:
        connection = create_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    select tabname
                    from syscat.tables
                    where tabschema = ?
                      and tabname = ?
                    """,
                    (schema_name, table_name),
                )
                if cursor.fetchone():
                    return
        finally:
            connection.close()
        time.sleep(1)
    raise RuntimeError(f"db2 table {schema_name}.{table_name} did not become visible in time")
