from __future__ import annotations

import os
import socket
import site
import time
import uuid
from pathlib import Path

_DB2_DLL_BOOTSTRAPPED = False


def _bootstrap_db2_windows_dlls() -> None:
    global _DB2_DLL_BOOTSTRAPPED
    if _DB2_DLL_BOOTSTRAPPED or os.name != "nt" or not hasattr(os, "add_dll_directory"):
        return
    candidates = [Path(path) for path in site.getsitepackages()] + [Path(site.getusersitepackages())]
    for base in candidates:
        clidriver_bin = base / "clidriver" / "bin"
        if not clidriver_bin.exists():
            continue
        os.add_dll_directory(str(clidriver_bin))
        for extra in ("amd64.VC14.CRT", "amd64.VC12.CRT"):
            extra_path = clidriver_bin / extra
            if extra_path.exists():
                os.add_dll_directory(str(extra_path))
        _DB2_DLL_BOOTSTRAPPED = True
        return


_bootstrap_db2_windows_dlls()

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


def wait_for_db2_ready(timeout_sec: float = 240.0, consecutive_successes: int = 3, success_interval_sec: float = 5.0) -> None:
    deadline = time.time() + timeout_sec
    successes = 0
    while time.time() < deadline:
        try:
            connection = create_connection()
            try:
                with connection.cursor() as cursor:
                    cursor.execute("select 1 from sysibm.sysdummy1")
                    cursor.fetchone()
            finally:
                connection.close()
            successes += 1
            if successes >= consecutive_successes:
                return
            time.sleep(success_interval_sec)
            continue
        except Exception:
            successes = 0
            time.sleep(2)
    raise RuntimeError("db2 did not become ready in time")


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
