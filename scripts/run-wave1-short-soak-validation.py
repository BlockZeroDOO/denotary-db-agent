from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable

import oracledb
import pymysql
import pytds
from pymongo import MongoClient

from denotary_db_agent.config import load_config
from denotary_db_agent.engine import AgentEngine


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RESTART_SCRIPT = PROJECT_ROOT / "scripts" / "run-wave1-source-restart-validation.py"


def _load_restart_helpers():
    spec = importlib.util.spec_from_file_location("wave1_restart_validation", RESTART_SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError("could not load wave1 restart validation helpers")
    module = importlib.util.module_from_spec(spec)
    sys.modules["wave1_restart_validation"] = module
    spec.loader.exec_module(module)
    return module


HELPERS = _load_restart_helpers()

SOAK_CYCLES = 5
EVENTS_PER_CYCLE = 3


def _run_cycles(
    *,
    engine: AgentEngine,
    cycle_writer: Callable[[int], None],
    sleep_after_write: float = 0.0,
) -> dict[str, Any]:
    baseline = engine.run_once()
    total_processed = baseline["processed"]
    total_failed = baseline["failed"]
    cycle_results: list[dict[str, int]] = []
    for cycle in range(SOAK_CYCLES):
        cycle_writer(cycle)
        if sleep_after_write > 0:
            time.sleep(sleep_after_write)
        result = engine.run_once()
        cycle_results.append(result)
        total_processed += result["processed"]
        total_failed += result["failed"]
    return {
        "baseline": baseline,
        "cycles": cycle_results,
        "total_processed": total_processed,
        "total_failed": total_failed,
    }


def _assert_short_soak_counts(result: dict[str, Any]) -> None:
    expected_total = SOAK_CYCLES * EVENTS_PER_CYCLE
    if int(result.get("total_processed") or 0) != expected_total:
        raise RuntimeError(
            f"{result['adapter']} short soak processed {result.get('total_processed')} events, expected {expected_total}"
        )
    if int(result.get("total_failed") or 0) != 0:
        raise RuntimeError(f"{result['adapter']} short soak recorded failures")
    if int(result.get("delivery_count") or 0) != expected_total:
        raise RuntimeError(
            f"{result['adapter']} short soak exported {result.get('delivery_count')} deliveries, expected {expected_total}"
        )
    if int(result.get("proof_count") or 0) != expected_total:
        raise RuntimeError(
            f"{result['adapter']} short soak exported {result.get('proof_count')} proofs, expected {expected_total}"
        )
    if int(result.get("dlq_count") or 0) != 0:
        raise RuntimeError(f"{result['adapter']} short soak pushed events to DLQ")


def mysql_short_soak() -> dict[str, Any]:
    HELPERS.docker_compose(HELPERS.MYSQL_COMPOSE_FILE, "down", "-v")
    HELPERS.docker_compose(HELPERS.MYSQL_COMPOSE_FILE, "up", "-d")
    HELPERS.wait_mysql_like(53306)
    with pymysql.connect(host="127.0.0.1", port=53306, user="denotary", password="denotarypw", database="ledger", autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute("delete from payments")
            cursor.execute("delete from invoices")
    admin = pymysql.connect(host="127.0.0.1", port=53306, user="root", password="rootpw", database="ledger", autocommit=True)
    try:
        with admin.cursor() as cursor:
            row = None
            for statement in ("show binary log status", "show master status"):
                try:
                    cursor.execute(statement)
                    row = cursor.fetchone()
                    if row:
                        break
                except Exception:
                    continue
    finally:
        admin.close()
    if not row:
        raise RuntimeError("mysql did not return SHOW MASTER STATUS output")
    start_file = str(row[0] if not isinstance(row, dict) else row["File"])
    start_pos = int(row[1] if not isinstance(row, dict) else row["Position"])
    mock = HELPERS.MockServiceStack()
    mock.start()
    try:
        with tempfile.TemporaryDirectory() as temp:
            temp_dir = Path(temp)
            source = {
                "id": "mysql-short-soak",
                "adapter": "mysql",
                "enabled": True,
                "source_instance": "short-soak",
                "database_name": "ledger",
                "include": {"ledger": ["invoices"]},
                "connection": {
                    "host": "127.0.0.1",
                    "port": 53306,
                    "username": "denotary",
                    "password": "denotarypw",
                    "database": "ledger",
                },
                "backfill_mode": "none",
                "batch_enabled": False,
                "options": {
                    "capture_mode": "binlog",
                    "watermark_column": "updated_at",
                    "commit_timestamp_column": "updated_at",
                    "row_limit": 50,
                    "binlog_server_id": 16101,
                    "binlog_start_file": start_file,
                    "binlog_start_pos": start_pos,
                },
            }
            config_path = HELPERS.write_config(temp_dir, "mysql-short-soak", source, mock)
            engine = AgentEngine(load_config(config_path))
            try:
                engine.bootstrap("mysql-short-soak")

                def write_cycle(cycle: int) -> None:
                    stamps = HELPERS.datetime_values(datetime(2026, 4, 19, 6, cycle, 0), EVENTS_PER_CYCLE)
                    with pymysql.connect(
                        host="127.0.0.1",
                        port=53306,
                        user="denotary",
                        password="denotarypw",
                        database="ledger",
                        autocommit=True,
                    ) as connection:
                        with connection.cursor() as cursor:
                            for index, stamp in enumerate(stamps, start=1):
                                cursor.execute(
                                    "insert into invoices (id, status, amount, updated_at) values (%s, %s, %s, %s)",
                                    (
                                        30000 + cycle * 100 + index,
                                        "issued" if cycle % 2 == 0 else "paid",
                                        Decimal("100.00") + cycle + index,
                                        stamp.strftime("%Y-%m-%d %H:%M:%S"),
                                    ),
                                )

                run = _run_cycles(engine=engine, cycle_writer=write_cycle)
                deliveries = engine.store.list_deliveries("mysql-short-soak")
                proofs = engine.store.list_proofs("mysql-short-soak")
                dlq = engine.store.list_dlq("mysql-short-soak")
                result = {
                    "adapter": "mysql",
                    "capture_mode": "binlog",
                    **run,
                    "delivery_count": len(deliveries),
                    "proof_count": len(proofs),
                    "dlq_count": len(dlq),
                }
                _assert_short_soak_counts(result)
                return result
            finally:
                engine.close()
    finally:
        mock.stop()
        HELPERS.docker_compose(HELPERS.MYSQL_COMPOSE_FILE, "down", "-v")


def mariadb_short_soak() -> dict[str, Any]:
    HELPERS.docker_compose(HELPERS.MARIADB_COMPOSE_FILE, "down", "-v")
    HELPERS.docker_compose(HELPERS.MARIADB_COMPOSE_FILE, "up", "-d")
    HELPERS.wait_mysql_like(53307)
    with pymysql.connect(host="127.0.0.1", port=53307, user="denotary", password="denotarypw", database="ledger", autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute("delete from payments")
            cursor.execute("delete from invoices")
    admin = pymysql.connect(host="127.0.0.1", port=53307, user="root", password="rootpw", database="ledger", autocommit=True)
    try:
        with admin.cursor() as cursor:
            row = None
            for statement in ("show binary log status", "show master status"):
                try:
                    cursor.execute(statement)
                    row = cursor.fetchone()
                    if row:
                        break
                except Exception:
                    continue
    finally:
        admin.close()
    if not row:
        raise RuntimeError("mariadb did not return binary log status output")
    start_file = str(row[0] if not isinstance(row, dict) else row["File"])
    start_pos = int(row[1] if not isinstance(row, dict) else row["Position"])
    mock = HELPERS.MockServiceStack()
    mock.start()
    try:
        with tempfile.TemporaryDirectory() as temp:
            temp_dir = Path(temp)
            source = {
                "id": "mariadb-short-soak",
                "adapter": "mariadb",
                "enabled": True,
                "source_instance": "short-soak",
                "database_name": "ledger",
                "include": {"ledger": ["invoices"]},
                "connection": {
                    "host": "127.0.0.1",
                    "port": 53307,
                    "username": "denotary",
                    "password": "denotarypw",
                    "database": "ledger",
                },
                "backfill_mode": "none",
                "batch_enabled": False,
                "options": {
                    "capture_mode": "binlog",
                    "watermark_column": "updated_at",
                    "commit_timestamp_column": "updated_at",
                    "row_limit": 50,
                    "binlog_server_id": 16121,
                    "binlog_start_file": start_file,
                    "binlog_start_pos": start_pos,
                },
            }
            config_path = HELPERS.write_config(temp_dir, "mariadb-short-soak", source, mock)
            engine = AgentEngine(load_config(config_path))
            try:
                engine.bootstrap("mariadb-short-soak")

                def write_cycle(cycle: int) -> None:
                    stamps = HELPERS.datetime_values(datetime(2026, 4, 19, 7, cycle, 0), EVENTS_PER_CYCLE)
                    with pymysql.connect(
                        host="127.0.0.1",
                        port=53307,
                        user="denotary",
                        password="denotarypw",
                        database="ledger",
                        autocommit=True,
                    ) as connection:
                        with connection.cursor() as cursor:
                            for index, stamp in enumerate(stamps, start=1):
                                cursor.execute(
                                    "insert into invoices (id, status, amount, updated_at) values (%s, %s, %s, %s)",
                                    (
                                        31000 + cycle * 100 + index,
                                        "issued" if cycle % 2 == 0 else "paid",
                                        Decimal("110.00") + cycle + index,
                                        stamp.strftime("%Y-%m-%d %H:%M:%S"),
                                    ),
                                )

                run = _run_cycles(engine=engine, cycle_writer=write_cycle)
                deliveries = engine.store.list_deliveries("mariadb-short-soak")
                proofs = engine.store.list_proofs("mariadb-short-soak")
                dlq = engine.store.list_dlq("mariadb-short-soak")
                result = {
                    "adapter": "mariadb",
                    "capture_mode": "binlog",
                    **run,
                    "delivery_count": len(deliveries),
                    "proof_count": len(proofs),
                    "dlq_count": len(dlq),
                }
                _assert_short_soak_counts(result)
                return result
            finally:
                engine.close()
    finally:
        mock.stop()
        HELPERS.docker_compose(HELPERS.MARIADB_COMPOSE_FILE, "down", "-v")


def sqlserver_short_soak() -> dict[str, Any]:
    HELPERS.docker_compose(HELPERS.SQLSERVER_COMPOSE_FILE, "down", "-v")
    HELPERS.docker_compose(HELPERS.SQLSERVER_COMPOSE_FILE, "up", "-d")
    HELPERS.wait_sqlserver()
    HELPERS.apply_sqlserver_init()
    connection = pytds.connect(
        dsn="127.0.0.1",
        port=51433,
        database="ledger",
        user="sa",
        password="StrongP@ssw0rd!",
        as_dict=True,
        autocommit=True,
        login_timeout=10,
        timeout=10,
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute("delete from dbo.payments")
            cursor.execute("delete from dbo.invoices")
    finally:
        connection.close()
    mock = HELPERS.MockServiceStack()
    mock.start()
    try:
        with tempfile.TemporaryDirectory() as temp:
            temp_dir = Path(temp)
            source = {
                "id": "sqlserver-short-soak",
                "adapter": "sqlserver",
                "enabled": True,
                "source_instance": "short-soak",
                "database_name": "ledger",
                "include": {"dbo": ["invoices"]},
                "connection": {
                    "host": "127.0.0.1",
                    "port": 51433,
                    "username": "sa",
                    "password": "StrongP@ssw0rd!",
                    "database": "ledger",
                },
                "backfill_mode": "none",
                "batch_enabled": False,
                "options": {
                    "capture_mode": "change_tracking",
                    "watermark_column": "updated_at",
                    "commit_timestamp_column": "updated_at",
                    "row_limit": 50,
                },
            }
            config_path = HELPERS.write_config(temp_dir, "sqlserver-short-soak", source, mock)
            engine = AgentEngine(load_config(config_path))
            try:
                engine.bootstrap("sqlserver-short-soak")

                def write_cycle(cycle: int) -> None:
                    stamps = HELPERS.datetime_values(datetime(2026, 4, 19, 8, cycle, 0), EVENTS_PER_CYCLE)
                    connection = pytds.connect(
                        dsn="127.0.0.1",
                        port=51433,
                        database="ledger",
                        user="sa",
                        password="StrongP@ssw0rd!",
                        as_dict=True,
                        autocommit=True,
                        login_timeout=10,
                        timeout=10,
                    )
                    try:
                        with connection.cursor() as cursor:
                            for index, stamp in enumerate(stamps, start=1):
                                cursor.execute(
                                    "insert into dbo.invoices (id, status, amount, updated_at) values (%s, %s, %s, %s)",
                                    (
                                        32000 + cycle * 100 + index,
                                        "issued" if cycle % 2 == 0 else "paid",
                                        Decimal("120.00") + cycle + index,
                                        stamp,
                                    ),
                                )
                    finally:
                        connection.close()

                run = _run_cycles(engine=engine, cycle_writer=write_cycle)
                deliveries = engine.store.list_deliveries("sqlserver-short-soak")
                proofs = engine.store.list_proofs("sqlserver-short-soak")
                dlq = engine.store.list_dlq("sqlserver-short-soak")
                result = {
                    "adapter": "sqlserver",
                    "capture_mode": "change_tracking",
                    **run,
                    "delivery_count": len(deliveries),
                    "proof_count": len(proofs),
                    "dlq_count": len(dlq),
                }
                _assert_short_soak_counts(result)
                return result
            finally:
                engine.close()
    finally:
        mock.stop()
        HELPERS.docker_compose(HELPERS.SQLSERVER_COMPOSE_FILE, "down", "-v")


def oracle_short_soak() -> dict[str, Any]:
    HELPERS.docker_compose(HELPERS.ORACLE_COMPOSE_FILE, "down", "-v")
    HELPERS.docker_compose(HELPERS.ORACLE_COMPOSE_FILE, "up", "-d")
    HELPERS.wait_oracle_root()
    HELPERS.enable_oracle_archivelog()
    HELPERS.wait_oracle_pdb()
    HELPERS.enable_oracle_root_supplemental_logging()
    HELPERS.apply_oracle_init()
    connection = oracledb.connect(
        user="denotary",
        password="denotarypw",
        host="127.0.0.1",
        port=51521,
        service_name="FREEPDB1",
        tcp_connect_timeout=10,
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute("delete from payments")
            cursor.execute("delete from invoices")
        connection.commit()
    finally:
        connection.close()
    mock = HELPERS.MockServiceStack()
    mock.start()
    try:
        with tempfile.TemporaryDirectory() as temp:
            temp_dir = Path(temp)
            source = {
                "id": "oracle-short-soak",
                "adapter": "oracle",
                "enabled": True,
                "source_instance": "short-soak",
                "database_name": "ledger",
                "include": {"DENOTARY": ["INVOICES"]},
                "connection": {
                    "host": "127.0.0.1",
                    "port": 51521,
                    "username": "denotary",
                    "password": "denotarypw",
                    "service_name": "FREEPDB1",
                    "admin_username": "system",
                    "admin_password": "oraclepw",
                    "admin_service_name": "FREE",
                },
                "backfill_mode": "none",
                "batch_enabled": False,
                "options": {
                    "capture_mode": "logminer",
                    "watermark_column": "updated_at",
                    "commit_timestamp_column": "updated_at",
                    "row_limit": 50,
                },
            }
            config_path = HELPERS.write_config(temp_dir, "oracle-short-soak", source, mock)
            engine = AgentEngine(load_config(config_path))
            try:
                engine.bootstrap("oracle-short-soak")

                def write_cycle(cycle: int) -> None:
                    stamps = HELPERS.datetime_values(datetime(2026, 4, 19, 9, cycle, 0), EVENTS_PER_CYCLE)
                    connection = oracledb.connect(
                        user="denotary",
                        password="denotarypw",
                        host="127.0.0.1",
                        port=51521,
                        service_name="FREEPDB1",
                        tcp_connect_timeout=10,
                    )
                    try:
                        with connection.cursor() as cursor:
                            for index, stamp in enumerate(stamps, start=1):
                                cursor.execute(
                                    "insert into invoices (id, status, amount, updated_at) values (:1, :2, :3, :4)",
                                    [33000 + cycle * 100 + index, "issued" if cycle % 2 == 0 else "paid", 130 + cycle + index, stamp],
                                )
                        connection.commit()
                    finally:
                        connection.close()

                run = _run_cycles(engine=engine, cycle_writer=write_cycle)
                deliveries = engine.store.list_deliveries("oracle-short-soak")
                proofs = engine.store.list_proofs("oracle-short-soak")
                dlq = engine.store.list_dlq("oracle-short-soak")
                result = {
                    "adapter": "oracle",
                    "capture_mode": "logminer",
                    **run,
                    "delivery_count": len(deliveries),
                    "proof_count": len(proofs),
                    "dlq_count": len(dlq),
                }
                _assert_short_soak_counts(result)
                return result
            finally:
                engine.close()
    finally:
        mock.stop()
        HELPERS.docker_compose(HELPERS.ORACLE_COMPOSE_FILE, "down", "-v")


def mongodb_short_soak() -> dict[str, Any]:
    HELPERS.docker_compose(HELPERS.MONGODB_COMPOSE_FILE, "down", "-v")
    HELPERS.docker_compose(HELPERS.MONGODB_COMPOSE_FILE, "up", "-d")
    HELPERS.wait_for_mongodb_replica_set()
    client = MongoClient(HELPERS.MONGODB_URI, serverSelectionTimeoutMS=5000)
    try:
        db = client["ledger"]
        existing = set(db.list_collection_names())
        if "invoices" not in existing:
            db.create_collection("invoices")
        if "payments" not in existing:
            db.create_collection("payments")
        db["payments"].delete_many({})
        db["invoices"].delete_many({})
    finally:
        client.close()
    mock = HELPERS.MockServiceStack()
    mock.start()
    try:
        with tempfile.TemporaryDirectory() as temp:
            temp_dir = Path(temp)
            source = {
                "id": "mongodb-short-soak",
                "adapter": "mongodb",
                "enabled": True,
                "source_instance": "short-soak",
                "database_name": "ledger",
                "include": {"ledger": ["invoices"]},
                "connection": {"uri": HELPERS.MONGODB_URI},
                "backfill_mode": "none",
                "batch_enabled": False,
                "options": {
                    "capture_mode": "change_streams",
                    "watermark_column": "updated_at",
                    "commit_timestamp_column": "updated_at",
                    "row_limit": 50,
                },
            }
            config_path = HELPERS.write_config(temp_dir, "mongodb-short-soak", source, mock)
            engine = AgentEngine(load_config(config_path))
            try:
                engine.bootstrap("mongodb-short-soak")

                def write_cycle(cycle: int) -> None:
                    client = MongoClient(HELPERS.MONGODB_URI, serverSelectionTimeoutMS=5000)
                    try:
                        db = client["ledger"]
                        for index, stamp in enumerate(
                            HELPERS.datetime_values(datetime(2026, 4, 19, 10, cycle, 0, tzinfo=timezone.utc), EVENTS_PER_CYCLE),
                            start=1,
                        ):
                            db["invoices"].insert_one(
                                {
                                    "status": "issued" if cycle % 2 == 0 else "paid",
                                    "amount": 140.0 + cycle + index,
                                    "updated_at": stamp,
                                }
                            )
                    finally:
                        client.close()

                run = _run_cycles(engine=engine, cycle_writer=write_cycle, sleep_after_write=2.0)
                deliveries = engine.store.list_deliveries("mongodb-short-soak")
                proofs = engine.store.list_proofs("mongodb-short-soak")
                dlq = engine.store.list_dlq("mongodb-short-soak")
                result = {
                    "adapter": "mongodb",
                    "capture_mode": "change_streams",
                    **run,
                    "delivery_count": len(deliveries),
                    "proof_count": len(proofs),
                    "dlq_count": len(dlq),
                }
                _assert_short_soak_counts(result)
                return result
            finally:
                engine.close()
    finally:
        mock.stop()
        HELPERS.docker_compose(HELPERS.MONGODB_COMPOSE_FILE, "down", "-v")


def main() -> None:
    runners = {
        "mysql": mysql_short_soak,
        "mariadb": mariadb_short_soak,
        "sqlserver": sqlserver_short_soak,
        "oracle": oracle_short_soak,
        "mongodb": mongodb_short_soak,
    }
    results: list[dict[str, Any]] = []
    for adapter, runner in runners.items():
        try:
            payload = runner()
            payload["status"] = "passed"
            results.append(payload)
        except Exception as exc:  # noqa: BLE001
            results.append({"adapter": adapter, "status": "failed", "error": str(exc)})
    print(json.dumps({"cycles": SOAK_CYCLES, "events_per_cycle": EVENTS_PER_CYCLE, "results": results}, indent=2))


if __name__ == "__main__":
    main()
