from __future__ import annotations

import argparse
import importlib.util
import json
import math
import sys
import time
from datetime import datetime, timedelta, timezone
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
DATA_ROOT = PROJECT_ROOT / "data"
RESTART_SCRIPT = PROJECT_ROOT / "scripts" / "run-wave1-source-restart-validation.py"

APPROX_SINGLE_EVENT_BYTES = 88
DEFAULT_TARGET_KIB = 100
DEFAULT_BATCH_SIZE = 100
DEFAULT_MONGODB_SLEEP_AFTER_WRITE = 2.0
DEFAULT_ORACLE_SLEEP_AFTER_WRITE = 10.0
DEFAULT_BASE_CONFIG = DATA_ROOT / "denotary-live-env-e2e" / "agent.1776475214.json"


def _load_restart_helpers():
    spec = importlib.util.spec_from_file_location("wave1_restart_validation", RESTART_SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError("could not load wave1 restart validation helpers")
    module = importlib.util.module_from_spec(spec)
    sys.modules["wave1_restart_validation"] = module
    spec.loader.exec_module(module)
    return module


HELPERS = _load_restart_helpers()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Wave 1 mainnet budget validation.")
    parser.add_argument("--config", default=str(DEFAULT_BASE_CONFIG), help="Base live denotary config for signer/off-chain settings.")
    parser.add_argument("--target-kib", type=int, default=DEFAULT_TARGET_KIB, help="Approximate KiB budget per adapter.")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Events per batch transaction.")
    parser.add_argument("--mode", choices=["batch", "single"], default="batch", help="Batch anchoring or single-submit mode.")
    parser.add_argument("--adapter", choices=["mysql", "mariadb", "sqlserver", "oracle", "mongodb"])
    parser.add_argument("--mongodb-sleep-after-write", type=float, default=DEFAULT_MONGODB_SLEEP_AFTER_WRITE)
    parser.add_argument("--oracle-sleep-after-write", type=float, default=DEFAULT_ORACLE_SLEEP_AFTER_WRITE)
    parser.add_argument("--output-root", default="", help="Optional persistent run directory.")
    return parser.parse_args()


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _run_seed(timestamp: str) -> int:
    digits = "".join(character for character in timestamp if character.isdigit())
    return int(digits[-6:])


def _base_row_id(prefix: int, timestamp: str) -> int:
    return prefix + (_run_seed(timestamp) * 10_000)


def _approx_batch_kib(batch_size: int) -> int:
    return max(1, math.ceil((APPROX_SINGLE_EVENT_BYTES * batch_size) / 1024))


def _cycle_count_for_budget(target_kib: int, batch_size: int) -> int:
    return max(1, math.ceil(target_kib / _approx_batch_kib(batch_size)))


def _policy_id_for_mode(mode: str) -> int:
    return 2 if mode == "batch" else 1


def _batch_enabled_for_mode(mode: str) -> bool:
    return mode == "batch"


def _expected_export_count(*, cycles: int, batch_size: int, mode: str) -> int:
    return cycles if mode == "batch" else cycles * batch_size


def _denotary_for_mode(denotary: dict[str, Any], mode: str) -> dict[str, Any]:
    payload = {**denotary, "policy_id": _policy_id_for_mode(mode)}
    if mode == "single":
        payload["wait_for_finality"] = False
    return payload


def _load_base_denotary(config_path: Path) -> dict[str, Any]:
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    return dict(payload["denotary"])


def _ensure_offchain_services_ready(denotary: dict[str, Any]) -> None:
    import urllib.request

    targets = [
        (str(denotary["ingress_url"]).rstrip("/") + "/healthz", "ingress"),
        (str(denotary["watcher_url"]).rstrip("/") + "/healthz", "watcher"),
        (str(denotary["receipt_url"]).rstrip("/") + "/healthz", "receipt"),
        (str(denotary["audit_url"]).rstrip("/") + "/healthz", "audit"),
    ]
    for url, name in targets:
        with urllib.request.urlopen(url, timeout=5) as response:  # noqa: S310
            if response.status >= 400:
                raise RuntimeError(f"{name} health probe failed with status {response.status}")


def _build_runtime_config(
    *,
    denotary: dict[str, Any],
    agent_name: str,
    source: dict[str, Any],
    state_db: Path,
    proof_dir: Path,
) -> dict[str, Any]:
    return {
        "agent_name": agent_name,
        "log_level": "INFO",
        "denotary": denotary,
        "storage": {
            "state_db": str(state_db.resolve()),
            "proof_dir": str(proof_dir.resolve()),
        },
        "sources": [source],
    }


def _read_proof(export_path: str | None) -> dict[str, Any]:
    if not export_path:
        return {}
    return json.loads(Path(export_path).read_text(encoding="utf-8"))


def _finalize_result(
    *,
    adapter: str,
    capture_mode: str,
    target_kib: int,
    batch_size: int,
    cycles: int,
    source_id: str,
    engine: AgentEngine,
) -> dict[str, Any]:
    deliveries = engine.store.list_deliveries(source_id)
    proofs = engine.store.list_proofs(source_id)
    dlq = engine.store.list_dlq(source_id)
    latest_proof = proofs[0] if proofs else {}
    proof_payload = _read_proof(str(latest_proof.get("export_path") or ""))
    return {
        "adapter": adapter,
        "capture_mode": capture_mode,
        "target_kib": target_kib,
        "approx_batch_kib": _approx_batch_kib(batch_size),
        "approx_total_kib": _approx_batch_kib(batch_size) * cycles,
        "batch_size": batch_size,
        "cycles": cycles,
        "delivery_count": len(deliveries),
        "proof_count": len(proofs),
        "dlq_count": len(dlq),
        "latest_request_id": latest_proof.get("request_id"),
        "latest_export_path": latest_proof.get("export_path"),
        "latest_tx_id": (proof_payload.get("receipt") or {}).get("tx_id"),
        "latest_block_num": (proof_payload.get("receipt") or {}).get("block_num"),
        "report": engine.report(source_id),
    }


def _drain_cycle(
    *,
    engine: AgentEngine,
    expected_processed: int,
    max_attempts: int = 3,
    sleep_after_idle: float = 1.0,
) -> dict[str, int]:
    total_processed = 0
    total_failed = 0
    for attempt in range(max_attempts):
        result = engine.run_once()
        total_processed += int(result.get("processed") or 0)
        total_failed += int(result.get("failed") or 0)
        if total_failed > 0 or total_processed >= expected_processed:
            break
        if int(result.get("processed") or 0) == 0:
            time.sleep(sleep_after_idle)
    return {"processed": total_processed, "failed": total_failed}


def _pending_finality_count(engine: AgentEngine, source_id: str) -> int:
    deliveries = engine.store.list_deliveries(source_id)
    return sum(1 for item in deliveries if str(item.get("status") or "") in {"prepared_registered", "broadcast_included"})


def _reconcile_single_phase(
    *,
    engine: AgentEngine,
    source_id: str,
    expected_exports: int,
    max_passes: int = 900,
    sleep_after_idle: float = 1.0,
) -> None:
    stall_passes = 0
    while max_passes > 0:
        max_passes -= 1
        proof_count_before = len(engine.store.list_proofs(source_id))
        pending_before = _pending_finality_count(engine, source_id)
        if proof_count_before >= expected_exports and pending_before == 0:
            return
        result = engine.reconcile_pending_finality(source_id, limit=256)
        proof_count_after = len(engine.store.list_proofs(source_id))
        pending_after = _pending_finality_count(engine, source_id)
        if proof_count_after >= expected_exports and pending_after == 0:
            return
        if result["reconciled"] == 0 and result["failed"] == 0 and proof_count_after == proof_count_before:
            stall_passes += 1
            time.sleep(sleep_after_idle)
        else:
            stall_passes = 0
        if stall_passes >= 30 and pending_after > 0:
            raise RuntimeError(
                f"single reconcile stalled for {source_id}: proofs={proof_count_after} pending={pending_after}"
            )
    raise RuntimeError(
        f"single reconcile timed out for {source_id}: proofs={len(engine.store.list_proofs(source_id))} "
        f"pending={_pending_finality_count(engine, source_id)}"
    )


def mysql_budget_run(*, denotary: dict[str, Any], target_kib: int, batch_size: int, run_root: Path, mode: str) -> dict[str, Any]:
    cycles = _cycle_count_for_budget(target_kib, batch_size)
    timestamp = utc_stamp()
    adapter_root = run_root / f"mysql-{timestamp}"
    adapter_root.mkdir(parents=True, exist_ok=True)
    state_db = adapter_root / "agent-state.sqlite3"
    proof_dir = adapter_root / "proofs"
    base_row_id = _base_row_id(500_000_000, timestamp)
    base_time = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)

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

    source_id = f"mysql-mainnet-budget-{timestamp.lower()}"
    config = _build_runtime_config(
        denotary=_denotary_for_mode(denotary, mode),
        agent_name=f"denotary-db-agent-{source_id}",
        state_db=state_db,
        proof_dir=proof_dir,
        source={
            "id": source_id,
            "adapter": "mysql",
            "enabled": True,
            "source_instance": "denotary-mysql-mainnet-budget",
            "database_name": "ledger",
            "include": {"ledger": ["invoices"]},
            "backfill_mode": "none",
            "batch_enabled": _batch_enabled_for_mode(mode),
            "batch_size": batch_size,
            "connection": {
                "host": "127.0.0.1",
                "port": 53306,
                "username": "denotary",
                "password": "denotarypw",
                "database": "ledger",
                "connect_timeout": 5,
            },
            "options": {
                "capture_mode": "binlog",
                "watermark_column": "updated_at",
                "commit_timestamp_column": "updated_at",
                "row_limit": batch_size,
                "binlog_server_id": 18101,
                "binlog_start_file": start_file,
                "binlog_start_pos": start_pos,
            },
        },
    )
    config_path = adapter_root / f"agent.{timestamp.lower()}.json"
    config_path.write_text(json.dumps(config, indent=2), encoding="utf-8")
    engine = AgentEngine(load_config(config_path))
    try:
        engine.bootstrap(source_id)
        baseline = engine.run_once()
        if baseline["processed"] != 0 or baseline["failed"] != 0:
            raise RuntimeError(f"mysql baseline was not idle: {baseline}")
        for cycle in range(cycles):
            stamps = HELPERS.datetime_values(base_time + timedelta(minutes=cycle), batch_size)
            with pymysql.connect(host="127.0.0.1", port=53306, user="denotary", password="denotarypw", database="ledger", autocommit=True) as connection:
                with connection.cursor() as cursor:
                    for index, stamp in enumerate(stamps, start=1):
                        cursor.execute(
                            "insert into invoices (id, status, amount, updated_at) values (%s, %s, %s, %s)",
                            (
                                base_row_id + cycle * 1000 + index,
                                "issued" if cycle % 2 == 0 else "paid",
                                Decimal("200.00") + cycle + index,
                                stamp.strftime("%Y-%m-%d %H:%M:%S"),
                            ),
                        )
            result = _drain_cycle(engine=engine, expected_processed=batch_size)
            if result["processed"] != batch_size or result["failed"] != 0:
                raise RuntimeError(f"mysql cycle {cycle} produced unexpected result: {result}")
        if mode == "single":
            _reconcile_single_phase(
                engine=engine,
                source_id=source_id,
                expected_exports=_expected_export_count(cycles=cycles, batch_size=batch_size, mode=mode),
            )
        payload = _finalize_result(
            adapter="mysql",
            capture_mode="binlog",
            target_kib=target_kib,
            batch_size=batch_size,
            cycles=cycles,
            source_id=source_id,
            engine=engine,
        )
        if payload["proof_count"] != _expected_export_count(cycles=cycles, batch_size=batch_size, mode=mode) or payload["dlq_count"] != 0:
            raise RuntimeError(f"mysql mainnet budget run exported unexpected counts: {payload}")
        payload["status"] = "passed"
        payload["config_path"] = str(config_path)
        return payload
    finally:
        engine.close()
        HELPERS.docker_compose(HELPERS.MYSQL_COMPOSE_FILE, "down", "-v")


def mariadb_budget_run(*, denotary: dict[str, Any], target_kib: int, batch_size: int, run_root: Path, mode: str) -> dict[str, Any]:
    cycles = _cycle_count_for_budget(target_kib, batch_size)
    timestamp = utc_stamp()
    adapter_root = run_root / f"mariadb-{timestamp}"
    adapter_root.mkdir(parents=True, exist_ok=True)
    state_db = adapter_root / "agent-state.sqlite3"
    proof_dir = adapter_root / "proofs"
    base_row_id = _base_row_id(600_000_000, timestamp)
    base_time = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)

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

    source_id = f"mariadb-mainnet-budget-{timestamp.lower()}"
    config = _build_runtime_config(
        denotary=_denotary_for_mode(denotary, mode),
        agent_name=f"denotary-db-agent-{source_id}",
        state_db=state_db,
        proof_dir=proof_dir,
        source={
            "id": source_id,
            "adapter": "mariadb",
            "enabled": True,
            "source_instance": "denotary-mariadb-mainnet-budget",
            "database_name": "ledger",
            "include": {"ledger": ["invoices"]},
            "backfill_mode": "none",
            "batch_enabled": _batch_enabled_for_mode(mode),
            "batch_size": batch_size,
            "connection": {
                "host": "127.0.0.1",
                "port": 53307,
                "username": "denotary",
                "password": "denotarypw",
                "database": "ledger",
                "connect_timeout": 5,
            },
            "options": {
                "capture_mode": "binlog",
                "watermark_column": "updated_at",
                "commit_timestamp_column": "updated_at",
                "row_limit": batch_size,
                "binlog_server_id": 18121,
                "binlog_start_file": start_file,
                "binlog_start_pos": start_pos,
            },
        },
    )
    config_path = adapter_root / f"agent.{timestamp.lower()}.json"
    config_path.write_text(json.dumps(config, indent=2), encoding="utf-8")
    engine = AgentEngine(load_config(config_path))
    try:
        engine.bootstrap(source_id)
        baseline = engine.run_once()
        if baseline["processed"] != 0 or baseline["failed"] != 0:
            raise RuntimeError(f"mariadb baseline was not idle: {baseline}")
        for cycle in range(cycles):
            stamps = HELPERS.datetime_values(base_time + timedelta(minutes=cycle), batch_size)
            with pymysql.connect(host="127.0.0.1", port=53307, user="denotary", password="denotarypw", database="ledger", autocommit=True) as connection:
                with connection.cursor() as cursor:
                    for index, stamp in enumerate(stamps, start=1):
                        cursor.execute(
                            "insert into invoices (id, status, amount, updated_at) values (%s, %s, %s, %s)",
                            (
                                base_row_id + cycle * 1000 + index,
                                "issued" if cycle % 2 == 0 else "paid",
                                Decimal("210.00") + cycle + index,
                                stamp.strftime("%Y-%m-%d %H:%M:%S"),
                            ),
                        )
            result = _drain_cycle(engine=engine, expected_processed=batch_size)
            if result["processed"] != batch_size or result["failed"] != 0:
                raise RuntimeError(f"mariadb cycle {cycle} produced unexpected result: {result}")
        if mode == "single":
            _reconcile_single_phase(
                engine=engine,
                source_id=source_id,
                expected_exports=_expected_export_count(cycles=cycles, batch_size=batch_size, mode=mode),
            )
        payload = _finalize_result(
            adapter="mariadb",
            capture_mode="binlog",
            target_kib=target_kib,
            batch_size=batch_size,
            cycles=cycles,
            source_id=source_id,
            engine=engine,
        )
        if payload["proof_count"] != _expected_export_count(cycles=cycles, batch_size=batch_size, mode=mode) or payload["dlq_count"] != 0:
            raise RuntimeError(f"mariadb mainnet budget run exported unexpected counts: {payload}")
        payload["status"] = "passed"
        payload["config_path"] = str(config_path)
        return payload
    finally:
        engine.close()
        HELPERS.docker_compose(HELPERS.MARIADB_COMPOSE_FILE, "down", "-v")


def sqlserver_budget_run(*, denotary: dict[str, Any], target_kib: int, batch_size: int, run_root: Path, mode: str) -> dict[str, Any]:
    cycles = _cycle_count_for_budget(target_kib, batch_size)
    timestamp = utc_stamp()
    adapter_root = run_root / f"sqlserver-{timestamp}"
    adapter_root.mkdir(parents=True, exist_ok=True)
    state_db = adapter_root / "agent-state.sqlite3"
    proof_dir = adapter_root / "proofs"
    base_row_id = _base_row_id(700_000_000, timestamp)
    base_time = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)

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

    source_id = f"sqlserver-mainnet-budget-{timestamp.lower()}"
    config = _build_runtime_config(
        denotary=_denotary_for_mode(denotary, mode),
        agent_name=f"denotary-db-agent-{source_id}",
        state_db=state_db,
        proof_dir=proof_dir,
        source={
            "id": source_id,
            "adapter": "sqlserver",
            "enabled": True,
            "source_instance": "denotary-sqlserver-mainnet-budget",
            "database_name": "ledger",
            "include": {"dbo": ["invoices"]},
            "backfill_mode": "none",
            "batch_enabled": _batch_enabled_for_mode(mode),
            "batch_size": batch_size,
            "connection": {
                "host": "127.0.0.1",
                "port": 51433,
                "username": "sa",
                "password": "StrongP@ssw0rd!",
                "database": "ledger",
            },
            "options": {
                "capture_mode": "change_tracking",
                "watermark_column": "updated_at",
                "commit_timestamp_column": "updated_at",
                "row_limit": batch_size,
            },
        },
    )
    config_path = adapter_root / f"agent.{timestamp.lower()}.json"
    config_path.write_text(json.dumps(config, indent=2), encoding="utf-8")
    engine = AgentEngine(load_config(config_path))
    try:
        engine.bootstrap(source_id)
        baseline = engine.run_once()
        if baseline["processed"] != 0 or baseline["failed"] != 0:
            raise RuntimeError(f"sqlserver baseline was not idle: {baseline}")
        for cycle in range(cycles):
            stamps = HELPERS.datetime_values(base_time + timedelta(minutes=cycle), batch_size)
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
                                base_row_id + cycle * 1000 + index,
                                "issued" if cycle % 2 == 0 else "paid",
                                Decimal("220.00") + cycle + index,
                                stamp,
                            ),
                        )
            finally:
                connection.close()
            result = _drain_cycle(engine=engine, expected_processed=batch_size)
            if result["processed"] != batch_size or result["failed"] != 0:
                raise RuntimeError(f"sqlserver cycle {cycle} produced unexpected result: {result}")
        if mode == "single":
            _reconcile_single_phase(
                engine=engine,
                source_id=source_id,
                expected_exports=_expected_export_count(cycles=cycles, batch_size=batch_size, mode=mode),
            )
        payload = _finalize_result(
            adapter="sqlserver",
            capture_mode="change_tracking",
            target_kib=target_kib,
            batch_size=batch_size,
            cycles=cycles,
            source_id=source_id,
            engine=engine,
        )
        if payload["proof_count"] != _expected_export_count(cycles=cycles, batch_size=batch_size, mode=mode) or payload["dlq_count"] != 0:
            raise RuntimeError(f"sqlserver mainnet budget run exported unexpected counts: {payload}")
        payload["status"] = "passed"
        payload["config_path"] = str(config_path)
        return payload
    finally:
        engine.close()
        HELPERS.docker_compose(HELPERS.SQLSERVER_COMPOSE_FILE, "down", "-v")


def oracle_budget_run(
    *,
    denotary: dict[str, Any],
    target_kib: int,
    batch_size: int,
    run_root: Path,
    mode: str,
    oracle_sleep_after_write: float,
) -> dict[str, Any]:
    cycles = _cycle_count_for_budget(target_kib, batch_size)
    timestamp = utc_stamp()
    adapter_root = run_root / f"oracle-{timestamp}"
    adapter_root.mkdir(parents=True, exist_ok=True)
    proof_dir = adapter_root / "proofs"
    base_row_id = _base_row_id(800_000_000, timestamp)
    base_time = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)

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

    base_source_id = f"oracle-mainnet-budget-{timestamp.lower()}"
    total_deliveries = 0
    total_proofs = 0
    total_dlq = 0
    latest_payload: dict[str, Any] | None = None
    latest_config_path: str | None = None

    try:
        for cycle in range(cycles):
            cycle_source_id = f"{base_source_id}-c{cycle + 1:02d}"
            cycle_state_db = adapter_root / f"agent-state-{cycle + 1:02d}.sqlite3"
            config = _build_runtime_config(
                denotary=_denotary_for_mode(denotary, mode),
                agent_name=f"denotary-db-agent-{cycle_source_id}",
                state_db=cycle_state_db,
                proof_dir=proof_dir,
                source={
                    "id": cycle_source_id,
                    "adapter": "oracle",
                    "enabled": True,
                    "source_instance": "denotary-oracle-mainnet-budget",
                    "database_name": "ledger",
                    "include": {"DENOTARY": ["INVOICES"]},
                    "backfill_mode": "none",
                    "batch_enabled": _batch_enabled_for_mode(mode),
                    "batch_size": batch_size,
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
                    "options": {
                        "capture_mode": "logminer",
                        "watermark_column": "updated_at",
                        "commit_timestamp_column": "updated_at",
                        "row_limit": batch_size,
                    },
                },
            )
            config_path = adapter_root / f"agent.{timestamp.lower()}.c{cycle + 1:02d}.json"
            config_path.write_text(json.dumps(config, indent=2), encoding="utf-8")
            engine = AgentEngine(load_config(config_path))
            try:
                engine.bootstrap(cycle_source_id)
                baseline = engine.run_once()
                if baseline["processed"] != 0 or baseline["failed"] != 0:
                    raise RuntimeError(f"oracle baseline was not idle: {baseline}")

                stamps = HELPERS.datetime_values(base_time + timedelta(minutes=cycle), batch_size)
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
                        cursor.executemany(
                            "insert into invoices (id, status, amount, updated_at) values (:1, :2, :3, :4)",
                            [
                                (
                                    base_row_id + cycle * 1000 + index,
                                    "issued" if cycle % 2 == 0 else "paid",
                                    230 + cycle + index,
                                    stamp,
                                )
                                for index, stamp in enumerate(stamps, start=1)
                            ],
                        )
                    connection.commit()
                finally:
                    connection.close()

                if oracle_sleep_after_write > 0:
                    time.sleep(oracle_sleep_after_write)
                result = _drain_cycle(engine=engine, expected_processed=batch_size)
                if result["processed"] != batch_size or result["failed"] != 0:
                    raise RuntimeError(f"oracle cycle {cycle} produced unexpected result: {result}")
                if mode == "single":
                    _reconcile_single_phase(
                        engine=engine,
                        source_id=cycle_source_id,
                        expected_exports=batch_size,
                    )

                cycle_payload = _finalize_result(
                    adapter="oracle",
                    capture_mode="logminer",
                    target_kib=target_kib,
                    batch_size=batch_size,
                    cycles=1,
                    source_id=cycle_source_id,
                    engine=engine,
                )
                expected_cycle_exports = 1 if mode == "batch" else batch_size
                if cycle_payload["proof_count"] != expected_cycle_exports or cycle_payload["delivery_count"] != expected_cycle_exports or cycle_payload["dlq_count"] != 0:
                    raise RuntimeError(f"oracle cycle {cycle} exported unexpected counts: {cycle_payload}")
                total_deliveries += int(cycle_payload["delivery_count"])
                total_proofs += int(cycle_payload["proof_count"])
                total_dlq += int(cycle_payload["dlq_count"])
                latest_payload = cycle_payload
                latest_config_path = str(config_path)
            finally:
                engine.close()

        if latest_payload is None:
            raise RuntimeError("oracle mainnet budget run did not produce any cycle payloads")

        payload = {
            **latest_payload,
            "cycles": cycles,
            "delivery_count": total_deliveries,
            "proof_count": total_proofs,
            "dlq_count": total_dlq,
            "approx_total_kib": _approx_batch_kib(batch_size) * cycles,
            "status": "passed",
            "config_path": latest_config_path,
        }
        return payload
    finally:
        HELPERS.docker_compose(HELPERS.ORACLE_COMPOSE_FILE, "down", "-v")


def mongodb_budget_run(
    *,
    denotary: dict[str, Any],
    target_kib: int,
    batch_size: int,
    run_root: Path,
    mode: str,
    mongodb_sleep_after_write: float,
) -> dict[str, Any]:
    cycles = _cycle_count_for_budget(target_kib, batch_size)
    timestamp = utc_stamp()
    adapter_root = run_root / f"mongodb-{timestamp}"
    adapter_root.mkdir(parents=True, exist_ok=True)
    state_db = adapter_root / "agent-state.sqlite3"
    proof_dir = adapter_root / "proofs"
    base_time = datetime.now(timezone.utc).replace(microsecond=0)

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

    source_id = f"mongodb-mainnet-budget-{timestamp.lower()}"
    config = _build_runtime_config(
        denotary=_denotary_for_mode(denotary, mode),
        agent_name=f"denotary-db-agent-{source_id}",
        state_db=state_db,
        proof_dir=proof_dir,
        source={
            "id": source_id,
            "adapter": "mongodb",
            "enabled": True,
            "source_instance": "denotary-mongodb-mainnet-budget",
            "database_name": "ledger",
            "include": {"ledger": ["invoices"]},
            "backfill_mode": "none",
            "batch_enabled": _batch_enabled_for_mode(mode),
            "batch_size": batch_size,
            "connection": {"uri": HELPERS.MONGODB_URI},
            "options": {
                "capture_mode": "change_streams",
                "watermark_column": "updated_at",
                "commit_timestamp_column": "updated_at",
                "row_limit": batch_size,
            },
        },
    )
    config_path = adapter_root / f"agent.{timestamp.lower()}.json"
    config_path.write_text(json.dumps(config, indent=2), encoding="utf-8")
    engine = AgentEngine(load_config(config_path))
    try:
        engine.bootstrap(source_id)
        baseline = engine.run_once()
        if baseline["processed"] != 0 or baseline["failed"] != 0:
            raise RuntimeError(f"mongodb baseline was not idle: {baseline}")
        for cycle in range(cycles):
            client = MongoClient(HELPERS.MONGODB_URI, serverSelectionTimeoutMS=5000)
            try:
                db = client["ledger"]
                db["invoices"].insert_many(
                    [
                        {
                            "status": "issued" if cycle % 2 == 0 else "paid",
                            "amount": 240.0 + cycle + index,
                            "updated_at": stamp,
                        }
                        for index, stamp in enumerate(
                            HELPERS.datetime_values(base_time + timedelta(minutes=cycle), batch_size),
                            start=1,
                        )
                    ]
                )
            finally:
                client.close()
            if mongodb_sleep_after_write > 0:
                time.sleep(mongodb_sleep_after_write)
            result = _drain_cycle(engine=engine, expected_processed=batch_size)
            if result["processed"] != batch_size or result["failed"] != 0:
                raise RuntimeError(f"mongodb cycle {cycle} produced unexpected result: {result}")
        if mode == "single":
            _reconcile_single_phase(
                engine=engine,
                source_id=source_id,
                expected_exports=_expected_export_count(cycles=cycles, batch_size=batch_size, mode=mode),
            )
        payload = _finalize_result(
            adapter="mongodb",
            capture_mode="change_streams",
            target_kib=target_kib,
            batch_size=batch_size,
            cycles=cycles,
            source_id=source_id,
            engine=engine,
        )
        if payload["proof_count"] != _expected_export_count(cycles=cycles, batch_size=batch_size, mode=mode) or payload["dlq_count"] != 0:
            raise RuntimeError(f"mongodb mainnet budget run exported unexpected counts: {payload}")
        payload["status"] = "passed"
        payload["config_path"] = str(config_path)
        return payload
    finally:
        engine.close()
        HELPERS.docker_compose(HELPERS.MONGODB_COMPOSE_FILE, "down", "-v")


def build_runners(
    *,
    denotary: dict[str, Any],
    target_kib: int,
    batch_size: int,
    run_root: Path,
    mode: str,
    mongodb_sleep_after_write: float,
    oracle_sleep_after_write: float,
) -> dict[str, Callable[[], dict[str, Any]]]:
    return {
        "mysql": lambda: mysql_budget_run(denotary=denotary, target_kib=target_kib, batch_size=batch_size, run_root=run_root, mode=mode),
        "mariadb": lambda: mariadb_budget_run(denotary=denotary, target_kib=target_kib, batch_size=batch_size, run_root=run_root, mode=mode),
        "sqlserver": lambda: sqlserver_budget_run(denotary=denotary, target_kib=target_kib, batch_size=batch_size, run_root=run_root, mode=mode),
        "oracle": lambda: oracle_budget_run(
            denotary=denotary,
            target_kib=target_kib,
            batch_size=batch_size,
            run_root=run_root,
            mode=mode,
            oracle_sleep_after_write=oracle_sleep_after_write,
        ),
        "mongodb": lambda: mongodb_budget_run(
            denotary=denotary,
            target_kib=target_kib,
            batch_size=batch_size,
            run_root=run_root,
            mode=mode,
            mongodb_sleep_after_write=mongodb_sleep_after_write,
        ),
    }


def main() -> None:
    args = parse_args()
    denotary = _load_base_denotary(Path(args.config))
    _ensure_offchain_services_ready(denotary)
    mode_suffix = "budget" if args.mode == "batch" else "single"
    run_root = (
        Path(args.output_root).resolve()
        if args.output_root
        else (DATA_ROOT / f"wave1-mainnet-{mode_suffix}-{utc_stamp().lower()}").resolve()
    )
    run_root.mkdir(parents=True, exist_ok=True)
    runners = build_runners(
        denotary=denotary,
        target_kib=args.target_kib,
        batch_size=args.batch_size,
        run_root=run_root,
        mode=args.mode,
        mongodb_sleep_after_write=args.mongodb_sleep_after_write,
        oracle_sleep_after_write=args.oracle_sleep_after_write,
    )
    selected = {args.adapter: runners[args.adapter]} if args.adapter else runners
    results: list[dict[str, Any]] = []
    for adapter, runner in selected.items():
        try:
            results.append(runner())
        except Exception as exc:  # noqa: BLE001
            results.append({"adapter": adapter, "status": "failed", "error": str(exc)})
    summary = {
        "mode": args.mode,
        "target_kib_per_adapter": args.target_kib,
        "batch_size": args.batch_size,
        "approx_batch_kib": _approx_batch_kib(args.batch_size),
        "approx_cycles": _cycle_count_for_budget(args.target_kib, args.batch_size),
        "run_root": str(run_root),
        "results": results,
    }
    (run_root / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
