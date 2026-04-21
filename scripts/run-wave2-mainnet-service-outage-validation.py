from __future__ import annotations

import argparse
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from denotary_db_agent.config import load_config
from denotary_db_agent.engine import AgentEngine


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = PROJECT_ROOT / "data"
BAD_SERVICE_URL = "http://127.0.0.1:1"
SCENARIOS = {
    "ingress": "ingress_url",
    "watcher": "watcher_url",
    "receipt": "receipt_url",
    "audit": "audit_url",
}


def _load_script(module_path: Path, module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


DENOTARY_VALIDATION = _load_script(
    PROJECT_ROOT / "scripts" / "run-wave2-denotary-validation.py",
    "wave2_denotary_validation_service_outage",
)
sys.path.insert(0, str(PROJECT_ROOT / "tests"))

from db2_live_support import (  # type: ignore
    agent_connection_config as db2_connection_config,
    create_connection as create_db2_connection,
    db2_schema,
    unique_table_name as unique_db2_table_name,
    wait_for_db2_ready,
    wait_for_table_visibility as wait_for_db2_table_visibility,
)
from scylladb_live_support import (  # type: ignore
    agent_connection_config as scylladb_connection_config,
    create_session as create_scylladb_session,
    scylladb_keyspace,
    unique_table_name as unique_scylladb_table_name,
    wait_for_table_visibility as wait_for_scylladb_table_visibility,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Wave 2 denotary mainnet service-outage validation.")
    parser.add_argument("--adapter", choices=("sqlite", "redis", "scylladb", "db2", "all"), default="all")
    parser.add_argument("--scenario", choices=sorted(SCENARIOS))
    parser.add_argument("--bad-service-url", default=BAD_SERVICE_URL)
    parser.add_argument("--output-root", default="", help="Optional persistent run directory.")
    return parser.parse_args()


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _load_proof_bundle(proof_entry: dict[str, Any]) -> dict[str, Any]:
    export_path = Path(str(proof_entry["export_path"]))
    return json.loads(export_path.read_text(encoding="utf-8"))


def _docker_compose(compose_file: Path, *args: str) -> None:
    command = ["docker", "compose", "-f", str(compose_file), *args]
    subprocess.run(command, cwd=str(PROJECT_ROOT), check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


class _temporary_env:
    def __init__(self, values: dict[str, str]) -> None:
        self.values = values
        self.original = {key: os.environ.get(key) for key in values}

    def __enter__(self) -> None:
        os.environ.update(self.values)
        return None

    def __exit__(self, exc_type, exc, tb) -> None:
        for key, previous in self.original.items():
            if previous is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = previous


def _wait_for_scylladb(timeout_sec: float = 180.0) -> None:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            cluster, _session = create_scylladb_session()
            cluster.shutdown()
            return
        except Exception:
            time.sleep(2)
    raise RuntimeError("scylladb did not become ready in time")


def _db2_timestamp_literal(value: str) -> str:
    return value.replace("T", "-").replace("Z", "").replace(":", ".")


def _ensure_db2_database(container_name: str = "denotary-db-agent-db2-live", timeout_sec: float = 480.0) -> None:
    deadline = time.time() + timeout_sec
    command = (
        "su - db2inst1 -c \""
        "db2 connect to DENOTARY >/dev/null 2>&1 || "
        "(db2 create db DENOTARY >/dev/null 2>&1 || true); "
        "db2 connect to DENOTARY >/dev/null 2>&1"
        "\""
    )
    while time.time() < deadline:
        result = subprocess.run(
            ["docker", "exec", container_name, "bash", "-lc", command],
            cwd=str(PROJECT_ROOT),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        if result.returncode == 0:
            return
        time.sleep(5)
    raise RuntimeError("db2 database DENOTARY did not become available in time")


def _build_sqlite_config(
    adapter_root: Path,
    database_path: Path,
    stack: Any,
    *,
    source_id: str,
    broken_field: str | None,
    bad_service_url: str,
    suffix: str,
) -> Path:
    denotary = DENOTARY_VALIDATION.build_denotary_config_for_stack(stack)
    if broken_field:
        denotary[broken_field] = bad_service_url
    config = {
        "agent_name": f"wave2-sqlite-denotary-outage-{suffix}",
        "log_level": "INFO",
        "denotary": denotary,
        "storage": {
            "state_db": str((adapter_root / "state.sqlite3").resolve()),
            "proof_dir": str((adapter_root / "proofs").resolve()),
        },
        "sources": [
            {
                "id": source_id,
                "adapter": "sqlite",
                "enabled": True,
                "source_instance": "edge-device-denotary-outage",
                "database_name": "ledger",
                "include": {"main": ["invoices"]},
                "checkpoint_policy": "after_ack",
                "backfill_mode": "full",
                "connection": {"path": str(database_path.resolve())},
                "options": {
                    "capture_mode": "watermark",
                    "watermark_column": "updated_at",
                    "commit_timestamp_column": "updated_at",
                    "primary_key_columns": ["id"],
                    "row_limit": 100,
                },
            }
        ],
    }
    path = adapter_root / f"sqlite-{suffix}.json"
    path.write_text(json.dumps(config, indent=2), encoding="utf-8")
    return path


def _build_redis_config(
    adapter_root: Path,
    stack: Any,
    *,
    source_id: str,
    broken_field: str | None,
    bad_service_url: str,
    suffix: str,
) -> Path:
    denotary = DENOTARY_VALIDATION.build_denotary_config_for_stack(stack)
    if broken_field:
        denotary[broken_field] = bad_service_url
    config = {
        "agent_name": f"wave2-redis-denotary-outage-{suffix}",
        "log_level": "INFO",
        "denotary": denotary,
        "storage": {
            "state_db": str((adapter_root / "state.sqlite3").resolve()),
            "proof_dir": str((adapter_root / "proofs").resolve()),
        },
        "sources": [
            {
                "id": source_id,
                "adapter": "redis",
                "enabled": True,
                "source_instance": "cache-denotary-outage",
                "database_name": "db0",
                "include": {"0": ["orders:*"]},
                "checkpoint_policy": "after_ack",
                "backfill_mode": "full",
                "connection": {"host": "127.0.0.1", "port": DENOTARY_VALIDATION.REDIS_PORT},
                "options": {
                    "capture_mode": "scan",
                    "row_limit": 100,
                    "scan_count": 100,
                },
            }
        ],
    }
    path = adapter_root / f"redis-{suffix}.json"
    path.write_text(json.dumps(config, indent=2), encoding="utf-8")
    return path


def _build_scylladb_config(
    adapter_root: Path,
    stack: Any,
    *,
    source_id: str,
    table_name: str,
    broken_field: str | None,
    bad_service_url: str,
    suffix: str,
) -> Path:
    denotary = DENOTARY_VALIDATION.build_denotary_config_for_stack(stack)
    if broken_field:
        denotary[broken_field] = bad_service_url
    config = {
        "agent_name": f"wave2-scylladb-denotary-outage-{suffix}",
        "log_level": "INFO",
        "denotary": denotary,
        "storage": {
            "state_db": str((adapter_root / "state.sqlite3").resolve()),
            "proof_dir": str((adapter_root / "proofs").resolve()),
        },
        "sources": [
            {
                "id": source_id,
                "adapter": "scylladb",
                "enabled": True,
                "source_instance": "cluster-denotary-outage",
                "database_name": scylladb_keyspace(),
                "include": {scylladb_keyspace(): [table_name]},
                "checkpoint_policy": "after_ack",
                "backfill_mode": "full",
                "connection": scylladb_connection_config(),
                "options": {
                    "capture_mode": "watermark",
                    "watermark_column": "updated_at",
                    "commit_timestamp_column": "updated_at",
                    "primary_key_columns": ["id"],
                    "row_limit": 100,
                },
            }
        ],
    }
    path = adapter_root / f"scylladb-{suffix}.json"
    path.write_text(json.dumps(config, indent=2), encoding="utf-8")
    return path


def _build_db2_config(
    adapter_root: Path,
    stack: Any,
    *,
    source_id: str,
    table_name: str,
    broken_field: str | None,
    bad_service_url: str,
    suffix: str,
) -> Path:
    denotary = DENOTARY_VALIDATION.build_denotary_config_for_stack(stack)
    if broken_field:
        denotary[broken_field] = bad_service_url
    config = {
        "agent_name": f"wave2-db2-denotary-outage-{suffix}",
        "log_level": "INFO",
        "denotary": denotary,
        "storage": {
            "state_db": str((adapter_root / "state.sqlite3").resolve()),
            "proof_dir": str((adapter_root / "proofs").resolve()),
        },
        "sources": [
            {
                "id": source_id,
                "adapter": "db2",
                "enabled": True,
                "source_instance": "db2-denotary-outage",
                "database_name": str(db2_connection_config()["database"]),
                "include": {db2_schema(): [table_name]},
                "checkpoint_policy": "after_ack",
                "backfill_mode": "full",
                "connection": db2_connection_config(),
                "options": {
                    "capture_mode": "watermark",
                    "watermark_column": "UPDATED_AT",
                    "commit_timestamp_column": "UPDATED_AT",
                    "primary_key_columns": ["ID"],
                    "row_limit": 100,
                },
            }
        ],
    }
    path = adapter_root / f"db2-{suffix}.json"
    path.write_text(json.dumps(config, indent=2), encoding="utf-8")
    return path


def run_sqlite_scenario(
    *,
    adapter_root: Path,
    scenario: str,
    broken_field: str,
    bad_service_url: str,
) -> dict[str, Any]:
    if adapter_root.exists():
        shutil.rmtree(adapter_root, ignore_errors=True)
    adapter_root.mkdir(parents=True, exist_ok=True)
    database_path = adapter_root / "ledger.sqlite3"
    state_db = adapter_root / "offchain-state.sqlite3"
    source_id = "sqlite-wave2-denotary-outage"
    DENOTARY_VALIDATION.initialize_sqlite(database_path)
    stack = DENOTARY_VALIDATION.OffchainStack(state_db)
    stack.start()
    try:
        broken_config_path = _build_sqlite_config(
            adapter_root,
            database_path,
            stack,
            source_id=source_id,
            broken_field=broken_field,
            bad_service_url=bad_service_url,
            suffix=f"{scenario}-broken",
        )
        healthy_config_path = _build_sqlite_config(
            adapter_root,
            database_path,
            stack,
            source_id=source_id,
            broken_field=None,
            bad_service_url=bad_service_url,
            suffix=f"{scenario}-healthy",
        )
        marker = DENOTARY_VALIDATION.build_run_marker(f"sqlite-{scenario}")
        broken_engine = AgentEngine(load_config(broken_config_path))
        try:
            baseline = broken_engine.run_once()
            DENOTARY_VALIDATION.insert_sqlite_invoice(
                database_path,
                record_id=int(marker["record_id"]),
                status=str(marker["status"]),
                updated_at=str(marker["timestamp"]),
            )
            first = broken_engine.run_once()
            first_deliveries = broken_engine.store.list_deliveries(source_id)
            first_proofs = broken_engine.store.list_proofs(source_id)
            first_dlq = broken_engine.store.list_dlq(source_id)
        finally:
            broken_engine.close()

        healthy_engine = AgentEngine(load_config(healthy_config_path))
        try:
            second = healthy_engine.run_once()
            deliveries = healthy_engine.store.list_deliveries(source_id)
            proofs = healthy_engine.store.list_proofs(source_id)
            dlq = healthy_engine.store.list_dlq(source_id)
        finally:
            healthy_engine.close()

        if int(baseline.get("processed") or 0) != 0 or int(baseline.get("failed") or 0) != 0:
            raise RuntimeError(f"sqlite {scenario} baseline was not idle: {baseline}")
        if int(first.get("processed") or 0) != 0 or int(first.get("failed") or 0) != 1:
            raise RuntimeError(f"sqlite {scenario} first run did not fail as expected: {first}")
        if int(second.get("processed") or 0) != 1 or int(second.get("failed") or 0) != 0:
            raise RuntimeError(f"sqlite {scenario} recovery run did not succeed as expected: {second}")
        if not proofs:
            raise RuntimeError(f"sqlite {scenario} did not export a proof after recovery")
        latest_proof = proofs[0]
        proof_payload = _load_proof_bundle(latest_proof)
        return {
            "adapter": "sqlite",
            "scenario": scenario,
            "failed_component": broken_field,
            "status": "passed",
            "baseline_processed": baseline["processed"],
            "first_run": first,
            "second_run": second,
            "first_delivery_count": len(first_deliveries),
            "first_proof_count": len(first_proofs),
            "first_dlq_count": len(first_dlq),
            "delivery_count": len(deliveries),
            "proof_count": len(proofs),
            "dlq_count": len(dlq),
            "request_id": latest_proof["request_id"],
            "tx_id": proof_payload["receipt"]["tx_id"],
            "block_num": proof_payload["receipt"]["block_num"],
            "proof_path": latest_proof["export_path"],
            "broadcast_backend": "private_key_env",
            "finality_mode": "finalized_exported",
        }
    finally:
        stack.stop()


def run_redis_scenario(
    *,
    adapter_root: Path,
    scenario: str,
    broken_field: str,
    bad_service_url: str,
) -> dict[str, Any]:
    if adapter_root.exists():
        shutil.rmtree(adapter_root, ignore_errors=True)
    adapter_root.mkdir(parents=True, exist_ok=True)
    state_db = adapter_root / "offchain-state.sqlite3"
    source_id = "redis-wave2-denotary-outage"
    DENOTARY_VALIDATION.run_redis_compose("down", "-v")
    DENOTARY_VALIDATION.run_redis_compose("up", "-d")
    DENOTARY_VALIDATION.wait_for_redis()
    DENOTARY_VALIDATION.flush_redis()
    stack = DENOTARY_VALIDATION.OffchainStack(state_db)
    stack.start()
    try:
        broken_config_path = _build_redis_config(
            adapter_root,
            stack,
            source_id=source_id,
            broken_field=broken_field,
            bad_service_url=bad_service_url,
            suffix=f"{scenario}-broken",
        )
        healthy_config_path = _build_redis_config(
            adapter_root,
            stack,
            source_id=source_id,
            broken_field=None,
            bad_service_url=bad_service_url,
            suffix=f"{scenario}-healthy",
        )
        marker = DENOTARY_VALIDATION.build_run_marker("orders")
        broken_engine = AgentEngine(load_config(broken_config_path))
        try:
            baseline = broken_engine.run_once()
            DENOTARY_VALIDATION.write_redis_key(str(marker["key"]), f"{marker['value']}-{scenario}")
            first = broken_engine.run_once()
            first_deliveries = broken_engine.store.list_deliveries(source_id)
            first_proofs = broken_engine.store.list_proofs(source_id)
            first_dlq = broken_engine.store.list_dlq(source_id)
        finally:
            broken_engine.close()

        healthy_engine = AgentEngine(load_config(healthy_config_path))
        try:
            second = healthy_engine.run_once()
            deliveries = healthy_engine.store.list_deliveries(source_id)
            proofs = healthy_engine.store.list_proofs(source_id)
            dlq = healthy_engine.store.list_dlq(source_id)
        finally:
            healthy_engine.close()

        if int(baseline.get("processed") or 0) != 0 or int(baseline.get("failed") or 0) != 0:
            raise RuntimeError(f"redis {scenario} baseline was not idle: {baseline}")
        if int(first.get("processed") or 0) != 0 or int(first.get("failed") or 0) != 1:
            raise RuntimeError(f"redis {scenario} first run did not fail as expected: {first}")
        if int(second.get("processed") or 0) != 1 or int(second.get("failed") or 0) != 0:
            raise RuntimeError(f"redis {scenario} recovery run did not succeed as expected: {second}")
        if not proofs:
            raise RuntimeError(f"redis {scenario} did not export a proof after recovery")
        latest_proof = proofs[0]
        proof_payload = _load_proof_bundle(latest_proof)
        return {
            "adapter": "redis",
            "scenario": scenario,
            "failed_component": broken_field,
            "status": "passed",
            "baseline_processed": baseline["processed"],
            "first_run": first,
            "second_run": second,
            "first_delivery_count": len(first_deliveries),
            "first_proof_count": len(first_proofs),
            "first_dlq_count": len(first_dlq),
            "delivery_count": len(deliveries),
            "proof_count": len(proofs),
            "dlq_count": len(dlq),
            "request_id": latest_proof["request_id"],
            "tx_id": proof_payload["receipt"]["tx_id"],
            "block_num": proof_payload["receipt"]["block_num"],
            "proof_path": latest_proof["export_path"],
            "broadcast_backend": "private_key_env",
            "finality_mode": "finalized_exported",
        }
    finally:
        stack.stop()
        DENOTARY_VALIDATION.run_redis_compose("down", "-v")


def run_scylladb_scenario(
    *,
    adapter_root: Path,
    scenario: str,
    broken_field: str,
    bad_service_url: str,
) -> dict[str, Any]:
    if adapter_root.exists():
        shutil.rmtree(adapter_root, ignore_errors=True)
    adapter_root.mkdir(parents=True, exist_ok=True)
    compose_file = PROJECT_ROOT / "deploy" / "scylladb-live" / "docker-compose.yml"
    env = {
        "DENOTARY_SCYLLADB_HOST": "127.0.0.1",
        "DENOTARY_SCYLLADB_PORT": "59043",
        "DENOTARY_SCYLLADB_KEYSPACE": "denotary_agent",
    }
    state_db = adapter_root / "offchain-state.sqlite3"
    source_id = "scylladb-wave2-denotary-outage"
    table_name = unique_scylladb_table_name(f"wave2_outage_{scenario}_{uuid4().hex[:6].lower()}")
    with _temporary_env(env):
        _docker_compose(compose_file, "down", "-v")
        _docker_compose(compose_file, "up", "-d")
        try:
            _wait_for_scylladb()
            cluster, session = create_scylladb_session()
            try:
                session.execute(
                    """
                    create keyspace if not exists denotary_agent
                    with replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
                    """
                )
                session.execute(
                    f"""
                    create table denotary_agent.{table_name} (
                        id int,
                        status text,
                        updated_at timestamp,
                        primary key (id)
                    )
                    """
                )
            finally:
                cluster.shutdown()
            wait_for_scylladb_table_visibility(table_name)
            stack = DENOTARY_VALIDATION.OffchainStack(state_db)
            stack.start()
            try:
                broken_config_path = _build_scylladb_config(
                    adapter_root,
                    stack,
                    source_id=source_id,
                    table_name=table_name,
                    broken_field=broken_field,
                    bad_service_url=bad_service_url,
                    suffix=f"{scenario}-broken",
                )
                healthy_config_path = _build_scylladb_config(
                    adapter_root,
                    stack,
                    source_id=source_id,
                    table_name=table_name,
                    broken_field=None,
                    bad_service_url=bad_service_url,
                    suffix=f"{scenario}-healthy",
                )
                marker = DENOTARY_VALIDATION.build_run_marker(f"scylladb-{scenario}")
                broken_engine = AgentEngine(load_config(broken_config_path))
                try:
                    baseline = broken_engine.run_once()
                    cluster, session = create_scylladb_session()
                    try:
                        session.execute(
                            f"""
                            insert into denotary_agent.{table_name} (id, status, updated_at)
                            values (%s, %s, %s)
                            """,
                            (
                                int(marker["record_id"]),
                                str(marker["status"]),
                                str(marker["timestamp"]),
                            ),
                        )
                    finally:
                        cluster.shutdown()
                    first = broken_engine.run_once()
                    first_deliveries = broken_engine.store.list_deliveries(source_id)
                    first_proofs = broken_engine.store.list_proofs(source_id)
                    first_dlq = broken_engine.store.list_dlq(source_id)
                finally:
                    broken_engine.close()

                healthy_engine = AgentEngine(load_config(healthy_config_path))
                try:
                    second = healthy_engine.run_once()
                    deliveries = healthy_engine.store.list_deliveries(source_id)
                    proofs = healthy_engine.store.list_proofs(source_id)
                    dlq = healthy_engine.store.list_dlq(source_id)
                finally:
                    healthy_engine.close()

                if int(baseline.get("processed") or 0) != 0 or int(baseline.get("failed") or 0) != 0:
                    raise RuntimeError(f"scylladb {scenario} baseline was not idle: {baseline}")
                if int(first.get("processed") or 0) != 0 or int(first.get("failed") or 0) != 1:
                    raise RuntimeError(f"scylladb {scenario} first run did not fail as expected: {first}")
                if int(second.get("processed") or 0) != 1 or int(second.get("failed") or 0) != 0:
                    raise RuntimeError(f"scylladb {scenario} recovery run did not succeed as expected: {second}")
                if not proofs:
                    raise RuntimeError(f"scylladb {scenario} did not export a proof after recovery")
                latest_proof = proofs[0]
                proof_payload = _load_proof_bundle(latest_proof)
                return {
                    "adapter": "scylladb",
                    "scenario": scenario,
                    "failed_component": broken_field,
                    "status": "passed",
                    "baseline_processed": baseline["processed"],
                    "first_run": first,
                    "second_run": second,
                    "first_delivery_count": len(first_deliveries),
                    "first_proof_count": len(first_proofs),
                    "first_dlq_count": len(first_dlq),
                    "delivery_count": len(deliveries),
                    "proof_count": len(proofs),
                    "dlq_count": len(dlq),
                    "request_id": latest_proof["request_id"],
                    "tx_id": proof_payload["receipt"]["tx_id"],
                    "block_num": proof_payload["receipt"]["block_num"],
                    "proof_path": latest_proof["export_path"],
                    "broadcast_backend": "private_key_env",
                    "finality_mode": "finalized_exported",
                }
            finally:
                stack.stop()
                cluster, session = create_scylladb_session()
                try:
                    session.execute(f"drop table if exists denotary_agent.{table_name}")
                finally:
                    cluster.shutdown()
        finally:
            _docker_compose(compose_file, "down", "-v")


def run_db2_scenario(
    *,
    adapter_root: Path,
    scenario: str,
    broken_field: str,
    bad_service_url: str,
) -> dict[str, Any]:
    compose_file = PROJECT_ROOT / "deploy" / "db2-live" / "docker-compose.yml"
    env = {
        "DENOTARY_DB2_HOST": "127.0.0.1",
        "DENOTARY_DB2_PORT": "55000",
        "DENOTARY_DB2_USERNAME": "db2inst1",
        "DENOTARY_DB2_PASSWORD": "password",
        "DENOTARY_DB2_DATABASE": "DENOTARY",
        "DENOTARY_DB2_SCHEMA": "DB2INST1",
    }
    with _temporary_env(env):
        _docker_compose(compose_file, "down", "-v")
        _docker_compose(compose_file, "up", "-d")
        try:
            _ensure_db2_database()
            wait_for_db2_ready(timeout_sec=720.0, consecutive_successes=3, success_interval_sec=5.0)
            return _run_db2_scenario_ready(
                adapter_root=adapter_root,
                scenario=scenario,
                broken_field=broken_field,
                bad_service_url=bad_service_url,
            )
        finally:
            _docker_compose(compose_file, "down", "-v")


def _run_db2_scenario_ready(
    *,
    adapter_root: Path,
    scenario: str,
    broken_field: str,
    bad_service_url: str,
) -> dict[str, Any]:
    if adapter_root.exists():
        shutil.rmtree(adapter_root, ignore_errors=True)
    adapter_root.mkdir(parents=True, exist_ok=True)
    state_db = adapter_root / "offchain-state.sqlite3"
    source_id = "db2-wave2-denotary-outage"
    table_name = unique_db2_table_name(f"DENOTARY_OUTAGE_{uuid4().hex[:6].upper()}")
    connection = create_db2_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                create table {db2_schema()}.{table_name} (
                    ID integer not null,
                    STATUS varchar(64),
                    UPDATED_AT timestamp,
                    primary key (ID)
                )
                """
            )
        connection.commit()
    finally:
        connection.close()
    wait_for_db2_table_visibility(table_name)
    stack = DENOTARY_VALIDATION.OffchainStack(state_db)
    stack.start()
    try:
        broken_config_path = _build_db2_config(
            adapter_root,
            stack,
            source_id=source_id,
            table_name=table_name,
            broken_field=broken_field,
            bad_service_url=bad_service_url,
            suffix=f"{scenario}-broken",
        )
        healthy_config_path = _build_db2_config(
            adapter_root,
            stack,
            source_id=source_id,
            table_name=table_name,
            broken_field=None,
            bad_service_url=bad_service_url,
            suffix=f"{scenario}-healthy",
        )
        marker = DENOTARY_VALIDATION.build_run_marker(f"db2-{scenario}")
        broken_engine = AgentEngine(load_config(broken_config_path))
        try:
            baseline = broken_engine.run_once()
            connection = create_db2_connection()
            try:
                with connection.cursor() as cursor:
                    cursor.execute(
                        f"""
                        insert into {db2_schema()}.{table_name} (ID, STATUS, UPDATED_AT)
                        values (?, ?, ?)
                        """,
                        (
                            int(marker["record_id"]),
                            str(marker["status"]),
                            _db2_timestamp_literal(str(marker["timestamp"])),
                        ),
                    )
                connection.commit()
            finally:
                connection.close()
            first = broken_engine.run_once()
            first_deliveries = broken_engine.store.list_deliveries(source_id)
            first_proofs = broken_engine.store.list_proofs(source_id)
            first_dlq = broken_engine.store.list_dlq(source_id)
        finally:
            broken_engine.close()

        healthy_engine = AgentEngine(load_config(healthy_config_path))
        try:
            second = healthy_engine.run_once()
            deliveries = healthy_engine.store.list_deliveries(source_id)
            proofs = healthy_engine.store.list_proofs(source_id)
            dlq = healthy_engine.store.list_dlq(source_id)
        finally:
            healthy_engine.close()

        if int(baseline.get("processed") or 0) != 0 or int(baseline.get("failed") or 0) != 0:
            raise RuntimeError(f"db2 {scenario} baseline was not idle: {baseline}")
        if int(first.get("processed") or 0) != 0 or int(first.get("failed") or 0) != 1:
            raise RuntimeError(f"db2 {scenario} first run did not fail as expected: {first}")
        if int(second.get("processed") or 0) != 1 or int(second.get("failed") or 0) != 0:
            raise RuntimeError(f"db2 {scenario} recovery run did not succeed as expected: {second}")
        if not proofs:
            raise RuntimeError(f"db2 {scenario} did not export a proof after recovery")
        latest_proof = proofs[0]
        proof_payload = _load_proof_bundle(latest_proof)
        return {
            "adapter": "db2",
            "scenario": scenario,
            "failed_component": broken_field,
            "status": "passed",
            "baseline_processed": baseline["processed"],
            "first_run": first,
            "second_run": second,
            "first_delivery_count": len(first_deliveries),
            "first_proof_count": len(first_proofs),
            "first_dlq_count": len(first_dlq),
            "delivery_count": len(deliveries),
            "proof_count": len(proofs),
            "dlq_count": len(dlq),
            "request_id": latest_proof["request_id"],
            "tx_id": proof_payload["receipt"]["tx_id"],
            "block_num": proof_payload["receipt"]["block_num"],
            "proof_path": latest_proof["export_path"],
            "broadcast_backend": "private_key_env",
            "finality_mode": "finalized_exported",
        }
    finally:
        stack.stop()
        connection = create_db2_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"drop table {db2_schema()}.{table_name}")
            connection.commit()
        except Exception:
            connection.rollback()
        finally:
            connection.close()


def main() -> None:
    args = parse_args()
    adapters = ["sqlite", "redis", "scylladb", "db2"] if args.adapter == "all" else [args.adapter]
    scenarios = [args.scenario] if args.scenario else list(SCENARIOS)
    run_root = (
        Path(args.output_root).resolve()
        if args.output_root
        else (DATA_ROOT / f"wave2-mainnet-service-outage-{utc_stamp().lower()}").resolve()
    )
    run_root.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, Any]] = []
    for adapter in adapters:
        if adapter == "db2" and len(scenarios) > 1:
            compose_file = PROJECT_ROOT / "deploy" / "db2-live" / "docker-compose.yml"
            env = {
                "DENOTARY_DB2_HOST": "127.0.0.1",
                "DENOTARY_DB2_PORT": "55000",
                "DENOTARY_DB2_USERNAME": "db2inst1",
                "DENOTARY_DB2_PASSWORD": "password",
                "DENOTARY_DB2_DATABASE": "DENOTARY",
                "DENOTARY_DB2_SCHEMA": "DB2INST1",
            }
            with _temporary_env(env):
                _docker_compose(compose_file, "down", "-v")
                _docker_compose(compose_file, "up", "-d")
                try:
                    _ensure_db2_database()
                    wait_for_db2_ready(timeout_sec=720.0, consecutive_successes=3, success_interval_sec=5.0)
                    for scenario in scenarios:
                        try:
                            broken_field = SCENARIOS[scenario]
                            adapter_root = run_root / adapter / scenario
                            results.append(
                                _run_db2_scenario_ready(
                                    adapter_root=adapter_root,
                                    scenario=scenario,
                                    broken_field=broken_field,
                                    bad_service_url=args.bad_service_url,
                                )
                            )
                        except Exception as exc:  # noqa: BLE001
                            results.append({"adapter": adapter, "scenario": scenario, "status": "failed", "error": str(exc)})
                finally:
                    _docker_compose(compose_file, "down", "-v")
            continue
        for scenario in scenarios:
            try:
                broken_field = SCENARIOS[scenario]
                adapter_root = run_root / adapter / scenario
                if adapter == "sqlite":
                    results.append(
                        run_sqlite_scenario(
                            adapter_root=adapter_root,
                            scenario=scenario,
                            broken_field=broken_field,
                            bad_service_url=args.bad_service_url,
                        )
                    )
                elif adapter == "redis":
                    results.append(
                        run_redis_scenario(
                            adapter_root=adapter_root,
                            scenario=scenario,
                            broken_field=broken_field,
                            bad_service_url=args.bad_service_url,
                        )
                    )
                elif adapter == "scylladb":
                    results.append(
                        run_scylladb_scenario(
                            adapter_root=adapter_root,
                            scenario=scenario,
                            broken_field=broken_field,
                            bad_service_url=args.bad_service_url,
                        )
                    )
                else:
                    results.append(
                        run_db2_scenario(
                            adapter_root=adapter_root,
                            scenario=scenario,
                            broken_field=broken_field,
                            bad_service_url=args.bad_service_url,
                        )
                    )
            except Exception as exc:  # noqa: BLE001
                results.append({"adapter": adapter, "scenario": scenario, "status": "failed", "error": str(exc)})
    summary = {
        "network": "denotary",
        "rpc_url": DENOTARY_VALIDATION.DENOTARY_RPC_URL,
        "submitter": DENOTARY_VALIDATION.SUBMITTER,
        "submitter_permission": DENOTARY_VALIDATION.SUBMITTER_PERMISSION,
        "run_root": str(run_root),
        "results": results,
    }
    (run_root / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
