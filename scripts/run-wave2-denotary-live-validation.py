from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator
from uuid import uuid4

from denotary_db_agent.config import load_config
from denotary_db_agent.engine import AgentEngine


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TESTS_ROOT = PROJECT_ROOT / "tests"
sys.path.insert(0, str(TESTS_ROOT))

import importlib.util


def _load_script(module_path: Path, module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


BASE_VALIDATION = _load_script(PROJECT_ROOT / "scripts" / "run-wave2-denotary-validation.py", "wave2_denotary_base")

from cassandra_live_support import (  # type: ignore
    agent_connection_config as cassandra_connection_config,
    cassandra_keyspace,
    create_session,
    unique_table_name as unique_cassandra_table_name,
    wait_for_table_visibility as wait_for_cassandra_table_visibility,
)
from db2_live_support import (  # type: ignore
    agent_connection_config as db2_connection_config,
    create_connection as create_db2_connection,
    db2_schema,
    unique_table_name as unique_db2_table_name,
    wait_for_db2_ready,
    wait_for_table_visibility as wait_for_db2_table_visibility,
)
from elasticsearch_live_support import (  # type: ignore
    agent_connection_config as elasticsearch_connection_config,
    create_client as create_elasticsearch_client,
    wait_for_index_visibility,
)


DATA_ROOT = PROJECT_ROOT / "data"
PYTHON_EXE = Path(sys.executable)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Wave 2 denotary mainnet validation for live Docker-backed adapters.")
    parser.add_argument("--adapter", choices=("elasticsearch", "cassandra", "db2", "all"), default="all")
    parser.add_argument("--output-root", default="", help="Optional persistent run directory.")
    return parser.parse_args()


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _docker_compose(compose_file: Path, *args: str) -> None:
    command = ["docker", "compose", "-f", str(compose_file), *args]
    subprocess.run(command, cwd=str(PROJECT_ROOT), check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


@contextmanager
def _temporary_env(values: dict[str, str]) -> Iterator[None]:
    original = {key: os.environ.get(key) for key in values}
    os.environ.update(values)
    try:
        yield
    finally:
        for key, previous in original.items():
            if previous is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = previous


def _proof_bundle(proof_entry: dict[str, Any]) -> dict[str, Any]:
    export_path = Path(str(proof_entry["export_path"]))
    return json.loads(export_path.read_text(encoding="utf-8"))


def _finalize_result(*, adapter: str, source_id: str, engine: AgentEngine, doctor: dict[str, Any], config_path: Path) -> dict[str, Any]:
    proofs = engine.store.list_proofs(source_id)
    deliveries = engine.store.list_deliveries(source_id)
    dlq = engine.store.list_dlq(source_id)
    if not proofs or not deliveries:
        raise RuntimeError(f"{adapter} denotary run did not export any finalized proof bundle")
    if dlq:
        raise RuntimeError(f"{adapter} denotary run produced DLQ entries: {len(dlq)}")
    proof = proofs[0]
    delivery = deliveries[0]
    payload = _proof_bundle(proof)
    receipt = dict(payload.get("receipt") or {})
    return {
        "adapter": adapter,
        "status": "passed",
        "processed": 1,
        "failed": 0,
        "request_id": str(proof["request_id"]),
        "tx_id": str(delivery["tx_id"] or receipt.get("tx_id") or ""),
        "block_num": int(receipt.get("block_num") or 0),
        "proof_path": proof["export_path"],
        "config_path": str(config_path),
        "broadcast_backend": doctor["signer"]["effective_broadcast_backend"],
        "finality_mode": "finalized_exported",
    }


def _run_single_event(engine: AgentEngine, source_id: str, adapter: str, config_path: Path) -> dict[str, Any]:
    doctor = engine.doctor(source_id)
    if doctor["overall"]["severity"] == "critical":
        raise RuntimeError(f"{adapter} denotary doctor failed: {doctor['errors']}")
    baseline = engine.run_once()
    if baseline["processed"] != 0 or baseline["failed"] != 0:
        raise RuntimeError(f"{adapter} denotary baseline was not idle: {baseline}")
    result = engine.run_once()
    if result["processed"] != 1 or result["failed"] != 0:
        raise RuntimeError(f"{adapter} denotary run did not process exactly one event: {result}")
    return _finalize_result(adapter=adapter, source_id=source_id, engine=engine, doctor=doctor, config_path=config_path)


def _build_storage(temp_dir: Path) -> dict[str, str]:
    return {
        "state_db": str((temp_dir / "state.sqlite3").resolve()),
        "proof_dir": str((temp_dir / "proofs").resolve()),
    }


def _write_config(path: Path, config: dict[str, Any]) -> Path:
    path.write_text(json.dumps(config, indent=2), encoding="utf-8")
    return path


def _wait_for_cassandra(timeout_sec: float = 180.0) -> None:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            cluster, _session = create_session()
            cluster.shutdown()
            return
        except Exception:
            time.sleep(2)
    raise RuntimeError("cassandra did not become ready in time")


def _wait_for_elasticsearch(timeout_sec: float = 120.0) -> None:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            client = create_elasticsearch_client()
            try:
                if client.ping():
                    return
            finally:
                client.close()
        except Exception:
            time.sleep(1)
    raise RuntimeError("elasticsearch did not become ready in time")


def run_elasticsearch_validation(temp_dir: Path) -> dict[str, Any]:
    compose_file = PROJECT_ROOT / "deploy" / "elasticsearch-live" / "docker-compose.yml"
    env = {
        "DENOTARY_ELASTICSEARCH_URL": "http://127.0.0.1:59200",
        "DENOTARY_ELASTICSEARCH_VERIFY_CERTS": "false",
    }
    if temp_dir.exists():
        shutil.rmtree(temp_dir, ignore_errors=True)
    temp_dir.mkdir(parents=True, exist_ok=True)
    state_db = temp_dir / "offchain-state.sqlite3"
    stack = BASE_VALIDATION.OffchainStack(state_db)
    index_name = f"denotary-agent-live-{uuid4().hex[:12]}".lower()
    with _temporary_env(env):
        _docker_compose(compose_file, "down", "-v")
        _docker_compose(compose_file, "up", "-d")
        try:
            _wait_for_elasticsearch()
            client = create_elasticsearch_client()
            try:
                if client.indices.exists(index=index_name):
                    client.indices.delete(index=index_name)
                client.indices.create(
                    index=index_name,
                    mappings={
                        "properties": {
                            "record_id": {"type": "keyword"},
                            "status": {"type": "keyword"},
                            "updated_at": {"type": "date"},
                        }
                    },
                )
                client.indices.refresh(index=index_name)
            finally:
                client.close()
            wait_for_index_visibility(index_name)
            stack.start()
            try:
                config = {
                    "agent_name": "wave2-elasticsearch-denotary-live-validation",
                    "log_level": "INFO",
                    "denotary": BASE_VALIDATION.build_denotary_config_for_stack(stack),
                    "storage": _build_storage(temp_dir),
                    "sources": [
                        {
                            "id": "elasticsearch-wave2-denotary-live",
                            "adapter": "elasticsearch",
                            "enabled": True,
                            "source_instance": "search-denotary-live",
                            "database_name": "search",
                            "include": {"default": [index_name]},
                            "checkpoint_policy": "after_ack",
                            "backfill_mode": "full",
                            "connection": elasticsearch_connection_config(),
                            "options": {
                                "capture_mode": "watermark",
                                "watermark_field": "updated_at",
                                "commit_timestamp_field": "updated_at",
                                "primary_key_field": "record_id",
                                "row_limit": 100,
                            },
                        }
                    ],
                }
                config_path = _write_config(temp_dir / "elasticsearch-denotary-live-config.json", config)
                engine = AgentEngine(load_config(config_path))
                try:
                    doctor = engine.doctor("elasticsearch-wave2-denotary-live")
                    if doctor["overall"]["severity"] == "critical":
                        raise RuntimeError(f"elasticsearch denotary doctor failed: {doctor['errors']}")
                    baseline = engine.run_once()
                    if baseline["processed"] != 0 or baseline["failed"] != 0:
                        raise RuntimeError(f"elasticsearch denotary baseline was not idle: {baseline}")
                    client = create_elasticsearch_client()
                    try:
                        marker = BASE_VALIDATION.build_run_marker("elasticsearch-denotary")
                        client.index(
                            index=index_name,
                            id=str(marker["record_id"]),
                            document={
                                "record_id": str(marker["record_id"]),
                                "status": str(marker["status"]),
                                "updated_at": str(marker["timestamp"]),
                            },
                        )
                        client.indices.refresh(index=index_name)
                    finally:
                        client.close()
                    result = engine.run_once()
                    if result["processed"] != 1 or result["failed"] != 0:
                        raise RuntimeError(f"elasticsearch denotary run did not process exactly one event: {result}")
                    return _finalize_result(
                        adapter="elasticsearch",
                        source_id="elasticsearch-wave2-denotary-live",
                        engine=engine,
                        doctor=doctor,
                        config_path=config_path,
                    )
                finally:
                    engine.close()
            finally:
                stack.stop()
                client = create_elasticsearch_client()
                try:
                    if client.indices.exists(index=index_name):
                        client.indices.delete(index=index_name)
                finally:
                    client.close()
        finally:
            _docker_compose(compose_file, "down", "-v")


def run_cassandra_validation(temp_dir: Path) -> dict[str, Any]:
    compose_file = PROJECT_ROOT / "deploy" / "cassandra-live" / "docker-compose.yml"
    env = {
        "DENOTARY_CASSANDRA_HOST": "127.0.0.1",
        "DENOTARY_CASSANDRA_PORT": "59042",
        "DENOTARY_CASSANDRA_KEYSPACE": "denotary_agent",
    }
    if temp_dir.exists():
        shutil.rmtree(temp_dir, ignore_errors=True)
    temp_dir.mkdir(parents=True, exist_ok=True)
    state_db = temp_dir / "offchain-state.sqlite3"
    stack = BASE_VALIDATION.OffchainStack(state_db)
    table_name = unique_cassandra_table_name()
    with _temporary_env(env):
        _docker_compose(compose_file, "down", "-v")
        _docker_compose(compose_file, "up", "-d")
        try:
            _wait_for_cassandra()
            cluster, session = create_session()
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
            wait_for_cassandra_table_visibility(table_name)
            stack.start()
            try:
                config = {
                    "agent_name": "wave2-cassandra-denotary-live-validation",
                    "log_level": "INFO",
                    "denotary": BASE_VALIDATION.build_denotary_config_for_stack(stack),
                    "storage": _build_storage(temp_dir),
                    "sources": [
                        {
                            "id": "cassandra-wave2-denotary-live",
                            "adapter": "cassandra",
                            "enabled": True,
                            "source_instance": "events-denotary-live",
                            "database_name": cassandra_keyspace(),
                            "include": {cassandra_keyspace(): [table_name]},
                            "checkpoint_policy": "after_ack",
                            "backfill_mode": "full",
                            "connection": cassandra_connection_config(),
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
                config_path = _write_config(temp_dir / "cassandra-denotary-live-config.json", config)
                engine = AgentEngine(load_config(config_path))
                try:
                    doctor = engine.doctor("cassandra-wave2-denotary-live")
                    if doctor["overall"]["severity"] == "critical":
                        raise RuntimeError(f"cassandra denotary doctor failed: {doctor['errors']}")
                    baseline = engine.run_once()
                    if baseline["processed"] != 0 or baseline["failed"] != 0:
                        raise RuntimeError(f"cassandra denotary baseline was not idle: {baseline}")
                    cluster, session = create_session()
                    try:
                        marker = BASE_VALIDATION.build_run_marker("cassandra-denotary")
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
                    result = engine.run_once()
                    if result["processed"] != 1 or result["failed"] != 0:
                        raise RuntimeError(f"cassandra denotary run did not process exactly one event: {result}")
                    return _finalize_result(
                        adapter="cassandra",
                        source_id="cassandra-wave2-denotary-live",
                        engine=engine,
                        doctor=doctor,
                        config_path=config_path,
                    )
                finally:
                    engine.close()
            finally:
                stack.stop()
                cluster, session = create_session()
                try:
                    session.execute(f"drop table if exists denotary_agent.{table_name}")
                finally:
                    cluster.shutdown()
        finally:
            _docker_compose(compose_file, "down", "-v")


def run_db2_validation(temp_dir: Path) -> dict[str, Any]:
    compose_file = PROJECT_ROOT / "deploy" / "db2-live" / "docker-compose.yml"
    env = {
        "DENOTARY_DB2_HOST": "127.0.0.1",
        "DENOTARY_DB2_PORT": "55000",
        "DENOTARY_DB2_USERNAME": "db2inst1",
        "DENOTARY_DB2_PASSWORD": "password",
        "DENOTARY_DB2_DATABASE": "DENOTARY",
        "DENOTARY_DB2_SCHEMA": "DB2INST1",
    }
    if temp_dir.exists():
        shutil.rmtree(temp_dir, ignore_errors=True)
    temp_dir.mkdir(parents=True, exist_ok=True)
    state_db = temp_dir / "offchain-state.sqlite3"
    stack = BASE_VALIDATION.OffchainStack(state_db)
    table_name = unique_db2_table_name()
    with _temporary_env(env):
        _docker_compose(compose_file, "down", "-v")
        _docker_compose(compose_file, "up", "-d")
        try:
            wait_for_db2_ready()
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
            stack.start()
            try:
                config = {
                    "agent_name": "wave2-db2-denotary-live-validation",
                    "log_level": "INFO",
                    "denotary": BASE_VALIDATION.build_denotary_config_for_stack(stack),
                    "storage": _build_storage(temp_dir),
                    "sources": [
                        {
                            "id": "db2-wave2-denotary-live",
                            "adapter": "db2",
                            "enabled": True,
                            "source_instance": "erp-denotary-live",
                            "database_name": db2_connection_config()["database"],
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
                config_path = _write_config(temp_dir / "db2-denotary-live-config.json", config)
                engine = AgentEngine(load_config(config_path))
                try:
                    doctor = engine.doctor("db2-wave2-denotary-live")
                    if doctor["overall"]["severity"] == "critical":
                        raise RuntimeError(f"db2 denotary doctor failed: {doctor['errors']}")
                    baseline = engine.run_once()
                    if baseline["processed"] != 0 or baseline["failed"] != 0:
                        raise RuntimeError(f"db2 denotary baseline was not idle: {baseline}")
                    marker = BASE_VALIDATION.build_run_marker("db2-denotary")
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
                                str(marker["timestamp"]).replace("T", "-").replace("Z", "").replace(":", "."),
                            ),
                        )
                        connection.commit()
                    finally:
                        connection.close()
                    result = engine.run_once()
                    if result["processed"] != 1 or result["failed"] != 0:
                        raise RuntimeError(f"db2 denotary run did not process exactly one event: {result}")
                    return _finalize_result(
                        adapter="db2",
                        source_id="db2-wave2-denotary-live",
                        engine=engine,
                        doctor=doctor,
                        config_path=config_path,
                    )
                finally:
                    engine.close()
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
        finally:
            _docker_compose(compose_file, "down", "-v")


def main() -> None:
    args = parse_args()
    adapters = ["elasticsearch", "cassandra", "db2"] if args.adapter == "all" else [args.adapter]
    run_root = (
        Path(args.output_root).resolve()
        if args.output_root
        else (DATA_ROOT / f"wave2-denotary-live-validation-{utc_stamp().lower()}").resolve()
    )
    run_root.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, Any]] = []
    runners = {
        "elasticsearch": run_elasticsearch_validation,
        "cassandra": run_cassandra_validation,
        "db2": run_db2_validation,
    }
    for adapter in adapters:
        try:
            results.append(runners[adapter](run_root / adapter))
        except Exception as exc:  # noqa: BLE001
            results.append({"adapter": adapter, "status": "failed", "error": str(exc)})
    summary = {
        "network": BASE_VALIDATION.DENOTARY_RPC_URL,
        "submitter": BASE_VALIDATION.SUBMITTER,
        "submitter_permission": BASE_VALIDATION.SUBMITTER_PERMISSION,
        "run_root": str(run_root),
        "results": results,
    }
    (run_root / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
