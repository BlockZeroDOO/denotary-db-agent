from __future__ import annotations

import argparse
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from denotary_db_agent.config import load_config
from denotary_db_agent.engine import AgentEngine


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TESTS_ROOT = PROJECT_ROOT / "tests"
DB2_ENV = {
    "DENOTARY_DB2_HOST": "127.0.0.1",
    "DENOTARY_DB2_PORT": "55000",
    "DENOTARY_DB2_USERNAME": "db2inst1",
    "DENOTARY_DB2_PASSWORD": "password",
    "DENOTARY_DB2_DATABASE": "DENOTARY",
    "DENOTARY_DB2_SCHEMA": "DB2INST1",
}


def _load_module(module_path: Path, module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


FULL_CYCLE = _load_module(TESTS_ROOT / "test_db2_full_cycle.py", "wave2_db2_full_cycle")
sys.path.insert(0, str(TESTS_ROOT))

from db2_live_support import (  # type: ignore
    agent_connection_config,
    create_connection,
    db2_schema,
    wait_for_db2_ready,
    wait_for_table_visibility,
)


class IncrementingIngressHandler(FULL_CYCLE.MockIngressHandler):
    counter = 0

    def do_POST(self) -> None:  # noqa: N802
        IncrementingIngressHandler.counter += 1
        body = self.rfile.read(int(self.headers.get("Content-Length", "0"))).decode("utf-8")
        payload = json.loads(body)
        response = {
            "request_id": f"wave2-db2-request-{IncrementingIngressHandler.counter}",
            "trace_id": f"wave2-db2-trace-{IncrementingIngressHandler.counter}",
            "external_ref_hash": f"{IncrementingIngressHandler.counter:064x}",
            "object_hash": f"{IncrementingIngressHandler.counter + 1000:064x}",
            "verification_account": "verif",
            "prepared_action": {
                "contract": "verifbill",
                "action": "submit",
                "data": {
                    "payer": payload["submitter"],
                    "submitter": payload["submitter"],
                    "schema_id": 1,
                    "policy_id": 1,
                    "object_hash": f"{IncrementingIngressHandler.counter + 1000:064x}",
                    "external_ref": f"{IncrementingIngressHandler.counter:064x}",
                },
            },
        }
        encoded = json.dumps(response).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


class MockServiceStack:
    def __init__(self) -> None:
        self.ingress_port = FULL_CYCLE.free_port()
        self.chain_port = FULL_CYCLE.free_port()
        self.watcher_port = FULL_CYCLE.free_port()
        self.receipt_port = FULL_CYCLE.free_port()
        self.audit_port = FULL_CYCLE.free_port()
        FULL_CYCLE.MockState.requests = {}
        IncrementingIngressHandler.counter = 0
        self.servers = [
            FULL_CYCLE.ThreadingHTTPServer(("127.0.0.1", self.ingress_port), IncrementingIngressHandler),
            FULL_CYCLE.ThreadingHTTPServer(("127.0.0.1", self.chain_port), FULL_CYCLE.MockChainHandler),
            FULL_CYCLE.ThreadingHTTPServer(("127.0.0.1", self.watcher_port), FULL_CYCLE.MockWatcherHandler),
            FULL_CYCLE.ThreadingHTTPServer(("127.0.0.1", self.receipt_port), FULL_CYCLE.MockReceiptHandler),
            FULL_CYCLE.ThreadingHTTPServer(("127.0.0.1", self.audit_port), FULL_CYCLE.MockAuditHandler),
        ]
        self.threads = [threading.Thread(target=server.serve_forever, daemon=True) for server in self.servers]

    def start(self) -> None:
        for thread in self.threads:
            thread.start()

    def stop(self) -> None:
        for server in self.servers:
            server.shutdown()
            server.server_close()
        for thread in self.threads:
            thread.join(timeout=2)


def _docker_compose(*args: str) -> None:
    compose_file = PROJECT_ROOT / "deploy" / "db2-live" / "docker-compose.yml"
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


def _ensure_db2_database(container_name: str = "denotary-db-agent-db2-live", timeout_sec: float = 900.0) -> None:
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


def _qualified_table(table_name: str) -> str:
    return f"{db2_schema()}.{table_name}"


def _db2_timestamp_literal(value: str) -> str:
    return value.replace("T", "-").replace("Z", "").replace(":", ".")


def _create_table(table_name: str) -> None:
    connection = create_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                create table {_qualified_table(table_name)} (
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
    wait_for_table_visibility(table_name)


def _drop_table(table_name: str) -> None:
    connection = create_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"drop table {_qualified_table(table_name)}")
        connection.commit()
    except Exception:
        connection.rollback()
    finally:
        connection.close()


def _insert_row(table_name: str, *, record_id: int, status: str, updated_at: str) -> None:
    connection = create_connection()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                insert into {_qualified_table(table_name)} (ID, STATUS, UPDATED_AT)
                values (?, ?, ?)
                """,
                (record_id, status, _db2_timestamp_literal(updated_at)),
            )
        connection.commit()
    finally:
        connection.close()


def build_config(temp_dir: Path, stack: MockServiceStack, *, source_id: str, table_name: str) -> Path:
    config = {
        "agent_name": f"{source_id}-validation",
        "log_level": "INFO",
        "denotary": {
            "ingress_url": f"http://127.0.0.1:{stack.ingress_port}",
            "watcher_url": f"http://127.0.0.1:{stack.watcher_port}",
            "watcher_auth_token": "token",
            "receipt_url": f"http://127.0.0.1:{stack.receipt_port}",
            "audit_url": f"http://127.0.0.1:{stack.audit_port}",
            "chain_rpc_url": f"http://127.0.0.1:{stack.chain_port}",
            "submitter": "dbagentstest",
            "submitter_permission": "dnanchor",
            "submitter_private_key": "5HpHagT65TZzG1PH3CSu63k8DbpvD8s5ip4nEB3kEsreAnchuDf",
            "schema_id": 1,
            "policy_id": 1,
            "wait_for_finality": True,
            "finality_timeout_sec": 5,
            "finality_poll_interval_sec": 0.01,
        },
        "storage": {
            "state_db": str((temp_dir / "state.sqlite3").resolve()),
            "proof_dir": str((temp_dir / "proofs").resolve()),
        },
        "sources": [
            {
                "id": source_id,
                "adapter": "db2",
                "enabled": True,
                "source_instance": "erp-eu-1",
                "database_name": agent_connection_config()["database"],
                "include": {db2_schema(): [table_name]},
                "connection": agent_connection_config(),
                "backfill_mode": "full",
                "batch_size": 100,
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
    path = temp_dir / "config.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    return path


def restart_validation() -> dict[str, Any]:
    stack = MockServiceStack()
    stack.start()
    try:
        with tempfile.TemporaryDirectory() as temp:
            temp_dir = Path(temp)
            table_name = f"DENOTARY_AGENT_RESTART_{uuid4().hex[:12].upper()}"
            _create_table(table_name)
            source_id = "db2-wave2-restart"
            config_path = build_config(temp_dir, stack, source_id=source_id, table_name=table_name)
            try:
                engine = AgentEngine(load_config(config_path))
                try:
                    engine.bootstrap(source_id)
                    baseline = engine.run_once()
                    for index in range(1, 4):
                        _insert_row(
                            table_name,
                            record_id=index,
                            status="issued",
                            updated_at=f"2026-04-21T10:00:0{index}Z",
                        )
                    first = engine.run_once()
                finally:
                    engine.close()

                engine = AgentEngine(load_config(config_path))
                try:
                    engine.bootstrap(source_id)
                    for index in range(4, 7):
                        _insert_row(
                            table_name,
                            record_id=index,
                            status="paid",
                            updated_at=f"2026-04-21T10:10:0{index - 3}Z",
                        )
                    second = engine.run_once()
                    deliveries = engine.store.list_deliveries(source_id)
                    proofs = engine.store.list_proofs(source_id)
                    dlq = engine.store.list_dlq(source_id)
                finally:
                    engine.close()
            finally:
                _drop_table(table_name)

            result = {
                "adapter": "db2",
                "validation": "restart",
                "baseline_processed": baseline["processed"],
                "first_processed": first["processed"],
                "second_processed": second["processed"],
                "delivery_count": len(deliveries),
                "proof_count": len(proofs),
                "dlq_count": len(dlq),
            }
            if result["baseline_processed"] != 0:
                raise RuntimeError("db2 restart validation expected a zero-event baseline run")
            if result["first_processed"] != 3 or result["second_processed"] != 3:
                raise RuntimeError("db2 restart validation did not process 3 + 3 events")
            if result["delivery_count"] != 6 or result["proof_count"] != 6 or result["dlq_count"] != 0:
                raise RuntimeError("db2 restart validation produced unexpected delivery/proof/DLQ counts")
            return result
    finally:
        stack.stop()


def short_soak_validation(*, cycles: int, events_per_cycle: int) -> dict[str, Any]:
    stack = MockServiceStack()
    stack.start()
    try:
        with tempfile.TemporaryDirectory() as temp:
            temp_dir = Path(temp)
            table_name = f"DENOTARY_AGENT_SOAK_{uuid4().hex[:12].upper()}"
            _create_table(table_name)
            source_id = "db2-wave2-short-soak"
            config_path = build_config(temp_dir, stack, source_id=source_id, table_name=table_name)
            try:
                engine = AgentEngine(load_config(config_path))
                try:
                    engine.bootstrap(source_id)
                    baseline = engine.run_once()
                    cycle_results: list[dict[str, int]] = []
                    for cycle in range(cycles):
                        for index in range(1, events_per_cycle + 1):
                            record_id = cycle * 100 + index
                            _insert_row(
                                table_name,
                                record_id=record_id,
                                status="issued" if cycle % 2 == 0 else "paid",
                                updated_at=datetime(2026, 4, 21, 11, cycle, index).isoformat() + "Z",
                            )
                        cycle_result = engine.run_once()
                        cycle_results.append(cycle_result)
                    deliveries = engine.store.list_deliveries(source_id)
                    proofs = engine.store.list_proofs(source_id)
                    dlq = engine.store.list_dlq(source_id)
                finally:
                    engine.close()
            finally:
                _drop_table(table_name)

            total_processed = sum(item["processed"] for item in cycle_results)
            total_failed = sum(item["failed"] for item in cycle_results)
            expected_total = cycles * events_per_cycle
            payload = {
                "adapter": "db2",
                "validation": "short-soak",
                "cycles": cycles,
                "events_per_cycle": events_per_cycle,
                "baseline_processed": baseline["processed"],
                "cycle_results": cycle_results,
                "total_processed": total_processed,
                "total_failed": total_failed,
                "delivery_count": len(deliveries),
                "proof_count": len(proofs),
                "dlq_count": len(dlq),
            }
            if payload["baseline_processed"] != 0:
                raise RuntimeError("db2 short-soak validation expected a zero-event baseline run")
            if payload["total_processed"] != expected_total:
                raise RuntimeError(f"db2 short-soak processed {payload['total_processed']} events, expected {expected_total}")
            if payload["total_failed"] != 0:
                raise RuntimeError("db2 short-soak reported failures")
            if payload["delivery_count"] != expected_total or payload["proof_count"] != expected_total or payload["dlq_count"] != 0:
                raise RuntimeError("db2 short-soak produced unexpected delivery/proof/DLQ counts")
            return payload
    finally:
        stack.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Wave 2 IBM Db2 validation.")
    parser.add_argument("--mode", choices=("restart", "short-soak"), default="restart")
    parser.add_argument("--cycles", type=int, default=5)
    parser.add_argument("--events-per-cycle", type=int, default=3)
    args = parser.parse_args()

    with _temporary_env(DB2_ENV):
        _docker_compose("down", "-v")
        _docker_compose("up", "-d")
        try:
            _ensure_db2_database()
            wait_for_db2_ready(timeout_sec=900.0, consecutive_successes=3, success_interval_sec=5.0)
            if args.mode == "restart":
                payload = restart_validation()
            else:
                payload = short_soak_validation(cycles=args.cycles, events_per_cycle=args.events_per_cycle)
        finally:
            _docker_compose("down", "-v")
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
