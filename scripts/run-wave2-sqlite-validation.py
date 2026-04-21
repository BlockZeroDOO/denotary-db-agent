from __future__ import annotations

import argparse
import importlib.util
import json
import sqlite3
import sys
import tempfile
import threading
from datetime import datetime
from http.server import ThreadingHTTPServer
from pathlib import Path
from typing import Any

from denotary_db_agent.config import load_config
from denotary_db_agent.engine import AgentEngine


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TESTS_ROOT = PROJECT_ROOT / "tests"


def _load_module(module_path: Path, module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


FULL_CYCLE = _load_module(TESTS_ROOT / "test_sqlite_full_cycle.py", "wave2_sqlite_full_cycle")


class IncrementingIngressHandler(FULL_CYCLE.BaseHTTPRequestHandler):
    counter = 0

    def do_POST(self) -> None:  # noqa: N802
        IncrementingIngressHandler.counter += 1
        body = self.rfile.read(int(self.headers.get("Content-Length", "0"))).decode("utf-8")
        payload = json.loads(body)
        response = {
            "request_id": f"wave2-sqlite-request-{IncrementingIngressHandler.counter}",
            "trace_id": f"wave2-sqlite-trace-{IncrementingIngressHandler.counter}",
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

    def log_message(self, format: str, *args: Any) -> None:
        return


class MockServiceStack:
    def __init__(self) -> None:
        self.ingress_port = FULL_CYCLE.free_port()
        self.watcher_port = FULL_CYCLE.free_port()
        self.receipt_port = FULL_CYCLE.free_port()
        self.audit_port = FULL_CYCLE.free_port()
        self.chain_port = FULL_CYCLE.free_port()
        FULL_CYCLE.MockState.requests = {}
        IncrementingIngressHandler.counter = 0
        self.servers = [
            ThreadingHTTPServer(("127.0.0.1", self.ingress_port), IncrementingIngressHandler),
            ThreadingHTTPServer(("127.0.0.1", self.watcher_port), FULL_CYCLE.MockWatcherHandler),
            ThreadingHTTPServer(("127.0.0.1", self.receipt_port), FULL_CYCLE.MockReceiptHandler),
            ThreadingHTTPServer(("127.0.0.1", self.audit_port), FULL_CYCLE.MockAuditHandler),
            ThreadingHTTPServer(("127.0.0.1", self.chain_port), FULL_CYCLE.MockChainHandler),
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


def initialize_database(database_path: Path) -> None:
    connection = sqlite3.connect(str(database_path))
    try:
        connection.execute(
            """
            create table if not exists invoices (
                id integer primary key,
                status text not null,
                updated_at text not null
            )
            """
        )
        connection.execute("delete from invoices")
        connection.commit()
    finally:
        connection.close()


def insert_invoice(database_path: Path, *, record_id: int, status: str, updated_at: str) -> None:
    connection = sqlite3.connect(str(database_path))
    try:
        connection.execute(
            "insert into invoices (id, status, updated_at) values (?, ?, ?)",
            (record_id, status, updated_at),
        )
        connection.commit()
    finally:
        connection.close()


def build_config(temp_dir: Path, mock_stack: MockServiceStack, *, source_id: str, database_path: Path) -> Path:
    config = {
        "agent_name": f"{source_id}-validation",
        "log_level": "INFO",
        "denotary": {
            "ingress_url": f"http://127.0.0.1:{mock_stack.ingress_port}",
            "watcher_url": f"http://127.0.0.1:{mock_stack.watcher_port}",
            "watcher_auth_token": "token",
            "receipt_url": f"http://127.0.0.1:{mock_stack.receipt_port}",
            "audit_url": f"http://127.0.0.1:{mock_stack.audit_port}",
            "chain_rpc_url": f"http://127.0.0.1:{mock_stack.chain_port}",
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
                "adapter": "sqlite",
                "enabled": True,
                "source_instance": "edge-device-1",
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
    path = temp_dir / "config.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    return path


def restart_validation() -> dict[str, Any]:
    mock = MockServiceStack()
    mock.start()
    try:
        with tempfile.TemporaryDirectory() as temp:
            temp_dir = Path(temp)
            database_path = temp_dir / "ledger.sqlite3"
            initialize_database(database_path)
            source_id = "sqlite-wave2-restart"
            config_path = build_config(temp_dir, mock, source_id=source_id, database_path=database_path)

            engine = AgentEngine(load_config(config_path))
            try:
                engine.bootstrap(source_id)
                baseline = engine.run_once()
                for index in range(1, 4):
                    insert_invoice(
                        database_path,
                        record_id=index,
                        status="issued",
                        updated_at=f"2026-04-20T16:00:0{index}Z",
                    )
                first = engine.run_once()
            finally:
                engine.close()

            engine = AgentEngine(load_config(config_path))
            try:
                engine.bootstrap(source_id)
                for index in range(4, 7):
                    insert_invoice(
                        database_path,
                        record_id=index,
                        status="paid",
                        updated_at=f"2026-04-20T16:10:0{index - 3}Z",
                    )
                second = engine.run_once()
                deliveries = engine.store.list_deliveries(source_id)
                proofs = engine.store.list_proofs(source_id)
                dlq = engine.store.list_dlq(source_id)
            finally:
                engine.close()

            result = {
                "adapter": "sqlite",
                "validation": "restart",
                "baseline_processed": baseline["processed"],
                "first_processed": first["processed"],
                "second_processed": second["processed"],
                "delivery_count": len(deliveries),
                "proof_count": len(proofs),
                "dlq_count": len(dlq),
            }
            if result["baseline_processed"] != 0:
                raise RuntimeError("sqlite restart validation expected a zero-event baseline run")
            if result["first_processed"] != 3 or result["second_processed"] != 3:
                raise RuntimeError("sqlite restart validation did not process 3 + 3 events")
            if result["delivery_count"] != 6 or result["proof_count"] != 6 or result["dlq_count"] != 0:
                raise RuntimeError("sqlite restart validation produced unexpected delivery/proof/DLQ counts")
            return result
    finally:
        mock.stop()


def short_soak_validation(*, cycles: int, events_per_cycle: int) -> dict[str, Any]:
    mock = MockServiceStack()
    mock.start()
    try:
        with tempfile.TemporaryDirectory() as temp:
            temp_dir = Path(temp)
            database_path = temp_dir / "ledger.sqlite3"
            initialize_database(database_path)
            source_id = "sqlite-wave2-short-soak"
            config_path = build_config(temp_dir, mock, source_id=source_id, database_path=database_path)
            engine = AgentEngine(load_config(config_path))
            try:
                engine.bootstrap(source_id)
                baseline = engine.run_once()
                total_processed = baseline["processed"]
                total_failed = baseline["failed"]
                cycle_results: list[dict[str, int]] = []
                for cycle in range(cycles):
                    for index in range(1, events_per_cycle + 1):
                        record_id = cycle * 100 + index
                        insert_invoice(
                            database_path,
                            record_id=record_id,
                            status="issued" if cycle % 2 == 0 else "paid",
                            updated_at=datetime(2026, 4, 20, 17, cycle, index).isoformat() + "Z",
                        )
                    result = engine.run_once()
                    cycle_results.append(result)
                    total_processed += result["processed"]
                    total_failed += result["failed"]
                deliveries = engine.store.list_deliveries(source_id)
                proofs = engine.store.list_proofs(source_id)
                dlq = engine.store.list_dlq(source_id)
            finally:
                engine.close()

            expected_total = cycles * events_per_cycle
            payload = {
                "adapter": "sqlite",
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
                raise RuntimeError("sqlite short-soak validation expected a zero-event baseline run")
            if payload["total_processed"] != expected_total:
                raise RuntimeError(f"sqlite short-soak processed {payload['total_processed']} events, expected {expected_total}")
            if payload["total_failed"] != 0:
                raise RuntimeError("sqlite short-soak reported failures")
            if payload["delivery_count"] != expected_total or payload["proof_count"] != expected_total or payload["dlq_count"] != 0:
                raise RuntimeError("sqlite short-soak produced unexpected delivery/proof/DLQ counts")
            return payload
    finally:
        mock.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Wave 2 SQLite validation.")
    parser.add_argument("--mode", choices=("restart", "short-soak"), default="restart")
    parser.add_argument("--cycles", type=int, default=5)
    parser.add_argument("--events-per-cycle", type=int, default=3)
    args = parser.parse_args()

    if args.mode == "restart":
        payload = restart_validation()
    else:
        payload = short_soak_validation(cycles=args.cycles, events_per_cycle=args.events_per_cycle)
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
