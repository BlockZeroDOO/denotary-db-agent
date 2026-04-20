from __future__ import annotations

import argparse
import json
import socket
import sqlite3
import sys
import tempfile
import threading
from dataclasses import dataclass, field
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from denotary_db_agent.config import load_config
from denotary_db_agent.engine import AgentEngine


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TESTS_ROOT = PROJECT_ROOT / "tests"
sys.path.insert(0, str(TESTS_ROOT))

from redis_live_support import REDIS_PORT, REDIS_URL, redis, run_redis_compose, wait_for_redis  # type: ignore


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as handle:
        handle.bind(("127.0.0.1", 0))
        return int(handle.getsockname()[1])


@dataclass
class FaultState:
    requests: dict[str, dict[str, Any]] = field(default_factory=dict)
    counter: int = 0
    fail_counts: dict[str, int] = field(default_factory=dict)

    def should_fail(self, key: str) -> bool:
        remaining = int(self.fail_counts.get(key) or 0)
        if remaining > 0:
            self.fail_counts[key] = remaining - 1
            return True
        return False


STATE = FaultState()


class BaseHandler(BaseHTTPRequestHandler):
    def _send_json(self, payload: dict[str, Any], status: int = HTTPStatus.OK) -> None:
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, format: str, *args: Any) -> None:
        return


class IngressHandler(BaseHandler):
    def do_POST(self) -> None:  # noqa: N802
        if STATE.should_fail("ingress.prepare"):
            self.send_error(HTTPStatus.SERVICE_UNAVAILABLE)
            return
        body = self.rfile.read(int(self.headers.get("Content-Length", "0"))).decode("utf-8")
        payload = json.loads(body)
        STATE.counter += 1
        request_id = f"wave2-outage-request-{STATE.counter:04d}"
        trace_id = f"wave2-outage-trace-{STATE.counter:04d}"
        response = {
            "request_id": request_id,
            "trace_id": trace_id,
            "external_ref_hash": f"{STATE.counter:064x}",
            "object_hash": f"{STATE.counter + 1000:064x}",
            "verification_account": "verif",
            "prepared_action": {
                "contract": "verifbill",
                "action": "submit",
                "data": {
                    "payer": payload["submitter"],
                    "submitter": payload["submitter"],
                    "schema_id": 1,
                    "policy_id": 1,
                    "object_hash": f"{STATE.counter + 1000:064x}",
                    "external_ref": f"{STATE.counter:064x}",
                },
            },
        }
        self._send_json(response)


class ChainHandler(BaseHandler):
    def do_POST(self) -> None:  # noqa: N802
        if self.path == "/v1/chain/get_info":
            self._send_json(
                {
                    "chain_id": "1" * 64,
                    "last_irreversible_block_id": "0" * 64,
                    "server_version": "mock-chain",
                }
            )
            return
        if self.path == "/v1/chain/abi_json_to_bin":
            self._send_json({"binargs": "00"})
            return
        if self.path == "/v1/chain/push_transaction":
            self._send_json(
                {
                    "transaction_id": f"{STATE.counter + 5000:064x}",
                    "processed": {"block_num": 975000 + STATE.counter},
                }
            )
            return
        self.send_error(HTTPStatus.NOT_FOUND)


class WatcherHandler(BaseHandler):
    def do_GET(self) -> None:  # noqa: N802
        prefix = "/v1/watch/"
        if not self.path.startswith(prefix):
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        request_id = self.path[len(prefix):]
        payload = STATE.requests.get(request_id)
        if payload is None:
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        self._send_json(payload)

    def do_POST(self) -> None:  # noqa: N802
        body = self.rfile.read(int(self.headers.get("Content-Length", "0"))).decode("utf-8")
        payload = json.loads(body) if body else {}
        if self.path == "/v1/watch/register":
            if STATE.should_fail("watcher.register"):
                self.send_error(HTTPStatus.SERVICE_UNAVAILABLE)
                return
            STATE.requests[payload["request_id"]] = {
                "request_id": payload["request_id"],
                "trace_id": payload["trace_id"],
                "mode": payload["mode"],
                "submitter": payload["submitter"],
                "contract": payload["contract"],
                "anchor": payload["anchor"],
                "status": "submitted",
                "inclusion_verified": False,
            }
            self._send_json(STATE.requests[payload["request_id"]])
            return
        if self.path.endswith("/included"):
            request_id = self.path.split("/")[-2]
            state = STATE.requests[request_id]
            state.update(
                {
                    "tx_id": payload["tx_id"],
                    "block_num": payload["block_num"],
                    "status": "included",
                    "inclusion_verified": True,
                }
            )
            self._send_json(state)
            return
        if self.path.endswith("/poll"):
            request_id = self.path.split("/")[-2]
            state = STATE.requests[request_id]
            state.update(
                {
                    "status": "finalized",
                    "inclusion_verified": True,
                    "finalized_at": "2026-04-20T18:00:00Z",
                }
            )
            self._send_json(state)
            return
        if self.path.endswith("/failed"):
            request_id = self.path.split("/")[-2]
            state = STATE.requests.setdefault(
                request_id,
                {
                    "request_id": request_id,
                    "trace_id": "",
                    "mode": "single",
                    "submitter": "",
                    "contract": "",
                    "anchor": {},
                },
            )
            state.update({"status": "failed", "failure_reason": payload["reason"]})
            self._send_json(state)
            return
        self.send_error(HTTPStatus.NOT_FOUND)


class ReceiptHandler(BaseHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/healthz":
            self._send_json({"status": "ok", "service": "receipt-service"})
            return
        if not self.path.startswith("/v1/receipts/"):
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        if STATE.should_fail("receipt.get"):
            self.send_error(HTTPStatus.SERVICE_UNAVAILABLE)
            return
        request_id = self.path.split("/")[-1]
        state = STATE.requests[request_id]
        self._send_json(
            {
                "request_id": request_id,
                "trace_id": state["trace_id"],
                "mode": "single",
                "submitter": state["submitter"],
                "contract": state["contract"],
                "object_hash": state["anchor"]["object_hash"],
                "external_ref_hash": state["anchor"]["external_ref_hash"],
                "tx_id": state["tx_id"],
                "block_num": state["block_num"],
                "finality_flag": True,
                "finalized_at": state["finalized_at"],
                "trust_state": "finalized_verified",
                "receipt_available": True,
                "inclusion_verified": True,
                "verification_policy": "single-provider",
                "verification_min_success": 1,
                "provider_disagreement": False,
            }
        )


class AuditHandler(BaseHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/healthz":
            self._send_json({"status": "ok", "service": "audit-api"})
            return
        if not self.path.startswith("/v1/audit/chain/"):
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        if STATE.should_fail("audit.get_chain"):
            self.send_error(HTTPStatus.SERVICE_UNAVAILABLE)
            return
        request_id = self.path.split("/")[-1]
        state = STATE.requests[request_id]
        self._send_json(
            {
                "record": {
                    "request_id": request_id,
                    "trace_id": state["trace_id"],
                    "status": state["status"],
                    "tx_id": state["tx_id"],
                    "block_num": state["block_num"],
                },
                "receipt": {"request_id": request_id, "receipt_available": True},
                "proof_chain": [
                    {"stage": "request_registered", "status": "completed"},
                    {"stage": "transaction_included", "status": "completed"},
                    {"stage": "block_finalized", "status": "completed"},
                ],
            }
        )


class MockServiceStack:
    def __init__(self) -> None:
        self.ingress_port = free_port()
        self.watcher_port = free_port()
        self.receipt_port = free_port()
        self.audit_port = free_port()
        self.chain_port = free_port()
        self.servers = [
            ThreadingHTTPServer(("127.0.0.1", self.ingress_port), IngressHandler),
            ThreadingHTTPServer(("127.0.0.1", self.watcher_port), WatcherHandler),
            ThreadingHTTPServer(("127.0.0.1", self.receipt_port), ReceiptHandler),
            ThreadingHTTPServer(("127.0.0.1", self.audit_port), AuditHandler),
            ThreadingHTTPServer(("127.0.0.1", self.chain_port), ChainHandler),
        ]
        self.threads = [threading.Thread(target=server.serve_forever, daemon=True) for server in self.servers]

    def start(self) -> None:
        STATE.requests = {}
        STATE.counter = 0
        STATE.fail_counts = {}
        for thread in self.threads:
            thread.start()

    def stop(self) -> None:
        for server in self.servers:
            server.shutdown()
            server.server_close()
        for thread in self.threads:
            thread.join(timeout=2)


def initialize_sqlite(database_path: Path) -> None:
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


def insert_sqlite_invoice(database_path: Path, *, record_id: int) -> None:
    connection = sqlite3.connect(str(database_path))
    try:
        connection.execute(
            "insert into invoices (id, status, updated_at) values (?, ?, ?)",
            (record_id, "issued", f"2026-04-20T18:10:{record_id:02d}Z"),
        )
        connection.commit()
    finally:
        connection.close()


def redis_client():
    if redis is None:
        raise RuntimeError("redis package is required for Wave 2 Redis service-outage validation")
    return redis.Redis.from_url(REDIS_URL, decode_responses=False, socket_timeout=5)


def flush_redis() -> None:
    wait_for_redis()
    last_error: Exception | None = None
    for _ in range(3):
        client = redis_client()
        try:
            client.flushdb()
            return
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            wait_for_redis()
        finally:
            close = getattr(client, "close", None)
            if callable(close):
                close()
    if last_error is not None:
        raise last_error


def write_redis_key(key: str, value: str) -> None:
    client = redis_client()
    try:
        client.set(key, value)
    finally:
        close = getattr(client, "close", None)
        if callable(close):
            close()


def write_sqlite_config(temp_dir: Path, stack: MockServiceStack, database_path: Path) -> Path:
    config = {
        "agent_name": "wave2-sqlite-service-outage",
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
                "id": "sqlite-wave2-service-outage",
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
    path = temp_dir / "sqlite-config.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    return path


def write_redis_config(temp_dir: Path, stack: MockServiceStack) -> Path:
    config = {
        "agent_name": "wave2-redis-service-outage",
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
                "id": "redis-wave2-service-outage",
                "adapter": "redis",
                "enabled": True,
                "source_instance": "wave2-validation",
                "database_name": "db0",
                "include": {"0": ["orders:*"]},
                "checkpoint_policy": "after_ack",
                "backfill_mode": "full",
                "connection": {
                    "host": "127.0.0.1",
                    "port": REDIS_PORT,
                },
                "options": {
                    "capture_mode": "scan",
                    "row_limit": 100,
                    "scan_count": 100,
                },
            }
        ],
    }
    path = temp_dir / "redis-config.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    return path


def assert_recovery(result: dict[str, Any]) -> None:
    if result["first"]["processed"] != 0 or result["first"]["failed"] != 1:
        raise RuntimeError(f"{result['adapter']} {result['scenario']} first attempt did not fail as expected")
    if result["second"]["processed"] != 1 or result["second"]["failed"] != 0:
        raise RuntimeError(f"{result['adapter']} {result['scenario']} second attempt did not recover as expected")
    if result["proof_count"] < 1:
        raise RuntimeError(f"{result['adapter']} {result['scenario']} did not export proof after recovery")


def run_sqlite_scenario(name: str, fail_key: str) -> dict[str, Any]:
    stack = MockServiceStack()
    stack.start()
    STATE.fail_counts = {fail_key: 3}
    try:
        with tempfile.TemporaryDirectory() as temp:
            temp_dir = Path(temp)
            database_path = temp_dir / "ledger.sqlite3"
            initialize_sqlite(database_path)
            config_path = write_sqlite_config(temp_dir, stack, database_path)
            engine = AgentEngine(load_config(config_path))
            try:
                engine.bootstrap("sqlite-wave2-service-outage")
                baseline = engine.run_once()
                insert_sqlite_invoice(database_path, record_id=1)
                first = engine.run_once()
                second = engine.run_once()
                result = {
                    "adapter": "sqlite",
                    "scenario": name,
                    "failed_component": fail_key,
                    "baseline_processed": baseline["processed"],
                    "first": first,
                    "second": second,
                    "delivery_count": len(engine.store.list_deliveries("sqlite-wave2-service-outage")),
                    "proof_count": len(engine.store.list_proofs("sqlite-wave2-service-outage")),
                    "dlq_count": len(engine.store.list_dlq("sqlite-wave2-service-outage")),
                }
                if result["baseline_processed"] != 0:
                    raise RuntimeError("sqlite outage validation expected a zero-event baseline run")
                assert_recovery(result)
                return result
            finally:
                engine.close()
    finally:
        stack.stop()


def run_redis_scenario(name: str, fail_key: str) -> dict[str, Any]:
    run_redis_compose("down", "-v")
    run_redis_compose("up", "-d")
    wait_for_redis()
    flush_redis()
    stack = MockServiceStack()
    stack.start()
    STATE.fail_counts = {fail_key: 3}
    try:
        with tempfile.TemporaryDirectory() as temp:
            temp_dir = Path(temp)
            config_path = write_redis_config(temp_dir, stack)
            engine = AgentEngine(load_config(config_path))
            try:
                engine.bootstrap("redis-wave2-service-outage")
                baseline = engine.run_once()
                write_redis_key("orders:0001", "issued")
                first = engine.run_once()
                second = engine.run_once()
                result = {
                    "adapter": "redis",
                    "scenario": name,
                    "failed_component": fail_key,
                    "baseline_processed": baseline["processed"],
                    "first": first,
                    "second": second,
                    "delivery_count": len(engine.store.list_deliveries("redis-wave2-service-outage")),
                    "proof_count": len(engine.store.list_proofs("redis-wave2-service-outage")),
                    "dlq_count": len(engine.store.list_dlq("redis-wave2-service-outage")),
                }
                if result["baseline_processed"] != 0:
                    raise RuntimeError("redis outage validation expected a zero-event baseline run")
                assert_recovery(result)
                return result
            finally:
                engine.close()
    finally:
        stack.stop()
        run_redis_compose("down", "-v")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Wave 2 local service-outage validation")
    parser.add_argument(
        "--adapter",
        choices=("sqlite", "redis", "all"),
        default="all",
        help="adapter to validate",
    )
    args = parser.parse_args()

    scenarios = [
        ("ingress_prepare_outage", "ingress.prepare"),
        ("watcher_register_outage", "watcher.register"),
        ("receipt_fetch_outage", "receipt.get"),
        ("audit_fetch_outage", "audit.get_chain"),
    ]
    adapters = ["sqlite", "redis"] if args.adapter == "all" else [args.adapter]
    results: list[dict[str, Any]] = []
    for adapter in adapters:
        runner = run_sqlite_scenario if adapter == "sqlite" else run_redis_scenario
        for name, fail_key in scenarios:
            try:
                payload = runner(name, fail_key)
                payload["status"] = "passed"
                results.append(payload)
            except Exception as exc:  # noqa: BLE001
                results.append(
                    {
                        "adapter": adapter,
                        "scenario": name,
                        "status": "failed",
                        "error": str(exc),
                    }
                )
    print(json.dumps({"results": results}, indent=2))


if __name__ == "__main__":
    main()
