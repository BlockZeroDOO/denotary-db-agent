from __future__ import annotations

import json
import socket
import tempfile
import threading
from dataclasses import dataclass, field
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from denotary_db_agent.config import load_config
from denotary_db_agent.engine import AgentEngine


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
        request_id = f"outage-request-{STATE.counter:04d}"
        trace_id = f"outage-trace-{STATE.counter:04d}"
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
                    "processed": {"block_num": 950000 + STATE.counter},
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
                    "finalized_at": "2026-04-19T00:00:00Z",
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
        for thread in self.threads:
            thread.start()

    def stop(self) -> None:
        for server in self.servers:
            server.shutdown()
            server.server_close()
        for thread in self.threads:
            thread.join(timeout=2)


def write_config(temp_dir: Path, agent_name: str, stack: MockServiceStack) -> Path:
    config = {
        "agent_name": agent_name,
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
                "id": "service-outage-source",
                "adapter": "postgresql",
                "enabled": True,
                "source_instance": "service-outage",
                "database_name": "ledger",
                "connection": {
                    "host": "127.0.0.1",
                    "port": 5432,
                    "username": "denotary",
                    "database": "ledger",
                },
                "options": {
                    "dry_run_events": [
                        {
                            "schema_or_namespace": "public",
                            "table_or_collection": "invoices",
                            "operation": "update",
                            "primary_key": {"id": 1},
                            "change_version": "lsn:1",
                            "checkpoint_token": "lsn:1",
                            "commit_timestamp": "2026-04-19T01:00:00Z",
                            "after": {"id": 1, "status": "issued"},
                            "before": {"id": 1, "status": "draft"},
                            "metadata": {"txid": 100},
                        }
                    ]
                },
            }
        ],
    }
    path = temp_dir / "config.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    return path


def _assert_recovery(result: dict[str, Any]) -> None:
    if result["first"]["processed"] != 0 or result["first"]["failed"] != 1:
        raise RuntimeError(f"{result['scenario']} first attempt did not fail as expected")
    if result["second"]["processed"] != 1 or result["second"]["failed"] != 0:
        raise RuntimeError(f"{result['scenario']} second attempt did not recover as expected")
    if result["delivery_count"] < 1 or result["proof_count"] < 1:
        raise RuntimeError(f"{result['scenario']} did not export proof after recovery")


def run_scenario(name: str, fail_key: str) -> dict[str, Any]:
    stack = MockServiceStack()
    stack.start()
    STATE.fail_counts = {fail_key: 3}
    try:
        with tempfile.TemporaryDirectory() as temp:
            config_path = write_config(Path(temp), f"{name}-validation", stack)
            engine = AgentEngine(load_config(config_path))
            try:
                first = engine.run_once()
                second = engine.run_once()
                result = {
                    "scenario": name,
                    "failed_component": fail_key,
                    "first": first,
                    "second": second,
                    "delivery_count": len(engine.store.list_deliveries("service-outage-source")),
                    "proof_count": len(engine.store.list_proofs("service-outage-source")),
                    "dlq_count": len(engine.store.list_dlq("service-outage-source")),
                }
                _assert_recovery(result)
                return result
            finally:
                engine.close()
    finally:
        stack.stop()


def main() -> None:
    scenarios = [
        ("ingress_prepare_outage", "ingress.prepare"),
        ("watcher_register_outage", "watcher.register"),
        ("receipt_fetch_outage", "receipt.get"),
        ("audit_fetch_outage", "audit.get_chain"),
    ]
    results: list[dict[str, Any]] = []
    for name, fail_key in scenarios:
        try:
            payload = run_scenario(name, fail_key)
            payload["status"] = "passed"
            results.append(payload)
        except Exception as exc:  # noqa: BLE001
            results.append({"scenario": name, "status": "failed", "error": str(exc)})
    print(json.dumps({"results": results}, indent=2))


if __name__ == "__main__":
    main()
