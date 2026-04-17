from __future__ import annotations

import json
import socket
import tempfile
import threading
import unittest
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


class MockState:
    requests: dict[str, dict[str, Any]] = {}


class MockIngressHandler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802
        body = self.rfile.read(int(self.headers.get("Content-Length", "0"))).decode("utf-8")
        payload = json.loads(body)
        response = {
            "request_id": "request-full-cycle-1",
            "trace_id": "trace-full-cycle-1",
            "external_ref_hash": "a" * 64,
            "object_hash": "b" * 64,
            "verification_account": "verif",
            "prepared_action": {
                "contract": "verifbill",
                "action": "submit",
                "data": {
                    "payer": payload["submitter"],
                    "submitter": payload["submitter"],
                    "schema_id": 1,
                    "policy_id": 1,
                    "object_hash": "b" * 64,
                    "external_ref": "a" * 64,
                },
            },
        }
        encoded = json.dumps(response).encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, format: str, *args: Any) -> None:
        return


class MockChainHandler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802
        if self.path == "/v1/chain/get_info":
            payload = {
                "chain_id": "1" * 64,
                "last_irreversible_block_id": "0" * 64,
                "server_version": "mock-chain",
            }
        elif self.path == "/v1/chain/abi_json_to_bin":
            payload = {"binargs": "00"}
        elif self.path == "/v1/chain/push_transaction":
            payload = {
                "transaction_id": "c" * 64,
                "processed": {
                    "block_num": 777,
                },
            }
        else:
            self.send_error(HTTPStatus.NOT_FOUND)
            return

        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, format: str, *args: Any) -> None:
        return


class MockWatcherHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        prefix = "/v1/watch/"
        if not self.path.startswith(prefix):
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        request_id = self.path[len(prefix):]
        payload = MockState.requests.get(request_id)
        if payload is None:
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def do_POST(self) -> None:  # noqa: N802
        body = self.rfile.read(int(self.headers.get("Content-Length", "0"))).decode("utf-8")
        payload = json.loads(body) if body else {}
        if self.path == "/v1/watch/register":
            MockState.requests[payload["request_id"]] = {
                "request_id": payload["request_id"],
                "trace_id": payload["trace_id"],
                "mode": payload["mode"],
                "submitter": payload["submitter"],
                "contract": payload["contract"],
                "anchor": payload["anchor"],
                "status": "submitted",
                "inclusion_verified": False,
            }
            response = MockState.requests[payload["request_id"]]
        elif self.path.endswith("/included"):
            request_id = self.path.split("/")[-2]
            state = MockState.requests[request_id]
            state.update(
                {
                    "tx_id": payload["tx_id"],
                    "block_num": payload["block_num"],
                    "status": "included",
                    "inclusion_verified": True,
                }
            )
            response = state
        elif self.path.endswith("/poll"):
            request_id = self.path.split("/")[-2]
            state = MockState.requests[request_id]
            state.update(
                {
                    "status": "finalized",
                    "inclusion_verified": True,
                    "finalized_at": "2026-04-17T12:30:00Z",
                }
            )
            response = state
        elif self.path.endswith("/failed"):
            request_id = self.path.split("/")[-2]
            state = MockState.requests[request_id]
            state.update({"status": "failed", "failure_reason": payload["reason"]})
            response = state
        else:
            self.send_error(HTTPStatus.NOT_FOUND)
            return

        encoded = json.dumps(response).encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, format: str, *args: Any) -> None:
        return


class MockReceiptHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/healthz":
            payload = {"status": "ok", "service": "receipt-service"}
        elif self.path.startswith("/v1/receipts/"):
            request_id = self.path.split("/")[-1]
            state = MockState.requests[request_id]
            if state.get("status") != "finalized":
                self.send_error(HTTPStatus.CONFLICT)
                return
            payload = {
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
        else:
            self.send_error(HTTPStatus.NOT_FOUND)
            return

        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, format: str, *args: Any) -> None:
        return


class MockAuditHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/healthz":
            payload = {"status": "ok", "service": "audit-api"}
        elif self.path.startswith("/v1/audit/chain/"):
            request_id = self.path.split("/")[-1]
            state = MockState.requests[request_id]
            payload = {
                "record": {
                    "request_id": request_id,
                    "trace_id": state["trace_id"],
                    "status": state["status"],
                    "tx_id": state["tx_id"],
                    "block_num": state["block_num"],
                },
                "receipt": {
                    "request_id": request_id,
                    "receipt_available": True,
                },
                "proof_chain": [
                    {"stage": "request_registered", "status": "completed"},
                    {"stage": "transaction_included", "status": "completed"},
                    {"stage": "block_finalized", "status": "completed"},
                ],
            }
        else:
            self.send_error(HTTPStatus.NOT_FOUND)
            return

        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, format: str, *args: Any) -> None:
        return


class FullCycleEngineTest(unittest.TestCase):
    def setUp(self) -> None:
        MockState.requests = {}
        self.temp_dir = tempfile.TemporaryDirectory()
        ingress_port = free_port()
        watcher_port = free_port()
        receipt_port = free_port()
        audit_port = free_port()
        chain_port = free_port()

        self.servers = [
            ThreadingHTTPServer(("127.0.0.1", ingress_port), MockIngressHandler),
            ThreadingHTTPServer(("127.0.0.1", watcher_port), MockWatcherHandler),
            ThreadingHTTPServer(("127.0.0.1", receipt_port), MockReceiptHandler),
            ThreadingHTTPServer(("127.0.0.1", audit_port), MockAuditHandler),
            ThreadingHTTPServer(("127.0.0.1", chain_port), MockChainHandler),
        ]
        self.threads = [threading.Thread(target=server.serve_forever, daemon=True) for server in self.servers]
        for thread in self.threads:
            thread.start()

        self.config_path = Path(self.temp_dir.name) / "config.json"
        config = {
            "agent_name": "full-cycle-agent",
            "log_level": "INFO",
            "denotary": {
                "ingress_url": f"http://127.0.0.1:{ingress_port}",
                "watcher_url": f"http://127.0.0.1:{watcher_port}",
                "watcher_auth_token": "token",
                "receipt_url": f"http://127.0.0.1:{receipt_port}",
                "audit_url": f"http://127.0.0.1:{audit_port}",
                "chain_rpc_url": f"http://127.0.0.1:{chain_port}",
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
                "state_db": str(Path(self.temp_dir.name) / "state.sqlite3"),
                "proof_dir": str(Path(self.temp_dir.name) / "proofs"),
            },
            "sources": [
                {
                    "id": "pg-core-ledger",
                    "adapter": "postgresql",
                    "enabled": True,
                    "source_instance": "erp-eu-1",
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
                                "commit_timestamp": "2026-04-17T10:11:12Z",
                                "after": {"id": 1, "status": "issued"},
                                "before": {"id": 1, "status": "draft"},
                                "metadata": {"txid": 100},
                            }
                        ]
                    },
                }
            ],
        }
        self.config_path.write_text(json.dumps(config), encoding="utf-8")

    def tearDown(self) -> None:
        for server in self.servers:
            server.shutdown()
            server.server_close()
        for thread in self.threads:
            thread.join(timeout=2)
        self.temp_dir.cleanup()

    def test_run_once_executes_full_cycle_and_exports_proof_bundle(self) -> None:
        engine = AgentEngine(load_config(self.config_path))

        result = engine.run_once()

        self.assertEqual(result["processed"], 1)
        self.assertEqual(result["failed"], 0)
        deliveries = engine.store.list_deliveries("pg-core-ledger")
        self.assertEqual(len(deliveries), 1)
        self.assertEqual(deliveries[0]["status"], "finalized_exported")
        self.assertEqual(deliveries[0]["tx_id"], "c" * 64)

        proofs = engine.store.list_proofs("pg-core-ledger")
        self.assertEqual(len(proofs), 1)
        export_path = proofs[0]["export_path"]
        self.assertTrue(export_path)
        self.assertTrue(Path(str(export_path)).exists())

        proof_payload = json.loads(Path(str(export_path)).read_text(encoding="utf-8"))
        self.assertEqual(proof_payload["request_id"], "request-full-cycle-1")
        self.assertEqual(proof_payload["receipt"]["tx_id"], "c" * 64)
        self.assertEqual(proof_payload["audit_chain"]["record"]["status"], "finalized")
