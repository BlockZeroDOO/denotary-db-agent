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
        if self.path.endswith("/v1/batch/prepare"):
            response = {
                "request_id": "elasticsearch-full-cycle-batch-1",
                "trace_id": "elasticsearch-full-cycle-batch-trace-1",
                "external_ref_hash": "d" * 64,
                "root_hash": "e" * 64,
                "manifest_hash": "f" * 64,
                "leaf_count": len(payload["items"]),
                "verification_account": "verif",
                "prepared_action": {
                    "contract": "verifbill",
                    "action": "submitroot",
                    "data": {
                        "payer": payload["submitter"],
                        "submitter": payload["submitter"],
                        "schema_id": 1,
                        "policy_id": 2,
                        "root_hash": "e" * 64,
                        "leaf_count": len(payload["items"]),
                        "manifest_hash": "f" * 64,
                        "external_ref": "d" * 64,
                    },
                },
            }
        else:
            response = {
                "request_id": "elasticsearch-full-cycle-1",
                "trace_id": "elasticsearch-full-cycle-trace-1",
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
        self._send_json(response)

    def _send_json(self, payload: dict[str, Any], status: int = HTTPStatus.OK) -> None:
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(status)
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
                    "block_num": 6777,
                },
            }
        else:
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        self._send_json(payload)

    def _send_json(self, payload: dict[str, Any], status: int = HTTPStatus.OK) -> None:
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(status)
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
        self._send_json(payload)

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
                    "finalized_at": "2026-04-20T15:05:00Z",
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
        self._send_json(response)

    def _send_json(self, payload: dict[str, Any], status: int = HTTPStatus.OK) -> None:
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(status)
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
            if state["mode"] == "batch":
                payload = {
                    "request_id": request_id,
                    "trace_id": state["trace_id"],
                    "mode": "batch",
                    "submitter": state["submitter"],
                    "contract": state["contract"],
                    "root_hash": state["anchor"]["root_hash"],
                    "manifest_hash": state["anchor"]["manifest_hash"],
                    "external_ref_hash": state["anchor"]["external_ref_hash"],
                    "leaf_count": state["anchor"]["leaf_count"],
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
        self._send_json(payload)

    def _send_json(self, payload: dict[str, Any], status: int = HTTPStatus.OK) -> None:
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(status)
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
        self._send_json(payload)

    def _send_json(self, payload: dict[str, Any], status: int = HTTPStatus.OK) -> None:
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, format: str, *args: Any) -> None:
        return


class ElasticsearchFullCycleTest(unittest.TestCase):
    def setUp(self) -> None:
        MockState.requests = {}
        self.temp_dir = tempfile.TemporaryDirectory()
        ingress_port = free_port()
        chain_port = free_port()
        watcher_port = free_port()
        receipt_port = free_port()
        audit_port = free_port()
        self.servers = [
            ThreadingHTTPServer(("127.0.0.1", ingress_port), MockIngressHandler),
            ThreadingHTTPServer(("127.0.0.1", chain_port), MockChainHandler),
            ThreadingHTTPServer(("127.0.0.1", watcher_port), MockWatcherHandler),
            ThreadingHTTPServer(("127.0.0.1", receipt_port), MockReceiptHandler),
            ThreadingHTTPServer(("127.0.0.1", audit_port), MockAuditHandler),
        ]
        self.threads = [threading.Thread(target=server.serve_forever, daemon=True) for server in self.servers]
        for thread in self.threads:
            thread.start()

        self.config_path = Path(self.temp_dir.name) / "config.json"
        config = {
            "agent_name": "elasticsearch-full-cycle-test",
            "log_level": "INFO",
            "denotary": {
                "ingress_url": f"http://127.0.0.1:{ingress_port}",
                "watcher_url": f"http://127.0.0.1:{watcher_port}",
                "watcher_auth_token": "token",
                "receipt_url": f"http://127.0.0.1:{receipt_port}",
                "audit_url": f"http://127.0.0.1:{audit_port}",
                "chain_rpc_url": f"http://127.0.0.1:{chain_port}",
                "broadcast_backend": "private_key",
                "submitter_private_key": "5HpHagT65TZzG1PH3CSu63k8DbpvD8s5ip4nEB3kEsreAnchuDf",
                "submitter": "enterpriseac1",
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
                    "id": "elasticsearch-orders",
                    "adapter": "elasticsearch",
                    "enabled": True,
                    "source_instance": "search-eu-1",
                    "database_name": "search",
                    "include": {"default": ["orders"]},
                    "checkpoint_policy": "after_ack",
                    "backfill_mode": "full",
                    "connection": {
                        "url": "http://127.0.0.1:9200",
                    },
                    "options": {
                        "capture_mode": "watermark",
                        "watermark_field": "updated_at",
                        "commit_timestamp_field": "updated_at",
                        "primary_key_field": "_id",
                        "dry_run_events": [
                            {
                                "schema_or_namespace": "default",
                                "table_or_collection": "orders",
                                "operation": "snapshot",
                                "primary_key": {"_id": "101"},
                                "change_version": "default.orders:2026-04-20T15:00:00Z:101",
                                "checkpoint_token": '{"default.orders":{"watermark":"2026-04-20T15:00:00Z","pk":"101"}}',
                                "commit_timestamp": "2026-04-20T15:00:00Z",
                                "after": {"_id": "101", "status": "issued", "updated_at": "2026-04-20T15:00:00Z"},
                                "before": None,
                                "metadata": {"capture_mode": "watermark-poll"},
                            }
                        ],
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

    def test_run_once_executes_elasticsearch_full_cycle_and_exports_proof_bundle(self) -> None:
        engine = AgentEngine(load_config(self.config_path))

        result = engine.run_once()

        self.assertEqual(result["processed"], 1)
        self.assertEqual(result["failed"], 0)
        deliveries = engine.store.list_deliveries("elasticsearch-orders")
        self.assertEqual(len(deliveries), 1)
        self.assertEqual(deliveries[0]["status"], "finalized_exported")
        self.assertEqual(deliveries[0]["tx_id"], "c" * 64)

        proofs = engine.store.list_proofs("elasticsearch-orders")
        self.assertEqual(len(proofs), 1)
        export_path = proofs[0]["export_path"]
        self.assertTrue(export_path)
        self.assertTrue(Path(str(export_path)).exists())

        proof_payload = json.loads(Path(str(export_path)).read_text(encoding="utf-8"))
        self.assertEqual(proof_payload["request_id"], "elasticsearch-full-cycle-1")
        self.assertEqual(proof_payload["receipt"]["tx_id"], "c" * 64)
        self.assertEqual(proof_payload["audit_chain"]["record"]["status"], "finalized")

    def test_run_once_executes_elasticsearch_batch_cycle_and_exports_batch_bundle(self) -> None:
        config = json.loads(self.config_path.read_text(encoding="utf-8"))
        config["denotary"]["policy_id"] = 2
        config["sources"][0]["batch_enabled"] = True
        config["sources"][0]["batch_size"] = 10
        config["sources"][0]["options"]["dry_run_events"] = [
            {
                "schema_or_namespace": "default",
                "table_or_collection": "orders",
                "operation": "snapshot",
                "primary_key": {"_id": "101"},
                "change_version": "default.orders:2026-04-20T15:00:00Z:101",
                "checkpoint_token": '{"default.orders":{"watermark":"2026-04-20T15:00:00Z","pk":"101"}}',
                "commit_timestamp": "2026-04-20T15:00:00Z",
                "after": {"_id": "101", "status": "issued", "updated_at": "2026-04-20T15:00:00Z"},
                "before": None,
                "metadata": {"capture_mode": "watermark-poll"},
            },
            {
                "schema_or_namespace": "default",
                "table_or_collection": "orders",
                "operation": "snapshot",
                "primary_key": {"_id": "102"},
                "change_version": "default.orders:2026-04-20T15:01:00Z:102",
                "checkpoint_token": '{"default.orders":{"watermark":"2026-04-20T15:01:00Z","pk":"102"}}',
                "commit_timestamp": "2026-04-20T15:01:00Z",
                "after": {"_id": "102", "status": "paid", "updated_at": "2026-04-20T15:01:00Z"},
                "before": None,
                "metadata": {"capture_mode": "watermark-poll"},
            },
        ]
        self.config_path.write_text(json.dumps(config), encoding="utf-8")

        engine = AgentEngine(load_config(self.config_path))
        result = engine.run_once()

        self.assertEqual(result["processed"], 2)
        self.assertEqual(result["failed"], 0)
        deliveries = engine.store.list_deliveries("elasticsearch-orders")
        self.assertEqual(len(deliveries), 1)
        self.assertEqual(deliveries[0]["status"], "finalized_exported")

        proofs = engine.store.list_proofs("elasticsearch-orders")
        self.assertEqual(len(proofs), 1)
        export_path = proofs[0]["export_path"]
        self.assertTrue(export_path)

        proof_payload = json.loads(Path(str(export_path)).read_text(encoding="utf-8"))
        self.assertEqual(proof_payload["request_id"], "elasticsearch-full-cycle-batch-1")
        self.assertEqual(proof_payload["mode"], "batch")
        self.assertEqual(len(proof_payload["members"]), 2)


if __name__ == "__main__":
    unittest.main()
