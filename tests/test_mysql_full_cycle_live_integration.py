from __future__ import annotations

import json
import socket
import subprocess
import tempfile
import threading
import time
import unittest
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pymysql

from denotary_db_agent.config import load_config
from denotary_db_agent.engine import AgentEngine


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MYSQL_COMPOSE_FILE = PROJECT_ROOT / "deploy" / "mysql-live" / "docker-compose.yml"
MYSQL_PORT = 57306


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
                "request_id": "mysql-batch-request-1",
                "trace_id": "mysql-batch-trace-1",
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
                "request_id": "mysql-request-1",
                "trace_id": "mysql-trace-1",
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
                    "block_num": 1777,
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
                    "finalized_at": "2026-04-18T11:00:00Z",
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


class MySqlFullCycleLiveIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._docker_compose("down", "-v")
        cls._docker_compose("up", "-d")
        cls._wait_for_mysql()

    @classmethod
    def tearDownClass(cls) -> None:
        cls._docker_compose("down", "-v")

    @classmethod
    def _docker_compose(cls, *args: str) -> None:
        subprocess.run(
            ["docker", "compose", "-f", str(MYSQL_COMPOSE_FILE), *args],
            check=True,
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
        )

    @classmethod
    def _wait_for_mysql(cls) -> None:
        deadline = time.time() + 90
        while time.time() < deadline:
            try:
                connection = pymysql.connect(
                    host="127.0.0.1",
                    port=MYSQL_PORT,
                    user="denotary",
                    password="denotarypw",
                    database="ledger",
                    connect_timeout=2,
                    cursorclass=pymysql.cursors.DictCursor,
                    autocommit=True,
                )
                with connection.cursor() as cursor:
                    cursor.execute("select 1 as ok")
                    cursor.fetchone()
                connection.close()
                return
            except Exception:
                time.sleep(1)
        raise RuntimeError("mysql live container did not become ready in time")

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

        self._truncate_tables()
        self.config_path = Path(self.temp_dir.name) / "config.json"
        self._write_config(
            ingress_port=ingress_port,
            watcher_port=watcher_port,
            receipt_port=receipt_port,
            audit_port=audit_port,
            chain_port=chain_port,
        )

    def tearDown(self) -> None:
        for server in self.servers:
            server.shutdown()
            server.server_close()
        for thread in self.threads:
            thread.join(timeout=2)
        self.temp_dir.cleanup()

    def _connect(self):
        return pymysql.connect(
            host="127.0.0.1",
            port=MYSQL_PORT,
            user="denotary",
            password="denotarypw",
            database="ledger",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
        )

    def _connect_admin(self):
        return pymysql.connect(
            host="127.0.0.1",
            port=MYSQL_PORT,
            user="root",
            password="rootpw",
            database="ledger",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
        )

    def _truncate_tables(self) -> None:
        connection = self._connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute("delete from payments")
                cursor.execute("delete from invoices")
        finally:
            connection.close()

    def _current_binlog_start(self) -> tuple[str, int]:
        connection = self._connect_admin()
        try:
            with connection.cursor() as cursor:
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
            connection.close()
        if not row:
            raise RuntimeError("mysql did not return SHOW MASTER STATUS output")
        return str(row["File"]), int(row["Position"])

    def _write_config(
        self,
        *,
        ingress_port: int,
        watcher_port: int,
        receipt_port: int,
        audit_port: int,
        chain_port: int,
        batch_enabled: bool = False,
        capture_mode: str = "watermark",
        binlog_start_file: str = "",
        binlog_start_pos: int = 4,
    ) -> None:
        config = {
            "agent_name": "mysql-full-cycle-live-test",
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
                "policy_id": 2 if batch_enabled else 1,
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
                    "id": "mysql-core-ledger",
                    "adapter": "mysql",
                    "enabled": True,
                    "source_instance": "erp-eu-1",
                    "database_name": "ledger",
                    "include": {"ledger": ["invoices", "payments"]},
                    "connection": {
                        "host": "127.0.0.1",
                        "port": MYSQL_PORT,
                        "username": "denotary",
                        "password": "denotarypw",
                        "database": "ledger",
                    },
                    "backfill_mode": "full",
                    "batch_enabled": batch_enabled,
                    "batch_size": 10,
                    "options": {
                        "capture_mode": capture_mode,
                        "watermark_column": "updated_at",
                        "commit_timestamp_column": "updated_at",
                        "row_limit": 100,
                        "binlog_server_id": 14111,
                        "binlog_start_file": binlog_start_file,
                        "binlog_start_pos": binlog_start_pos,
                    },
                }
            ],
        }
        self.config_path.write_text(json.dumps(config), encoding="utf-8")

    def _seed_single_row(self) -> None:
        connection = self._connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    insert into invoices (id, status, amount, updated_at)
                    values (9001, 'issued', 111.00, '2026-04-18 10:00:01')
                    """
                )
        finally:
            connection.close()

    def _seed_batch_rows(self) -> None:
        connection = self._connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    insert into invoices (id, status, amount, updated_at)
                    values
                        (9101, 'issued', 121.00, '2026-04-18 10:10:01'),
                        (9102, 'paid', 122.00, '2026-04-18 10:10:02')
                    """
                )
        finally:
            connection.close()

    def test_live_mysql_full_cycle_exports_proof_bundle(self) -> None:
        self._seed_single_row()
        engine = AgentEngine(load_config(self.config_path))

        result = engine.run_once()

        self.assertEqual(result["processed"], 1)
        self.assertEqual(result["failed"], 0)
        deliveries = engine.store.list_deliveries("mysql-core-ledger")
        self.assertEqual(len(deliveries), 1)
        self.assertEqual(deliveries[0]["status"], "finalized_exported")
        self.assertEqual(deliveries[0]["tx_id"], "c" * 64)

        proofs = engine.store.list_proofs("mysql-core-ledger")
        self.assertEqual(len(proofs), 1)
        export_path = proofs[0]["export_path"]
        self.assertTrue(export_path)
        self.assertTrue(Path(str(export_path)).exists())

        proof_payload = json.loads(Path(str(export_path)).read_text(encoding="utf-8"))
        self.assertEqual(proof_payload["request_id"], "mysql-request-1")
        self.assertEqual(proof_payload["receipt"]["tx_id"], "c" * 64)
        self.assertEqual(proof_payload["audit_chain"]["record"]["status"], "finalized")

    def test_live_mysql_binlog_full_cycle_exports_proof_bundle(self) -> None:
        start_file, start_pos = self._current_binlog_start()
        self._write_config(
            ingress_port=self.servers[0].server_address[1],
            watcher_port=self.servers[1].server_address[1],
            receipt_port=self.servers[2].server_address[1],
            audit_port=self.servers[3].server_address[1],
            chain_port=self.servers[4].server_address[1],
            capture_mode="binlog",
            binlog_start_file=start_file,
            binlog_start_pos=start_pos,
        )
        self._seed_single_row()
        engine = AgentEngine(load_config(self.config_path))

        result = engine.run_once()

        self.assertEqual(result["processed"], 1)
        self.assertEqual(result["failed"], 0)
        deliveries = engine.store.list_deliveries("mysql-core-ledger")
        self.assertEqual(len(deliveries), 1)
        self.assertEqual(deliveries[0]["status"], "finalized_exported")
        checkpoint = engine.checkpoint_summary()[0]
        checkpoint_state = json.loads(checkpoint["token"])
        self.assertEqual(checkpoint_state["mode"], "binlog")

        proofs = engine.store.list_proofs("mysql-core-ledger")
        self.assertEqual(len(proofs), 1)
        export_path = proofs[0]["export_path"]
        self.assertTrue(export_path)
        self.assertTrue(Path(str(export_path)).exists())

        proof_payload = json.loads(Path(str(export_path)).read_text(encoding="utf-8"))
        self.assertEqual(proof_payload["request_id"], "mysql-request-1")
        self.assertEqual(proof_payload["receipt"]["mode"], "single")

    def test_live_mysql_batch_full_cycle_exports_batch_bundle(self) -> None:
        self._write_config(
            ingress_port=self.servers[0].server_address[1],
            watcher_port=self.servers[1].server_address[1],
            receipt_port=self.servers[2].server_address[1],
            audit_port=self.servers[3].server_address[1],
            chain_port=self.servers[4].server_address[1],
            batch_enabled=True,
        )
        self._seed_batch_rows()
        engine = AgentEngine(load_config(self.config_path))

        result = engine.run_once()

        self.assertEqual(result["processed"], 2)
        self.assertEqual(result["failed"], 0)
        deliveries = engine.store.list_deliveries("mysql-core-ledger")
        self.assertEqual(len(deliveries), 1)
        self.assertEqual(deliveries[0]["status"], "finalized_exported")

        proofs = engine.store.list_proofs("mysql-core-ledger")
        self.assertEqual(len(proofs), 1)
        export_path = proofs[0]["export_path"]
        self.assertTrue(export_path)
        self.assertTrue(Path(str(export_path)).exists())

        proof_payload = json.loads(Path(str(export_path)).read_text(encoding="utf-8"))
        self.assertEqual(proof_payload["request_id"], "mysql-batch-request-1")
        self.assertEqual(proof_payload["mode"], "batch")
        self.assertEqual(len(proof_payload["members"]), 2)
