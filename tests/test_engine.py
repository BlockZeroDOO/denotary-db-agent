from __future__ import annotations

import json
import socket
import tempfile
import threading
import unittest
import urllib.parse
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


class MockIngressHandler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802
        body = self.rfile.read(int(self.headers.get("Content-Length", "0"))).decode("utf-8")
        payload = json.loads(body)
        response = {
            "request_id": "request-1",
            "trace_id": "trace-1",
            "external_ref_hash": "1" * 64,
            "object_hash": "2" * 64,
            "verification_account": "verif",
            "prepared_action": {
                "contract": "verifbill",
                "action": "submit",
                "data": {
                    "payer": payload["submitter"],
                    "submitter": payload["submitter"],
                    "schema_id": 1,
                    "policy_id": 1,
                    "object_hash": "2" * 64,
                    "external_ref": "1" * 64,
                },
            },
        }
        body_out = json.dumps(response).encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body_out)))
        self.end_headers()
        self.wfile.write(body_out)

    def log_message(self, format: str, *args: Any) -> None:
        return


class MockWatcherHandler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802
        if self.path != "/v1/watch/register":
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        body = self.rfile.read(int(self.headers.get("Content-Length", "0"))).decode("utf-8")
        payload = json.loads(body)
        response = {"ok": True, "request_id": payload["request_id"]}
        body_out = json.dumps(response).encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body_out)))
        self.end_headers()
        self.wfile.write(body_out)

    def log_message(self, format: str, *args: Any) -> None:
        return


class EngineTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        ingress_port = free_port()
        watcher_port = free_port()
        self.ingress_server = ThreadingHTTPServer(("127.0.0.1", ingress_port), MockIngressHandler)
        self.watcher_server = ThreadingHTTPServer(("127.0.0.1", watcher_port), MockWatcherHandler)
        self.threads = [
            threading.Thread(target=self.ingress_server.serve_forever, daemon=True),
            threading.Thread(target=self.watcher_server.serve_forever, daemon=True),
        ]
        for thread in self.threads:
            thread.start()

        self.config_path = Path(self.temp_dir.name) / "config.json"
        config = {
            "agent_name": "test-agent",
            "log_level": "INFO",
            "denotary": {
                "ingress_url": f"http://127.0.0.1:{ingress_port}",
                "watcher_url": f"http://127.0.0.1:{watcher_port}",
                "watcher_auth_token": "token",
                "submitter": "enterpriseac1",
                "schema_id": 1,
                "policy_id": 1
            },
            "storage": {
                "state_db": str(Path(self.temp_dir.name) / "state.sqlite3")
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
                        "database": "ledger"
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
                                "metadata": {"txid": 100}
                            }
                        ]
                    }
                }
            ]
        }
        self.config_path.write_text(json.dumps(config), encoding="utf-8")

    def tearDown(self) -> None:
        self.ingress_server.shutdown()
        self.watcher_server.shutdown()
        self.ingress_server.server_close()
        self.watcher_server.server_close()
        for thread in self.threads:
            thread.join(timeout=2)
        self.temp_dir.cleanup()

    def test_run_once_processes_event_and_updates_checkpoint(self) -> None:
        engine = AgentEngine(load_config(self.config_path))
        result = engine.run_once()
        self.assertEqual(result["processed"], 1)
        self.assertEqual(result["failed"], 0)
        checkpoints = engine.checkpoint_summary()
        self.assertEqual(len(checkpoints), 1)
        self.assertEqual(checkpoints[0]["token"], "lsn:1")
        deliveries = engine.store.list_deliveries("pg-core-ledger")
        self.assertEqual(len(deliveries), 1)
        self.assertEqual(deliveries[0]["status"], "prepared_registered")

    def test_run_forever_stops_after_max_loops(self) -> None:
        engine = AgentEngine(load_config(self.config_path))
        result = engine.run_forever(interval_sec=0.01, max_loops=2)
        self.assertEqual(result["processed"], 2)
        self.assertEqual(result["failed"], 0)
        self.assertEqual(result["loops"], 2)

    def test_pause_and_resume_source_controls_processing(self) -> None:
        engine = AgentEngine(load_config(self.config_path))
        engine.pause_source("pg-core-ledger")
        paused_status = engine.status()
        self.assertTrue(paused_status["sources"][0]["paused"])

        paused_result = engine.run_once()
        self.assertEqual(paused_result["processed"], 0)
        self.assertEqual(paused_result["failed"], 0)

        engine.resume_source("pg-core-ledger")
        resumed_status = engine.health()
        self.assertFalse(resumed_status["sources"][0]["paused"])

        resumed_result = engine.run_once()
        self.assertEqual(resumed_result["processed"], 1)
        self.assertEqual(resumed_result["failed"], 0)
