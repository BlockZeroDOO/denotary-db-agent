from __future__ import annotations

import json
import tempfile
import threading
import unittest
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from denotary_db_agent.config import load_config
from denotary_db_agent.engine import AgentEngine
from redis_live_support import REDIS_PORT, REDIS_URL, free_port, redis, run_redis_compose, wait_for_redis


class MockIngressHandler(BaseHTTPRequestHandler):
    counter = 0

    def do_POST(self) -> None:  # noqa: N802
        MockIngressHandler.counter += 1
        body = self.rfile.read(int(self.headers.get("Content-Length", "0"))).decode("utf-8")
        payload = json.loads(body)
        response = {
            "request_id": f"request-{MockIngressHandler.counter}",
            "trace_id": f"trace-{MockIngressHandler.counter}",
            "external_ref_hash": f"{MockIngressHandler.counter:064x}",
            "object_hash": f"{MockIngressHandler.counter + 1000:064x}",
            "verification_account": "verif",
            "prepared_action": {
                "contract": "verifbill",
                "action": "submit",
                "data": {
                    "payer": payload["submitter"],
                    "submitter": payload["submitter"],
                    "schema_id": 1,
                    "policy_id": 1,
                    "object_hash": f"{MockIngressHandler.counter + 1000:064x}",
                    "external_ref": f"{MockIngressHandler.counter:064x}",
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


class MockWatcherHandler(BaseHTTPRequestHandler):
    registrations: list[dict[str, Any]] = []

    def do_POST(self) -> None:  # noqa: N802
        if self.path != "/v1/watch/register":
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        body = self.rfile.read(int(self.headers.get("Content-Length", "0"))).decode("utf-8")
        payload = json.loads(body)
        MockWatcherHandler.registrations.append(payload)
        self._send_json({"ok": True, "request_id": payload["request_id"]})

    def _send_json(self, payload: dict[str, Any], status: int = HTTPStatus.OK) -> None:
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, format: str, *args: Any) -> None:
        return


class RedisLiveIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        if redis is None:
            raise unittest.SkipTest("redis package is required for live Redis validation")
        run_redis_compose("down", "-v")
        run_redis_compose("up", "-d")
        wait_for_redis()

    @classmethod
    def tearDownClass(cls) -> None:
        run_redis_compose("down", "-v")

    def setUp(self) -> None:
        MockIngressHandler.counter = 0
        MockWatcherHandler.registrations = []
        self.temp_dir = tempfile.TemporaryDirectory()
        self.ingress_port = free_port()
        self.watcher_port = free_port()
        self.ingress_server = ThreadingHTTPServer(("127.0.0.1", self.ingress_port), MockIngressHandler)
        self.watcher_server = ThreadingHTTPServer(("127.0.0.1", self.watcher_port), MockWatcherHandler)
        self.threads = [
            threading.Thread(target=self.ingress_server.serve_forever, daemon=True),
            threading.Thread(target=self.watcher_server.serve_forever, daemon=True),
        ]
        for thread in self.threads:
            thread.start()
        self._flushdb()
        self.config_path = Path(self.temp_dir.name) / "config.json"
        self._write_config()

    def tearDown(self) -> None:
        self.ingress_server.shutdown()
        self.watcher_server.shutdown()
        self.ingress_server.server_close()
        self.watcher_server.server_close()
        for thread in self.threads:
            thread.join(timeout=2)
        self.temp_dir.cleanup()

    def _connect(self):
        return redis.Redis.from_url(REDIS_URL, decode_responses=False, socket_timeout=5)

    def _flushdb(self) -> None:
        wait_for_redis()
        last_error: Exception | None = None
        for _ in range(3):
            client = self._connect()
            try:
                client.flushdb()
                return
            except Exception as exc:
                last_error = exc
                wait_for_redis()
            finally:
                close = getattr(client, "close", None)
                if callable(close):
                    close()
        if last_error is not None:
            raise last_error

    def _write_config(self, backfill_mode: str = "full", row_limit: int = 100) -> None:
        config = {
            "agent_name": "live-redis-test",
            "log_level": "INFO",
            "denotary": {
                "ingress_url": f"http://127.0.0.1:{self.ingress_port}",
                "watcher_url": f"http://127.0.0.1:{self.watcher_port}",
                "watcher_auth_token": "token",
                "submitter": "enterpriseac1",
                "schema_id": 1,
                "policy_id": 1,
            },
            "storage": {
                "state_db": str(Path(self.temp_dir.name) / "state.sqlite3"),
            },
            "sources": [
                {
                    "id": "redis-cache-core",
                    "adapter": "redis",
                    "enabled": True,
                    "source_instance": "cache-eu-1",
                    "database_name": "db0",
                    "include": {"0": ["orders:*"]},
                    "connection": {
                        "host": "127.0.0.1",
                        "port": REDIS_PORT,
                    },
                    "backfill_mode": backfill_mode,
                    "batch_size": 100,
                    "options": {
                        "capture_mode": "scan",
                        "row_limit": row_limit,
                        "scan_count": 100,
                    },
                }
            ],
        }
        self.config_path.write_text(json.dumps(config), encoding="utf-8")

    def _seed_initial_keys(self) -> None:
        client = self._connect()
        try:
            client.set("orders:101", "issued")
            client.hset("orders:102", mapping={"status": "paid", "currency": "EUR"})
        finally:
            close = getattr(client, "close", None)
            if callable(close):
                close()

    def _insert_incremental_key(self) -> None:
        client = self._connect()
        try:
            client.set("orders:103", "refunded")
        finally:
            close = getattr(client, "close", None)
            if callable(close):
                close()

    def test_live_redis_snapshot_and_resume(self) -> None:
        self._seed_initial_keys()
        engine = AgentEngine(load_config(self.config_path))

        first_result = engine.run_once()
        self.assertEqual(first_result["processed"], 2)
        self.assertEqual(first_result["failed"], 0)
        self.assertEqual(len(MockWatcherHandler.registrations), 2)
        checkpoints = engine.checkpoint_summary()
        self.assertEqual(len(checkpoints), 1)
        checkpoint_state = json.loads(checkpoints[0]["token"])
        self.assertEqual(checkpoint_state["db0:orders:*"]["last_key"], "orders:102")

        self._insert_incremental_key()
        second_result = engine.run_once()
        self.assertEqual(second_result["processed"], 1)
        self.assertEqual(second_result["failed"], 0)
        self.assertEqual(len(MockWatcherHandler.registrations), 3)
        deliveries = engine.store.list_deliveries("redis-cache-core")
        self.assertEqual(len(deliveries), 3)

    def test_live_redis_validate_bootstrap_and_inspect(self) -> None:
        engine = AgentEngine(load_config(self.config_path))

        validation = engine.validate()
        self.assertEqual(validation[0]["source_type"], "redis")
        self.assertEqual(validation[0]["supports_snapshot"], "true")
        self.assertEqual(validation[0]["capture_modes"], ["scan"])

        bootstrap = engine.bootstrap("redis-cache-core")
        self.assertEqual(bootstrap["sources"][0]["capture_mode"], "scan")
        self.assertEqual(len(bootstrap["sources"][0]["tracked_keys"]), 1)

        inspect = engine.inspect("redis-cache-core")
        self.assertEqual(inspect["sources"][0]["capture_mode"], "scan")
        tracked = inspect["sources"][0]["tracked_keys"][0]
        self.assertEqual(tracked["database_number"], 0)
        self.assertEqual(tracked["key_pattern"], "orders:*")

    def test_live_redis_backfill_none_skips_until_new_keys_exist(self) -> None:
        self._write_config(backfill_mode="none")
        engine = AgentEngine(load_config(self.config_path))

        empty_result = engine.run_once()
        self.assertEqual(empty_result["processed"], 0)
        self.assertEqual(empty_result["failed"], 0)

        self._seed_initial_keys()
        result = engine.run_once()
        self.assertEqual(result["processed"], 0)
        self.assertEqual(result["failed"], 0)


if __name__ == "__main__":
    unittest.main()
