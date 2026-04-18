from __future__ import annotations

import json
import subprocess
import tempfile
import threading
import time
import unittest
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from bson import ObjectId
from pymongo import MongoClient

from denotary_db_agent.config import load_config
from denotary_db_agent.engine import AgentEngine
from mongodb_live_support import MONGODB_COMPOSE_FILE, MONGODB_PORT, MONGODB_URI, PROJECT_ROOT, free_port, wait_for_mongodb_replica_set


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


class MongoDbLiveIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._docker_compose("down", "-v")
        cls._docker_compose("up", "-d")
        wait_for_mongodb_replica_set()

    @classmethod
    def tearDownClass(cls) -> None:
        cls._docker_compose("down", "-v")

    @classmethod
    def _docker_compose(cls, *args: str) -> None:
        subprocess.run(
            ["docker", "compose", "-f", str(MONGODB_COMPOSE_FILE), *args],
            check=True,
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
        )

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
        self._truncate_collections()
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

    def _connect(self) -> MongoClient:
        return MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)

    def _truncate_collections(self) -> None:
        client = self._connect()
        try:
            database = client["ledger"]
            existing = set(database.list_collection_names())
            if "invoices" not in existing:
                database.create_collection("invoices")
            if "payments" not in existing:
                database.create_collection("payments")
            database["payments"].delete_many({})
            database["invoices"].delete_many({})
        finally:
            client.close()

    def _write_config(self, backfill_mode: str = "full", row_limit: int = 100) -> None:
        config = {
            "agent_name": "live-mongodb-test",
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
                    "id": "mongodb-core-ledger",
                    "adapter": "mongodb",
                    "enabled": True,
                    "source_instance": "erp-eu-1",
                    "database_name": "ledger",
                    "include": {"ledger": ["invoices", "payments"]},
                    "connection": {
                        "uri": MONGODB_URI,
                    },
                    "backfill_mode": backfill_mode,
                    "batch_size": 100,
                    "options": {
                        "capture_mode": "watermark",
                        "watermark_column": "updated_at",
                        "commit_timestamp_column": "updated_at",
                        "row_limit": row_limit,
                    },
                }
            ],
        }
        self.config_path.write_text(json.dumps(config), encoding="utf-8")

    def _seed_initial_documents(self) -> None:
        client = self._connect()
        try:
            client["ledger"]["invoices"].insert_many(
                [
                    {"status": "draft", "amount": 100.0, "updated_at": datetime(2026, 4, 17, 10, 0, 1, tzinfo=timezone.utc)},
                    {"status": "issued", "amount": 250.0, "updated_at": datetime(2026, 4, 17, 10, 0, 3, tzinfo=timezone.utc)},
                ]
            )
            client["ledger"]["payments"].insert_one(
                {"invoice_id": 1, "amount": 250.0, "updated_at": datetime(2026, 4, 17, 10, 0, 2, tzinfo=timezone.utc)}
            )
        finally:
            client.close()

    def _insert_incremental_document(self) -> ObjectId:
        client = self._connect()
        try:
            result = client["ledger"]["payments"].insert_one(
                {"invoice_id": 2, "amount": 100.0, "updated_at": datetime(2026, 4, 17, 10, 0, 4, tzinfo=timezone.utc)}
            )
            return result.inserted_id
        finally:
            client.close()

    def test_live_mongodb_snapshot_and_resume(self) -> None:
        self._seed_initial_documents()
        engine = AgentEngine(load_config(self.config_path))

        first_result = engine.run_once()
        self.assertEqual(first_result["processed"], 3)
        self.assertEqual(first_result["failed"], 0)
        self.assertEqual(len(MockWatcherHandler.registrations), 3)
        checkpoints = engine.checkpoint_summary()
        self.assertEqual(len(checkpoints), 1)
        checkpoint_state = json.loads(checkpoints[0]["token"])
        self.assertIn("ledger.invoices", checkpoint_state)
        self.assertIn("ledger.payments", checkpoint_state)
        self.assertEqual(checkpoint_state["ledger.invoices"]["watermark_type"], "datetime")
        self.assertEqual(checkpoint_state["ledger.payments"]["pk_type"], "objectid")

        inserted_id = self._insert_incremental_document()
        second_result = engine.run_once()
        self.assertEqual(second_result["processed"], 1)
        self.assertEqual(second_result["failed"], 0)
        self.assertEqual(len(MockWatcherHandler.registrations), 4)
        deliveries = engine.store.list_deliveries("mongodb-core-ledger")
        self.assertEqual(len(deliveries), 4)

        latest_checkpoint = json.loads(engine.checkpoint_summary()[0]["token"])
        self.assertEqual(latest_checkpoint["ledger.payments"]["pk"], str(inserted_id))

    def test_live_mongodb_validate_bootstrap_and_inspect(self) -> None:
        engine = AgentEngine(load_config(self.config_path))

        validation = engine.validate()
        self.assertEqual(validation[0]["source_type"], "mongodb")
        self.assertEqual(validation[0]["supports_snapshot"], "true")
        self.assertEqual(validation[0]["capture_modes"], ["watermark", "change_streams"])

        bootstrap = engine.bootstrap("mongodb-core-ledger")
        self.assertEqual(bootstrap["sources"][0]["capture_mode"], "watermark")
        self.assertEqual(len(bootstrap["sources"][0]["tracked_collections"]), 2)

        inspect = engine.inspect("mongodb-core-ledger")
        self.assertEqual(inspect["sources"][0]["capture_mode"], "watermark")
        invoices = next(
            item
            for item in inspect["sources"][0]["tracked_collections"]
            if item["database_name"] == "ledger" and item["collection_name"] == "invoices"
        )
        self.assertEqual(invoices["primary_key_field"], "_id")

    def test_live_mongodb_backfill_none_skips_until_new_documents_exist(self) -> None:
        self._write_config(backfill_mode="none")
        engine = AgentEngine(load_config(self.config_path))

        empty_result = engine.run_once()
        self.assertEqual(empty_result["processed"], 0)
        self.assertEqual(empty_result["failed"], 0)

        self._seed_initial_documents()
        result = engine.run_once()
        self.assertEqual(result["processed"], 0)
        self.assertEqual(result["failed"], 0)

    def test_live_mongodb_snapshot_normalizes_objectid_primary_keys(self) -> None:
        client = self._connect()
        try:
            inserted_id = client["ledger"]["invoices"].insert_one(
                {"status": "issued", "amount": 123.45, "updated_at": datetime(2026, 4, 18, 11, 22, 33, tzinfo=timezone.utc)}
            ).inserted_id
        finally:
            client.close()

        engine = AgentEngine(load_config(self.config_path))
        result = engine.run_once()
        self.assertEqual(result["processed"], 1)
        deliveries = engine.store.list_deliveries("mongodb-core-ledger")
        self.assertEqual(len(deliveries), 1)
        event_checkpoint = json.loads(engine.checkpoint_summary()[0]["token"])
        self.assertEqual(event_checkpoint["ledger.invoices"]["pk"], str(inserted_id))

    def test_live_mongodb_change_streams_capture_insert_update_delete(self) -> None:
        self._write_config()
        raw = json.loads(self.config_path.read_text(encoding="utf-8"))
        raw["sources"][0]["backfill_mode"] = "none"
        raw["sources"][0]["options"]["capture_mode"] = "change_streams"
        self.config_path.write_text(json.dumps(raw), encoding="utf-8")
        engine = AgentEngine(load_config(self.config_path))

        empty_result = engine.run_once()
        self.assertEqual(empty_result["processed"], 0)
        self.assertEqual(empty_result["failed"], 0)

        client = self._connect()
        try:
            inserted_id = client["ledger"]["invoices"].insert_one(
                {"status": "draft", "amount": 10.0, "updated_at": datetime(2026, 4, 18, 12, 0, 0, tzinfo=timezone.utc)}
            ).inserted_id
            client["ledger"]["invoices"].update_one(
                {"_id": inserted_id},
                {"$set": {"status": "issued", "updated_at": datetime(2026, 4, 18, 12, 0, 1, tzinfo=timezone.utc)}},
            )
            client["ledger"]["invoices"].delete_one({"_id": inserted_id})
        finally:
            client.close()

        time.sleep(1)
        result = engine.run_once()
        self.assertEqual(result["processed"], 3)
        self.assertEqual(result["failed"], 0)
        deliveries = engine.store.list_deliveries("mongodb-core-ledger")
        self.assertEqual(len(deliveries), 3)


if __name__ == "__main__":
    unittest.main()
