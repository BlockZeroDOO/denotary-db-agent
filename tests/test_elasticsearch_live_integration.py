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
from elasticsearch_live_support import (
    agent_connection_config,
    create_client,
    free_port,
    missing_elasticsearch_env,
    wait_for_index_visibility,
)


class MockIngressHandler(BaseHTTPRequestHandler):
    counter = 0

    def do_POST(self) -> None:  # noqa: N802
        MockIngressHandler.counter += 1
        body = self.rfile.read(int(self.headers.get("Content-Length", "0"))).decode("utf-8")
        payload = json.loads(body)
        response = {
            "request_id": f"elasticsearch-live-request-{MockIngressHandler.counter}",
            "trace_id": f"elasticsearch-live-trace-{MockIngressHandler.counter}",
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


class ElasticsearchLiveIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        missing = missing_elasticsearch_env()
        if missing:
            raise unittest.SkipTest(f"missing Elasticsearch live env: {', '.join(missing)}")

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
        self.index_name = "denotary-agent-live-orders"
        self._create_index()
        self.config_path = Path(self.temp_dir.name) / "config.json"
        self._write_config()

    def tearDown(self) -> None:
        self._drop_index()
        self.ingress_server.shutdown()
        self.watcher_server.shutdown()
        self.ingress_server.server_close()
        self.watcher_server.server_close()
        for thread in self.threads:
            thread.join(timeout=2)
        self.temp_dir.cleanup()

    def _create_index(self) -> None:
        client = create_client()
        try:
            if client.indices.exists(index=self.index_name):
                client.indices.delete(index=self.index_name)
            client.indices.create(
                index=self.index_name,
                mappings={
                    "properties": {
                        "record_id": {"type": "keyword"},
                        "status": {"type": "keyword"},
                        "updated_at": {"type": "date"},
                    }
                },
            )
            client.indices.refresh(index=self.index_name)
        finally:
            client.close()
        wait_for_index_visibility(self.index_name)

    def _drop_index(self) -> None:
        client = create_client()
        try:
            if client.indices.exists(index=self.index_name):
                client.indices.delete(index=self.index_name)
        finally:
            client.close()

    def _write_config(self, backfill_mode: str = "full", row_limit: int = 100) -> None:
        config = {
            "agent_name": "live-elasticsearch-test",
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
                    "id": "elasticsearch-orders",
                    "adapter": "elasticsearch",
                    "enabled": True,
                    "source_instance": "search-eu-1",
                    "database_name": "search",
                    "include": {
                        "default": [self.index_name],
                    },
                    "connection": agent_connection_config(),
                    "backfill_mode": backfill_mode,
                    "batch_size": 100,
                    "options": {
                        "capture_mode": "watermark",
                        "watermark_field": "updated_at",
                        "commit_timestamp_field": "updated_at",
                        "primary_key_field": "record_id",
                        "row_limit": row_limit,
                    },
                }
            ],
        }
        self.config_path.write_text(json.dumps(config), encoding="utf-8")

    def _seed_initial_rows(self) -> None:
        client = create_client()
        try:
            client.index(index=self.index_name, id="1001", document={"record_id": "1001", "status": "draft", "updated_at": "2026-04-20T12:00:01Z"})
            client.index(index=self.index_name, id="1002", document={"record_id": "1002", "status": "issued", "updated_at": "2026-04-20T12:00:03Z"})
            client.indices.refresh(index=self.index_name)
        finally:
            client.close()

    def _insert_incremental_row(self) -> None:
        client = create_client()
        try:
            client.index(index=self.index_name, id="1003", document={"record_id": "1003", "status": "paid", "updated_at": "2026-04-20T12:00:04Z"})
            client.indices.refresh(index=self.index_name)
        finally:
            client.close()

    def test_live_elasticsearch_snapshot_and_resume(self) -> None:
        self._seed_initial_rows()
        engine = AgentEngine(load_config(self.config_path))

        first_result = engine.run_once()
        self.assertEqual(first_result["processed"], 2)
        self.assertEqual(first_result["failed"], 0)
        self.assertEqual(len(MockWatcherHandler.registrations), 2)

        checkpoints = engine.checkpoint_summary()
        self.assertEqual(len(checkpoints), 1)
        checkpoint_state = json.loads(checkpoints[0]["token"])
        object_key = f"default.{self.index_name}"
        self.assertEqual(checkpoint_state[object_key]["pk"], "1002")

        self._insert_incremental_row()
        second_result = engine.run_once()
        self.assertEqual(second_result["processed"], 1)
        self.assertEqual(second_result["failed"], 0)
        self.assertEqual(len(MockWatcherHandler.registrations), 3)

    def test_live_elasticsearch_validate_bootstrap_and_inspect(self) -> None:
        engine = AgentEngine(load_config(self.config_path))

        validation = engine.validate()
        self.assertEqual(validation[0]["source_type"], "elasticsearch")
        self.assertEqual(validation[0]["supports_snapshot"], "true")
        self.assertEqual(validation[0]["capture_modes"], ["watermark"])

        bootstrap = engine.bootstrap("elasticsearch-orders")
        self.assertEqual(bootstrap["sources"][0]["capture_mode"], "watermark")
        self.assertEqual(len(bootstrap["sources"][0]["tracked_tables"]), 1)

        inspect = engine.inspect("elasticsearch-orders")
        self.assertEqual(inspect["sources"][0]["capture_mode"], "watermark")
        tracked = inspect["sources"][0]["tracked_tables"][0]
        self.assertEqual(tracked["table_name"], self.index_name)
        self.assertIn("updated_at", tracked["selected_columns"])

    def test_live_elasticsearch_backfill_none_skips_until_new_rows_exist(self) -> None:
        self._write_config(backfill_mode="none")
        engine = AgentEngine(load_config(self.config_path))

        empty_result = engine.run_once()
        self.assertEqual(empty_result["processed"], 0)
        self.assertEqual(empty_result["failed"], 0)

        self._seed_initial_rows()
        result = engine.run_once()
        self.assertEqual(result["processed"], 0)
        self.assertEqual(result["failed"], 0)


if __name__ == "__main__":
    unittest.main()
