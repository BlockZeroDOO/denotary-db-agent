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
from db2_live_support import (
    agent_connection_config,
    create_connection,
    db2_schema,
    free_port,
    missing_db2_env,
    unique_table_name,
    wait_for_table_visibility,
)


class MockIngressHandler(BaseHTTPRequestHandler):
    counter = 0

    def do_POST(self) -> None:  # noqa: N802
        MockIngressHandler.counter += 1
        body = self.rfile.read(int(self.headers.get("Content-Length", "0"))).decode("utf-8")
        payload = json.loads(body)
        response = {
            "request_id": f"db2-live-request-{MockIngressHandler.counter}",
            "trace_id": f"db2-live-trace-{MockIngressHandler.counter}",
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


class Db2LiveIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        missing = missing_db2_env()
        if missing:
            raise unittest.SkipTest(f"missing IBM Db2 live env: {', '.join(missing)}")

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
        self.table_name = unique_table_name()
        self._create_table()
        self.config_path = Path(self.temp_dir.name) / "config.json"
        self._write_config()

    def tearDown(self) -> None:
        self._drop_table()
        self.ingress_server.shutdown()
        self.watcher_server.shutdown()
        self.ingress_server.server_close()
        self.watcher_server.server_close()
        for thread in self.threads:
            thread.join(timeout=2)
        self.temp_dir.cleanup()

    def _qualified_table(self) -> str:
        return f'{db2_schema()}.{self.table_name}'

    def _create_table(self) -> None:
        connection = create_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    create table {self._qualified_table()} (
                        ID integer not null,
                        STATUS varchar(32),
                        UPDATED_AT timestamp,
                        primary key (ID)
                    )
                    """
                )
            connection.commit()
        finally:
            connection.close()
        wait_for_table_visibility(self.table_name)

    def _drop_table(self) -> None:
        connection = create_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"drop table {self._qualified_table()}")
            connection.commit()
        except Exception:
            connection.rollback()
        finally:
            connection.close()

    def _write_config(self, backfill_mode: str = "full", row_limit: int = 100) -> None:
        config = {
            "agent_name": "live-db2-test",
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
                    "id": "db2-ledger-core",
                    "adapter": "db2",
                    "enabled": True,
                    "source_instance": "erp-eu-1",
                    "database_name": agent_connection_config()["database"],
                    "include": {
                        db2_schema(): [self.table_name],
                    },
                    "connection": agent_connection_config(),
                    "backfill_mode": backfill_mode,
                    "batch_size": 100,
                    "options": {
                        "capture_mode": "watermark",
                        "watermark_column": "UPDATED_AT",
                        "commit_timestamp_column": "UPDATED_AT",
                        "primary_key_columns": ["ID"],
                        "row_limit": row_limit,
                    },
                }
            ],
        }
        self.config_path.write_text(json.dumps(config), encoding="utf-8")

    def _seed_initial_rows(self) -> None:
        connection = create_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    insert into {self._qualified_table()} (ID, STATUS, UPDATED_AT)
                    values (1001, 'draft', '2026-04-20-12.00.01'),
                           (1002, 'issued', '2026-04-20-12.00.03')
                    """
                )
            connection.commit()
        finally:
            connection.close()

    def _insert_incremental_row(self) -> None:
        connection = create_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""
                    insert into {self._qualified_table()} (ID, STATUS, UPDATED_AT)
                    values (1003, 'paid', '2026-04-20-12.00.04')
                    """
                )
            connection.commit()
        finally:
            connection.close()

    def test_live_db2_snapshot_and_resume(self) -> None:
        self._seed_initial_rows()
        engine = AgentEngine(load_config(self.config_path))

        first_result = engine.run_once()
        self.assertEqual(first_result["processed"], 2)
        self.assertEqual(first_result["failed"], 0)
        self.assertEqual(len(MockWatcherHandler.registrations), 2)

        checkpoints = engine.checkpoint_summary()
        self.assertEqual(len(checkpoints), 1)
        checkpoint_state = json.loads(checkpoints[0]["token"])
        object_key = f"{db2_schema()}.{self.table_name}"
        self.assertEqual(checkpoint_state[object_key]["pk"], [1002])

        self._insert_incremental_row()
        second_result = engine.run_once()
        self.assertEqual(second_result["processed"], 1)
        self.assertEqual(second_result["failed"], 0)
        self.assertEqual(len(MockWatcherHandler.registrations), 3)

    def test_live_db2_validate_bootstrap_and_inspect(self) -> None:
        engine = AgentEngine(load_config(self.config_path))

        validation = engine.validate()
        self.assertEqual(validation[0]["source_type"], "db2")
        self.assertEqual(validation[0]["supports_snapshot"], "true")
        self.assertEqual(validation[0]["capture_modes"], ["watermark"])

        bootstrap = engine.bootstrap("db2-ledger-core")
        self.assertEqual(bootstrap["sources"][0]["capture_mode"], "watermark")
        self.assertEqual(len(bootstrap["sources"][0]["tracked_tables"]), 1)

        inspect = engine.inspect("db2-ledger-core")
        self.assertEqual(inspect["sources"][0]["capture_mode"], "watermark")
        tracked = inspect["sources"][0]["tracked_tables"][0]
        self.assertEqual(tracked["table_name"], self.table_name)
        self.assertIn("UPDATED_AT", tracked["selected_columns"])

    def test_live_db2_backfill_none_skips_until_new_rows_exist(self) -> None:
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
