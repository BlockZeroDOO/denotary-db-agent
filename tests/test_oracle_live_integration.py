from __future__ import annotations

import json
import subprocess
import tempfile
import threading
import time
import unittest
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import oracledb

from denotary_db_agent.config import load_config
from denotary_db_agent.engine import AgentEngine
from oracle_live_support import (
    ORACLE_COMPOSE_FILE,
    ORACLE_INIT_SQL,
    ORACLE_PASSWORD,
    ORACLE_PORT,
    ORACLE_SERVICE_NAME,
    ORACLE_USERNAME,
    PROJECT_ROOT,
    free_port,
    split_oracle_blocks,
)


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


class OracleLiveIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._docker_compose("down", "-v")
        cls._docker_compose("up", "-d")
        cls._wait_for_oracle()
        cls._apply_init_sql()

    @classmethod
    def tearDownClass(cls) -> None:
        cls._docker_compose("down", "-v")

    @classmethod
    def _docker_compose(cls, *args: str) -> None:
        subprocess.run(
            ["docker", "compose", "-f", str(ORACLE_COMPOSE_FILE), *args],
            check=True,
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
        )

    @classmethod
    def _wait_for_oracle(cls) -> None:
        deadline = time.time() + 420
        while time.time() < deadline:
            try:
                connection = cls._connect()
                with connection.cursor() as cursor:
                    cursor.execute("select 1 from dual")
                    cursor.fetchone()
                connection.close()
                return
            except Exception:
                time.sleep(5)
        raise RuntimeError("oracle live container did not become ready in time")

    @classmethod
    def _apply_init_sql(cls) -> None:
        connection = cls._connect()
        try:
            cursor = connection.cursor()
            for block in split_oracle_blocks(ORACLE_INIT_SQL.read_text(encoding="utf-8")):
                cursor.execute(block)
            connection.commit()
        finally:
            connection.close()

    @classmethod
    def _connect(cls):
        return oracledb.connect(
            user=ORACLE_USERNAME,
            password=ORACLE_PASSWORD,
            host="127.0.0.1",
            port=ORACLE_PORT,
            service_name=ORACLE_SERVICE_NAME,
            tcp_connect_timeout=10,
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
        self._truncate_tables()
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

    def _truncate_tables(self) -> None:
        connection = self._connect()
        try:
            cursor = connection.cursor()
            cursor.execute("delete from payments")
            cursor.execute("delete from invoices")
            connection.commit()
        finally:
            connection.close()

    def _write_config(self, backfill_mode: str = "full", row_limit: int = 100) -> None:
        config = {
            "agent_name": "live-oracle-test",
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
                    "id": "oracle-core-ledger",
                    "adapter": "oracle",
                    "enabled": True,
                    "source_instance": "erp-eu-1",
                    "database_name": "ledger",
                    "include": {"DENOTARY": ["INVOICES", "PAYMENTS"]},
                    "connection": {
                        "host": "127.0.0.1",
                        "port": ORACLE_PORT,
                        "username": ORACLE_USERNAME,
                        "password": ORACLE_PASSWORD,
                        "service_name": ORACLE_SERVICE_NAME,
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

    def _seed_initial_rows(self) -> None:
        connection = self._connect()
        try:
            cursor = connection.cursor()
            cursor.executemany(
                """
                insert into invoices (id, status, amount, updated_at)
                values (:1, :2, :3, :4)
                """,
                [
                    (1001, "draft", 100.00, datetime(2026, 4, 17, 10, 0, 1)),
                    (1002, "issued", 250.00, datetime(2026, 4, 17, 10, 0, 3)),
                ],
            )
            cursor.execute(
                """
                insert into payments (id, invoice_id, amount, updated_at)
                values (:1, :2, :3, :4)
                """,
                (2001, 1002, 250.00, datetime(2026, 4, 17, 10, 0, 2)),
            )
            connection.commit()
        finally:
            connection.close()

    def _insert_incremental_row(self) -> None:
        connection = self._connect()
        try:
            cursor = connection.cursor()
            cursor.execute(
                """
                insert into payments (id, invoice_id, amount, updated_at)
                values (:1, :2, :3, :4)
                """,
                (2002, 1001, 100.00, datetime(2026, 4, 17, 10, 0, 4)),
            )
            connection.commit()
        finally:
            connection.close()

    def _alter_invoices_add_column(self, column_name: str) -> None:
        connection = self._connect()
        try:
            cursor = connection.cursor()
            cursor.execute(f'alter table invoices add "{column_name}" varchar2(255 char)')
            connection.commit()
        finally:
            connection.close()

    def _alter_invoices_drop_column(self, column_name: str) -> None:
        connection = self._connect()
        try:
            cursor = connection.cursor()
            cursor.execute(f'alter table invoices drop column "{column_name}"')
            connection.commit()
        finally:
            connection.close()

    def test_live_oracle_snapshot_and_resume(self) -> None:
        self._seed_initial_rows()
        engine = AgentEngine(load_config(self.config_path))

        first_result = engine.run_once()
        self.assertEqual(first_result["processed"], 3)
        self.assertEqual(first_result["failed"], 0)
        self.assertEqual(len(MockWatcherHandler.registrations), 3)
        checkpoints = engine.checkpoint_summary()
        self.assertEqual(len(checkpoints), 1)
        checkpoint_state = json.loads(checkpoints[0]["token"])
        self.assertEqual(checkpoint_state["DENOTARY.INVOICES"]["watermark"], "2026-04-17T10:00:03Z")
        self.assertEqual(checkpoint_state["DENOTARY.PAYMENTS"]["watermark"], "2026-04-17T10:00:02Z")

        self._insert_incremental_row()
        second_result = engine.run_once()
        self.assertEqual(second_result["processed"], 1)
        self.assertEqual(second_result["failed"], 0)
        self.assertEqual(len(MockWatcherHandler.registrations), 4)
        deliveries = engine.store.list_deliveries("oracle-core-ledger")
        self.assertEqual(len(deliveries), 4)

    def test_live_oracle_validate_bootstrap_and_inspect(self) -> None:
        engine = AgentEngine(load_config(self.config_path))

        validation = engine.validate()
        self.assertEqual(validation[0]["source_type"], "oracle")
        self.assertEqual(validation[0]["supports_snapshot"], "true")
        self.assertEqual(validation[0]["capture_modes"], ["watermark"])

        bootstrap = engine.bootstrap("oracle-core-ledger")
        self.assertEqual(bootstrap["sources"][0]["capture_mode"], "watermark")
        self.assertEqual(len(bootstrap["sources"][0]["tracked_tables"]), 2)

        inspect = engine.inspect("oracle-core-ledger")
        self.assertEqual(inspect["sources"][0]["capture_mode"], "watermark")
        invoices = next(
            item
            for item in inspect["sources"][0]["tracked_tables"]
            if item["schema_name"] == "DENOTARY" and item["table_name"] == "INVOICES"
        )
        self.assertIn("UPDATED_AT", invoices["selected_columns"])

    def test_live_oracle_auto_refreshes_runtime_signature_after_schema_drift(self) -> None:
        engine = AgentEngine(load_config(self.config_path))
        bootstrap = engine.bootstrap("oracle-core-ledger")
        self.assertEqual(bootstrap["sources"][0]["capture_mode"], "watermark")

        original_signature = engine.store.get_runtime_signature("oracle-core-ledger")
        self.assertIsNotNone(original_signature)

        drift_column = "DENOTARY_RUNTIME_DRIFT_NOTE"
        try:
            self._alter_invoices_add_column(drift_column)
            refresh_result = engine.run_once()
            self.assertEqual(refresh_result["processed"], 0)
            self.assertEqual(refresh_result["failed"], 0)

            refreshed_signature = engine.store.get_runtime_signature("oracle-core-ledger")
            self.assertIsNotNone(refreshed_signature)
            self.assertNotEqual(original_signature, refreshed_signature)

            inspect = engine.inspect("oracle-core-ledger")
            invoices = next(
                item
                for item in inspect["sources"][0]["tracked_tables"]
                if item["schema_name"] == "DENOTARY" and item["table_name"] == "INVOICES"
            )
            self.assertIn(drift_column, invoices["selected_columns"])
        finally:
            self._alter_invoices_drop_column(drift_column)

    def test_live_oracle_backfill_none_skips_until_new_rows_exist(self) -> None:
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
