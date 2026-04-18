from __future__ import annotations

import json
import subprocess
import tempfile
import threading
import time
import unittest
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytds

from denotary_db_agent.config import load_config
from denotary_db_agent.engine import AgentEngine
from sqlserver_live_support import (
    PROJECT_ROOT,
    SQLSERVER_COMPOSE_FILE,
    SQLSERVER_INIT_SQL,
    SQLSERVER_PORT,
    free_port,
    split_tsql_batches,
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


class SqlServerLiveIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._docker_compose("down", "-v")
        cls._docker_compose("up", "-d")
        cls._wait_for_sqlserver()
        cls._apply_init_sql()

    @classmethod
    def tearDownClass(cls) -> None:
        cls._docker_compose("down", "-v")

    @classmethod
    def _docker_compose(cls, *args: str) -> None:
        subprocess.run(
            ["docker", "compose", "-f", str(SQLSERVER_COMPOSE_FILE), *args],
            check=True,
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
        )

    @classmethod
    def _wait_for_sqlserver(cls) -> None:
        deadline = time.time() + 120
        while time.time() < deadline:
            try:
                connection = cls._connect(database="master")
                with connection.cursor() as cursor:
                    cursor.execute("select 1 as ok")
                    cursor.fetchone()
                connection.close()
                return
            except Exception:
                time.sleep(1)
        raise RuntimeError("sqlserver live container did not become ready in time")

    @classmethod
    def _apply_init_sql(cls) -> None:
        connection = cls._connect(database="master")
        try:
            with connection.cursor() as cursor:
                for batch in split_tsql_batches(SQLSERVER_INIT_SQL.read_text(encoding="utf-8")):
                    cursor.execute(batch)
            connection.commit()
        finally:
            connection.close()

    @classmethod
    def _connect(cls, database: str = "ledger"):
        return pytds.connect(
            dsn="127.0.0.1",
            port=SQLSERVER_PORT,
            database=database,
            user="sa",
            password="StrongP@ssw0rd!",
            as_dict=True,
            autocommit=True,
            login_timeout=10,
            timeout=10,
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
            with connection.cursor() as cursor:
                cursor.execute("delete from dbo.payments")
                cursor.execute("delete from dbo.invoices")
        finally:
            connection.close()

    def _write_config(self, backfill_mode: str = "full", row_limit: int = 100) -> None:
        config = {
            "agent_name": "live-sqlserver-test",
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
                    "id": "sqlserver-core-ledger",
                    "adapter": "sqlserver",
                    "enabled": True,
                    "source_instance": "erp-eu-1",
                    "database_name": "ledger",
                    "include": {"dbo": ["invoices", "payments"]},
                    "connection": {
                        "host": "127.0.0.1",
                        "port": SQLSERVER_PORT,
                        "username": "sa",
                        "password": "StrongP@ssw0rd!",
                        "database": "ledger",
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
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    insert into dbo.invoices (id, status, amount, updated_at)
                    values
                        (1001, N'draft', 100.00, '2026-04-17T10:00:01'),
                        (1002, N'issued', 250.00, '2026-04-17T10:00:03')
                    """
                )
                cursor.execute(
                    """
                    insert into dbo.payments (id, invoice_id, amount, updated_at)
                    values
                        (2001, 1002, 250.00, '2026-04-17T10:00:02')
                    """
                )
        finally:
            connection.close()

    def _insert_incremental_row(self) -> None:
        connection = self._connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    insert into dbo.payments (id, invoice_id, amount, updated_at)
                    values (2002, 1001, 100.00, '2026-04-17T10:00:04')
                    """
                )
        finally:
            connection.close()

    def _alter_invoices_add_column(self, column_name: str) -> None:
        connection = self._connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"alter table dbo.invoices add [{column_name}] nvarchar(255) null")
        finally:
            connection.close()

    def _alter_invoices_drop_column(self, column_name: str) -> None:
        connection = self._connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"alter table dbo.invoices drop column [{column_name}]")
        finally:
            connection.close()

    def test_live_sqlserver_snapshot_and_resume(self) -> None:
        self._seed_initial_rows()
        engine = AgentEngine(load_config(self.config_path))

        first_result = engine.run_once()
        self.assertEqual(first_result["processed"], 3)
        self.assertEqual(first_result["failed"], 0)
        self.assertEqual(len(MockWatcherHandler.registrations), 3)
        checkpoints = engine.checkpoint_summary()
        self.assertEqual(len(checkpoints), 1)
        checkpoint_state = json.loads(checkpoints[0]["token"])
        self.assertEqual(checkpoint_state["dbo.invoices"]["watermark"], "2026-04-17T10:00:03Z")
        self.assertEqual(checkpoint_state["dbo.payments"]["watermark"], "2026-04-17T10:00:02Z")

        self._insert_incremental_row()
        second_result = engine.run_once()
        self.assertEqual(second_result["processed"], 1)
        self.assertEqual(second_result["failed"], 0)
        self.assertEqual(len(MockWatcherHandler.registrations), 4)
        deliveries = engine.store.list_deliveries("sqlserver-core-ledger")
        self.assertEqual(len(deliveries), 4)

    def test_live_sqlserver_validate_bootstrap_and_inspect(self) -> None:
        engine = AgentEngine(load_config(self.config_path))

        validation = engine.validate()
        self.assertEqual(validation[0]["source_type"], "sqlserver")
        self.assertEqual(validation[0]["supports_snapshot"], "true")
        self.assertEqual(validation[0]["capture_modes"], ["watermark"])

        bootstrap = engine.bootstrap("sqlserver-core-ledger")
        self.assertEqual(bootstrap["sources"][0]["capture_mode"], "watermark")
        self.assertEqual(len(bootstrap["sources"][0]["tracked_tables"]), 2)

        inspect = engine.inspect("sqlserver-core-ledger")
        self.assertEqual(inspect["sources"][0]["capture_mode"], "watermark")
        invoices = next(
            item
            for item in inspect["sources"][0]["tracked_tables"]
            if item["schema_name"] == "dbo" and item["table_name"] == "invoices"
        )
        self.assertIn("updated_at", invoices["selected_columns"])

    def test_live_sqlserver_auto_refreshes_runtime_signature_after_schema_drift(self) -> None:
        engine = AgentEngine(load_config(self.config_path))
        bootstrap = engine.bootstrap("sqlserver-core-ledger")
        self.assertEqual(bootstrap["sources"][0]["capture_mode"], "watermark")

        original_signature = engine.store.get_runtime_signature("sqlserver-core-ledger")
        self.assertIsNotNone(original_signature)

        drift_column = "denotary_runtime_drift_note"
        try:
            self._alter_invoices_add_column(drift_column)
            refresh_result = engine.run_once()
            self.assertEqual(refresh_result["processed"], 0)
            self.assertEqual(refresh_result["failed"], 0)

            refreshed_signature = engine.store.get_runtime_signature("sqlserver-core-ledger")
            self.assertIsNotNone(refreshed_signature)
            self.assertNotEqual(original_signature, refreshed_signature)

            inspect = engine.inspect("sqlserver-core-ledger")
            invoices = next(
                item
                for item in inspect["sources"][0]["tracked_tables"]
                if item["schema_name"] == "dbo" and item["table_name"] == "invoices"
            )
            self.assertIn(drift_column, invoices["selected_columns"])
        finally:
            self._alter_invoices_drop_column(drift_column)

    def test_live_sqlserver_backfill_none_skips_until_new_rows_exist(self) -> None:
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
