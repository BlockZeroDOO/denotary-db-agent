from __future__ import annotations

import json
import socket
import subprocess
import tempfile
import threading
import time
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pymysql

from denotary_db_agent.config import load_config
from denotary_db_agent.engine import AgentEngine


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MYSQL_COMPOSE_FILE = PROJECT_ROOT / "deploy" / "mysql-live" / "docker-compose.yml"
MYSQL_PORT = 53306


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as handle:
        handle.bind(("127.0.0.1", 0))
        return int(handle.getsockname()[1])


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


class MySqlLiveIntegrationTest(unittest.TestCase):
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
        backfill_mode: str = "full",
        row_limit: int = 100,
        capture_mode: str = "watermark",
        binlog_start_file: str = "",
        binlog_start_pos: int = 4,
    ) -> None:
        config = {
            "agent_name": "live-mysql-test",
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
                    "backfill_mode": backfill_mode,
                    "batch_size": 100,
                    "options": {
                        "capture_mode": capture_mode,
                        "watermark_column": "updated_at",
                        "commit_timestamp_column": "updated_at",
                        "row_limit": row_limit,
                        "binlog_server_id": 14101,
                        "binlog_start_file": binlog_start_file,
                        "binlog_start_pos": binlog_start_pos,
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
                    insert into invoices (id, status, amount, updated_at)
                    values
                        (1001, 'draft', 100.00, '2026-04-17 10:00:01'),
                        (1002, 'issued', 250.00, '2026-04-17 10:00:03')
                    """
                )
                cursor.execute(
                    """
                    insert into payments (id, invoice_id, amount, updated_at)
                    values
                        (2001, 1002, 250.00, '2026-04-17 10:00:02')
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
                    insert into payments (id, invoice_id, amount, updated_at)
                    values (2002, 1001, 100.00, '2026-04-17 10:00:04')
                    """
                )
        finally:
            connection.close()

    def _alter_invoices_add_column(self, column_name: str) -> None:
        connection = self._connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"alter table invoices add column {column_name} varchar(255) null")
        finally:
            connection.close()

    def _alter_invoices_drop_column(self, column_name: str) -> None:
        connection = self._connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"alter table invoices drop column {column_name}")
        finally:
            connection.close()

    def test_live_mysql_snapshot_and_resume(self) -> None:
        self._seed_initial_rows()
        engine = AgentEngine(load_config(self.config_path))

        first_result = engine.run_once()
        self.assertEqual(first_result["processed"], 3)
        self.assertEqual(first_result["failed"], 0)
        self.assertEqual(len(MockWatcherHandler.registrations), 3)
        checkpoints = engine.checkpoint_summary()
        self.assertEqual(len(checkpoints), 1)
        checkpoint_state = json.loads(checkpoints[0]["token"])
        self.assertEqual(checkpoint_state["ledger.invoices"]["watermark"], "2026-04-17T10:00:03Z")
        self.assertEqual(checkpoint_state["ledger.payments"]["watermark"], "2026-04-17T10:00:02Z")

        self._insert_incremental_row()
        second_result = engine.run_once()
        self.assertEqual(second_result["processed"], 1)
        self.assertEqual(second_result["failed"], 0)
        self.assertEqual(len(MockWatcherHandler.registrations), 4)
        deliveries = engine.store.list_deliveries("mysql-core-ledger")
        self.assertEqual(len(deliveries), 4)

    def test_live_mysql_validate_bootstrap_and_inspect(self) -> None:
        engine = AgentEngine(load_config(self.config_path))

        validation = engine.validate()
        self.assertEqual(validation[0]["source_type"], "mysql")
        self.assertEqual(validation[0]["supports_snapshot"], "true")
        self.assertEqual(validation[0]["capture_modes"], ["watermark", "binlog"])

        bootstrap = engine.bootstrap("mysql-core-ledger")
        self.assertEqual(bootstrap["sources"][0]["capture_mode"], "watermark")
        self.assertEqual(len(bootstrap["sources"][0]["tracked_tables"]), 2)

        inspect = engine.inspect("mysql-core-ledger")
        self.assertEqual(inspect["sources"][0]["capture_mode"], "watermark")
        invoices = next(
            item
            for item in inspect["sources"][0]["tracked_tables"]
            if item["schema_name"] == "ledger" and item["table_name"] == "invoices"
        )
        self.assertIn("updated_at", invoices["selected_columns"])

    def test_live_mysql_auto_refreshes_runtime_signature_after_schema_drift(self) -> None:
        engine = AgentEngine(load_config(self.config_path))
        bootstrap = engine.bootstrap("mysql-core-ledger")
        self.assertEqual(bootstrap["sources"][0]["capture_mode"], "watermark")

        original_signature = engine.store.get_runtime_signature("mysql-core-ledger")
        self.assertIsNotNone(original_signature)

        drift_column = "denotary_runtime_drift_note"
        try:
            self._alter_invoices_add_column(drift_column)
            refresh_result = engine.run_once()
            self.assertEqual(refresh_result["processed"], 0)
            self.assertEqual(refresh_result["failed"], 0)

            refreshed_signature = engine.store.get_runtime_signature("mysql-core-ledger")
            self.assertIsNotNone(refreshed_signature)
            self.assertNotEqual(original_signature, refreshed_signature)

            inspect = engine.inspect("mysql-core-ledger")
            invoices = next(
                item
                for item in inspect["sources"][0]["tracked_tables"]
                if item["schema_name"] == "ledger" and item["table_name"] == "invoices"
            )
            self.assertIn(drift_column, invoices["selected_columns"])
        finally:
            self._alter_invoices_drop_column(drift_column)

    def test_live_mysql_backfill_none_skips_until_new_rows_exist(self) -> None:
        self._write_config(backfill_mode="none")
        engine = AgentEngine(load_config(self.config_path))

        empty_result = engine.run_once()
        self.assertEqual(empty_result["processed"], 0)
        self.assertEqual(empty_result["failed"], 0)

        self._seed_initial_rows()
        result = engine.run_once()
        self.assertEqual(result["processed"], 0)
        self.assertEqual(result["failed"], 0)

    def test_live_mysql_snapshot_normalizes_types(self) -> None:
        connection = self._connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    insert into invoices (id, status, amount, updated_at)
                    values (3001, 'issued', 123.45, '2026-04-18 11:22:33')
                    """
                )
        finally:
            connection.close()

        engine = AgentEngine(load_config(self.config_path))
        result = engine.run_once()
        self.assertEqual(result["processed"], 1)
        proofs = engine.store.list_proofs("mysql-core-ledger")
        self.assertEqual(len(proofs), 0)
        deliveries = engine.store.list_deliveries("mysql-core-ledger")
        self.assertEqual(len(deliveries), 1)

    def test_live_mysql_binlog_cdc_captures_insert_update_delete(self) -> None:
        start_file, start_pos = self._current_binlog_start()
        self._write_config(
            backfill_mode="none",
            capture_mode="binlog",
            row_limit=10,
            binlog_start_file=start_file,
            binlog_start_pos=start_pos,
        )
        engine = AgentEngine(load_config(self.config_path))

        bootstrap = engine.bootstrap("mysql-core-ledger")
        self.assertEqual(bootstrap["sources"][0]["capture_mode"], "binlog")
        self.assertTrue(bootstrap["sources"][0]["cdc"]["log_bin"])
        self.assertEqual(bootstrap["sources"][0]["cdc"]["binlog_format"], "ROW")

        connection = self._connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    insert into invoices (id, status, amount, updated_at)
                    values (4001, 'draft', 125.00, '2026-04-18 12:00:01')
                    """
                )
                cursor.execute(
                    """
                    update invoices
                    set status = 'issued', updated_at = '2026-04-18 12:00:02'
                    where id = 4001
                    """
                )
                cursor.execute("delete from invoices where id = 4001")
        finally:
            connection.close()

        first_result = engine.run_once()
        self.assertEqual(first_result["processed"], 3)
        self.assertEqual(first_result["failed"], 0)
        self.assertEqual(len(MockWatcherHandler.registrations), 3)

        checkpoint_token = engine.checkpoint_summary()[0]["token"]
        checkpoint_state = json.loads(checkpoint_token)
        self.assertEqual(checkpoint_state["mode"], "binlog")
        self.assertEqual(checkpoint_state["log_file"], start_file)
        self.assertGreaterEqual(int(checkpoint_state["log_pos"]), start_pos)

        deliveries = engine.store.list_deliveries("mysql-core-ledger")
        self.assertEqual(len(deliveries), 3)

        second_result = engine.run_once()
        self.assertEqual(second_result["processed"], 0)
        self.assertEqual(second_result["failed"], 0)
