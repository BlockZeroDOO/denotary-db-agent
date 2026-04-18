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

import psycopg

from denotary_db_agent.config import load_config
from denotary_db_agent.engine import AgentEngine


PROJECT_ROOT = Path(__file__).resolve().parents[1]
POSTGRES_COMPOSE_FILE = PROJECT_ROOT / "deploy" / "postgres-live" / "docker-compose.yml"


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
        body_out = json.dumps(response).encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body_out)))
        self.end_headers()
        self.wfile.write(body_out)

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
        response = {"ok": True, "request_id": payload["request_id"]}
        body_out = json.dumps(response).encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body_out)))
        self.end_headers()
        self.wfile.write(body_out)

    def log_message(self, format: str, *args: Any) -> None:
        return


class PostgresLiveIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._docker_compose("down", "-v")
        cls._docker_compose("up", "-d")
        cls._wait_for_postgres()

    @classmethod
    def tearDownClass(cls) -> None:
        cls._docker_compose("down", "-v")

    @classmethod
    def _docker_compose(cls, *args: str) -> None:
        subprocess.run(
            ["docker", "compose", "-f", str(POSTGRES_COMPOSE_FILE), *args],
            check=True,
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
        )

    @classmethod
    def _wait_for_postgres(cls) -> None:
        deadline = time.time() + 60
        while time.time() < deadline:
            try:
                with psycopg.connect(
                    host="127.0.0.1",
                    port=55432,
                    user="denotary",
                    password="denotarypw",
                    dbname="ledger",
                    connect_timeout=2,
                ) as connection:
                    with connection.cursor() as cursor:
                        cursor.execute("select 1")
                        cursor.fetchone()
                return
            except Exception:
                time.sleep(1)
        raise RuntimeError("postgres live container did not become ready in time")

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

    def _write_config(self, capture_mode: str = "watermark", backfill_mode: str = "full", row_limit: int = 100) -> None:
        config = {
            "agent_name": "live-postgres-test",
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
                    "id": "pg-core-ledger",
                    "adapter": "postgresql",
                    "enabled": True,
                    "source_instance": "erp-eu-1",
                    "database_name": "ledger",
                    "include": {"public": ["invoices", "payments"]},
                    "connection": {
                        "host": "127.0.0.1",
                        "port": 55432,
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
                        "cleanup_processed_events": True,
                        "slot_name": "denotary_test_slot",
                        "publication_name": "denotary_test_pub",
                        "output_plugin": "test_decoding",
                        "auto_create_slot": True,
                        "auto_create_publication": True,
                        "replica_identity_full": True,
                    },
                }
            ],
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

    def _truncate_tables(self) -> None:
        with psycopg.connect(
            host="127.0.0.1",
            port=55432,
            user="denotary",
            password="denotarypw",
            dbname="ledger",
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute("truncate table public.payments, public.invoices restart identity")
                cursor.execute("drop schema if exists denotary_cdc cascade")
                cursor.execute(
                    """
                    select slot_name
                    from pg_replication_slots
                    where slot_name = 'denotary_test_slot'
                    """
                )
                if cursor.fetchone():
                    cursor.execute("select pg_drop_replication_slot('denotary_test_slot')")
                cursor.execute("drop publication if exists denotary_test_pub")
            connection.commit()

    def _cdc_event_count(self) -> int:
        with psycopg.connect(
            host="127.0.0.1",
            port=55432,
            user="denotary",
            password="denotarypw",
            dbname="ledger",
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute("select count(*) from denotary_cdc.events")
                row = cursor.fetchone()
                return int(row[0] if row else 0)

    def _seed_initial_rows(self) -> None:
        with psycopg.connect(
            host="127.0.0.1",
            port=55432,
            user="denotary",
            password="denotarypw",
            dbname="ledger",
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    insert into public.invoices (id, status, amount, updated_at)
                    values
                        (1001, 'draft', 100.00, '2026-04-17T10:00:01Z'),
                        (1002, 'issued', 250.00, '2026-04-17T10:00:03Z')
                    """
                )
                cursor.execute(
                    """
                    insert into public.payments (id, invoice_id, amount, updated_at)
                    values
                        (2001, 1002, 250.00, '2026-04-17T10:00:02Z')
                    """
                )
            connection.commit()

    def _insert_incremental_row(self) -> None:
        with psycopg.connect(
            host="127.0.0.1",
            port=55432,
            user="denotary",
            password="denotarypw",
            dbname="ledger",
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    insert into public.payments (id, invoice_id, amount, updated_at)
                    values (2002, 1001, 100.00, '2026-04-17T10:00:04Z')
                    """
                )
            connection.commit()

    def _alter_invoices_add_column(self, column_name: str) -> None:
        with psycopg.connect(
            host="127.0.0.1",
            port=55432,
            user="denotary",
            password="denotarypw",
            dbname="ledger",
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"alter table public.invoices add column {column_name} text")
            connection.commit()

    def _alter_invoices_drop_column(self, column_name: str) -> None:
        with psycopg.connect(
            host="127.0.0.1",
            port=55432,
            user="denotary",
            password="denotarypw",
            dbname="ledger",
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"alter table public.invoices drop column if exists {column_name}")
            connection.commit()

    def _set_publication_tables(self, publication_name: str, tables: list[str]) -> None:
        with psycopg.connect(
            host="127.0.0.1",
            port=55432,
            user="denotary",
            password="denotarypw",
            dbname="ledger",
        ) as connection:
            with connection.cursor() as cursor:
                table_sql = ", ".join(tables)
                cursor.execute(f"alter publication {publication_name} set table {table_sql}")
            connection.commit()

    def _drop_publication(self, publication_name: str) -> None:
        with psycopg.connect(
            host="127.0.0.1",
            port=55432,
            user="denotary",
            password="denotarypw",
            dbname="ledger",
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"drop publication if exists {publication_name}")
            connection.commit()

    def _drop_replication_slot(self, slot_name: str) -> None:
        with psycopg.connect(
            host="127.0.0.1",
            port=55432,
            user="denotary",
            password="denotarypw",
            dbname="ledger",
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    select slot_name
                    from pg_replication_slots
                    where slot_name = %s
                    """,
                    (slot_name,),
                )
                if cursor.fetchone():
                    cursor.execute("select pg_drop_replication_slot(%s)", (slot_name,))
            connection.commit()

    def _set_replica_identity_default(self, table_name: str) -> None:
        with psycopg.connect(
            host="127.0.0.1",
            port=55432,
            user="denotary",
            password="denotarypw",
            dbname="ledger",
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"alter table public.{table_name} replica identity default")
            connection.commit()

    def test_live_postgres_snapshot_and_resume(self) -> None:
        self._seed_initial_rows()
        engine = AgentEngine(load_config(self.config_path))

        first_result = engine.run_once()
        self.assertEqual(first_result["processed"], 3)
        self.assertEqual(first_result["failed"], 0)
        self.assertEqual(len(MockWatcherHandler.registrations), 3)
        checkpoints = engine.checkpoint_summary()
        self.assertEqual(len(checkpoints), 1)
        checkpoint_state = json.loads(checkpoints[0]["token"])
        self.assertEqual(checkpoint_state["public.invoices"]["watermark"], "2026-04-17T10:00:03Z")
        self.assertEqual(checkpoint_state["public.payments"]["watermark"], "2026-04-17T10:00:02Z")

        self._insert_incremental_row()
        second_result = engine.run_once()
        self.assertEqual(second_result["processed"], 1)
        self.assertEqual(second_result["failed"], 0)
        self.assertEqual(len(MockWatcherHandler.registrations), 4)
        deliveries = engine.store.list_deliveries("pg-core-ledger")
        self.assertEqual(len(deliveries), 4)

    def test_live_postgres_trigger_cdc_captures_insert_update_delete(self) -> None:
        self._write_config(capture_mode="trigger", backfill_mode="none")
        engine = AgentEngine(load_config(self.config_path))
        validation = engine.validate()
        self.assertEqual(validation[0]["supports_cdc"], "true")

        empty_result = engine.run_once()
        self.assertEqual(empty_result["processed"], 0)
        self.assertEqual(empty_result["failed"], 0)

        with psycopg.connect(
            host="127.0.0.1",
            port=55432,
            user="denotary",
            password="denotarypw",
            dbname="ledger",
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    insert into public.invoices (id, status, amount, updated_at)
                    values (3001, 'draft', 77.00, '2026-04-17T11:00:01Z')
                    """
                )
            connection.commit()

        insert_result = engine.run_once()
        self.assertEqual(insert_result["processed"], 1)
        self.assertEqual(insert_result["failed"], 0)
        self.assertEqual(self._cdc_event_count(), 0)

        with psycopg.connect(
            host="127.0.0.1",
            port=55432,
            user="denotary",
            password="denotarypw",
            dbname="ledger",
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    update public.invoices
                    set status = 'issued', updated_at = '2026-04-17T11:00:02Z'
                    where id = 3001
                    """
                )
            connection.commit()

        update_result = engine.run_once()
        self.assertEqual(update_result["processed"], 1)
        self.assertEqual(update_result["failed"], 0)
        self.assertEqual(self._cdc_event_count(), 0)

        with psycopg.connect(
            host="127.0.0.1",
            port=55432,
            user="denotary",
            password="denotarypw",
            dbname="ledger",
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute("delete from public.invoices where id = 3001")
            connection.commit()

        delete_result = engine.run_once()
        self.assertEqual(delete_result["processed"], 1)
        self.assertEqual(delete_result["failed"], 0)
        self.assertEqual(self._cdc_event_count(), 0)

        deliveries = engine.store.list_deliveries("pg-core-ledger")
        self.assertEqual(len(deliveries), 3)

    def test_live_postgres_logical_cdc_captures_insert_update_delete(self) -> None:
        self._write_config(capture_mode="logical", backfill_mode="none")
        engine = AgentEngine(load_config(self.config_path))
        validation = engine.validate()
        self.assertEqual(validation[0]["supports_cdc"], "true")

        bootstrap = engine.bootstrap("pg-core-ledger")
        self.assertEqual(bootstrap["sources"][0]["capture_mode"], "logical")
        self.assertTrue(bootstrap["sources"][0]["cdc"]["slot_exists"])

        empty_result = engine.run_once()
        self.assertEqual(empty_result["processed"], 0)
        self.assertEqual(empty_result["failed"], 0)

        with psycopg.connect(
            host="127.0.0.1",
            port=55432,
            user="denotary",
            password="denotarypw",
            dbname="ledger",
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    insert into public.invoices (id, status, amount, updated_at)
                    values (4001, 'draft', 88.00, '2026-04-17T12:00:01Z')
                    """
                )
            connection.commit()

        insert_result = engine.run_once()
        self.assertEqual(insert_result["processed"], 1)
        self.assertEqual(insert_result["failed"], 0)

        with psycopg.connect(
            host="127.0.0.1",
            port=55432,
            user="denotary",
            password="denotarypw",
            dbname="ledger",
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    update public.invoices
                    set status = 'issued', updated_at = '2026-04-17T12:00:02Z'
                    where id = 4001
                    """
                )
            connection.commit()

        update_result = engine.run_once()
        self.assertEqual(update_result["processed"], 1)
        self.assertEqual(update_result["failed"], 0)

        with psycopg.connect(
            host="127.0.0.1",
            port=55432,
            user="denotary",
            password="denotarypw",
            dbname="ledger",
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute("delete from public.invoices where id = 4001")
            connection.commit()

        delete_result = engine.run_once()
        self.assertEqual(delete_result["processed"], 1)
        self.assertEqual(delete_result["failed"], 0)

        inspect = engine.inspect("pg-core-ledger")
        self.assertEqual(inspect["sources"][0]["capture_mode"], "logical")
        self.assertTrue(inspect["sources"][0]["cdc"]["slot_exists"])
        deliveries = engine.store.list_deliveries("pg-core-ledger")
        self.assertEqual(len(deliveries), 3)

    def test_live_postgres_logical_cdc_preserves_multirow_same_transaction_with_small_row_limit(self) -> None:
        self._write_config(capture_mode="logical", backfill_mode="none", row_limit=1)
        engine = AgentEngine(load_config(self.config_path))
        engine.validate()

        with psycopg.connect(
            host="127.0.0.1",
            port=55432,
            user="denotary",
            password="denotarypw",
            dbname="ledger",
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    insert into public.invoices (id, status, amount, updated_at)
                    values
                        (5001, 'draft', 15.00, '2026-04-17T13:00:01Z'),
                        (5002, 'draft', 25.00, '2026-04-17T13:00:01Z')
                    """
                )
            connection.commit()

        first_result = engine.run_once()
        self.assertEqual(first_result["processed"], 1)
        self.assertEqual(first_result["failed"], 0)

        second_result = engine.run_once()
        self.assertEqual(second_result["processed"], 1)
        self.assertEqual(second_result["failed"], 0)

        third_result = engine.run_once()
        self.assertEqual(third_result["processed"], 0)
        self.assertEqual(third_result["failed"], 0)

        deliveries = engine.store.list_deliveries("pg-core-ledger")
        self.assertEqual(len(deliveries), 2)

    def test_live_postgres_trigger_cdc_auto_refreshes_runtime_signature_after_schema_drift(self) -> None:
        self._write_config(capture_mode="trigger", backfill_mode="none")
        engine = AgentEngine(load_config(self.config_path))
        bootstrap = engine.bootstrap("pg-core-ledger")
        self.assertEqual(bootstrap["sources"][0]["capture_mode"], "trigger")

        original_signature = engine.store.get_runtime_signature("pg-core-ledger")
        self.assertIsNotNone(original_signature)

        drift_column = "denotary_runtime_drift_note"
        try:
            self._alter_invoices_add_column(drift_column)
            refresh_result = engine.run_once()
            self.assertEqual(refresh_result["processed"], 0)
            self.assertEqual(refresh_result["failed"], 0)

            refreshed_signature = engine.store.get_runtime_signature("pg-core-ledger")
            self.assertIsNotNone(refreshed_signature)
            self.assertNotEqual(original_signature, refreshed_signature)

            inspect = engine.inspect("pg-core-ledger")
            invoices = next(
                item
                for item in inspect["sources"][0]["tracked_tables"]
                if item["schema"] == "public" and item["table"] == "invoices"
            )
            self.assertIn(drift_column, invoices["selected_columns"])
        finally:
            self._alter_invoices_drop_column(drift_column)

    def test_live_postgres_pgoutput_bootstrap_and_inspect_publication_state(self) -> None:
        self._write_config(capture_mode="logical", backfill_mode="none")
        config = json.loads(self.config_path.read_text(encoding="utf-8"))
        config["sources"][0]["options"]["output_plugin"] = "pgoutput"
        self.config_path.write_text(json.dumps(config), encoding="utf-8")

        engine = AgentEngine(load_config(self.config_path))
        validation = engine.validate()
        self.assertEqual(validation[0]["supports_cdc"], "true")

        bootstrap = engine.bootstrap("pg-core-ledger")
        self.assertEqual(bootstrap["sources"][0]["capture_mode"], "logical")
        self.assertEqual(bootstrap["sources"][0]["cdc"]["plugin"], "pgoutput")
        self.assertTrue(bootstrap["sources"][0]["cdc"]["publication_exists"])
        self.assertIn("public.invoices", bootstrap["sources"][0]["cdc"]["publication_tables"])
        self.assertIn("public.payments", bootstrap["sources"][0]["cdc"]["publication_tables"])

        inspect = engine.inspect("pg-core-ledger")
        self.assertEqual(inspect["sources"][0]["cdc"]["plugin"], "pgoutput")
        self.assertEqual(inspect["sources"][0]["cdc"]["publication_name"], "denotary_test_pub")
        self.assertTrue(inspect["sources"][0]["cdc"]["slot_exists"])
        self.assertTrue(inspect["sources"][0]["cdc"]["publication_in_sync"])
        self.assertFalse(inspect["sources"][0]["cdc"]["pending_changes"])
        self.assertTrue(inspect["sources"][0]["cdc"]["replica_identity_in_sync"])
        self.assertIsInstance(inspect["sources"][0]["cdc"]["retained_wal_bytes"], int)
        self.assertIsInstance(inspect["sources"][0]["cdc"]["flush_lag_bytes"], int)

    def test_live_postgres_pgoutput_refresh_repairs_publication_drift(self) -> None:
        self._write_config(capture_mode="logical", backfill_mode="none")
        config = json.loads(self.config_path.read_text(encoding="utf-8"))
        config["sources"][0]["options"]["output_plugin"] = "pgoutput"
        self.config_path.write_text(json.dumps(config), encoding="utf-8")

        engine = AgentEngine(load_config(self.config_path))
        engine.validate()
        engine.bootstrap("pg-core-ledger")

        self._set_publication_tables(
            "denotary_test_pub",
            ['public.invoices'],
        )

        drifted = engine.inspect("pg-core-ledger")
        self.assertFalse(drifted["sources"][0]["cdc"]["publication_in_sync"])
        self.assertEqual(drifted["sources"][0]["cdc"]["publication_tables"], ["public.invoices"])

        refreshed = engine.refresh_source("pg-core-ledger")
        self.assertEqual(refreshed["sources"][0]["capture_mode"], "logical")

        repaired = engine.inspect("pg-core-ledger")
        self.assertTrue(repaired["sources"][0]["cdc"]["publication_in_sync"])
        self.assertIn("public.invoices", repaired["sources"][0]["cdc"]["publication_tables"])
        self.assertIn("public.payments", repaired["sources"][0]["cdc"]["publication_tables"])

    def test_live_postgres_pgoutput_refresh_recreates_missing_publication(self) -> None:
        self._write_config(capture_mode="logical", backfill_mode="none")
        config = json.loads(self.config_path.read_text(encoding="utf-8"))
        config["sources"][0]["options"]["output_plugin"] = "pgoutput"
        self.config_path.write_text(json.dumps(config), encoding="utf-8")

        engine = AgentEngine(load_config(self.config_path))
        engine.validate()
        engine.bootstrap("pg-core-ledger")

        self._drop_publication("denotary_test_pub")

        drifted = engine.inspect("pg-core-ledger")
        self.assertFalse(drifted["sources"][0]["cdc"]["publication_exists"])
        self.assertFalse(drifted["sources"][0]["cdc"]["publication_in_sync"])

        refreshed = engine.refresh_source("pg-core-ledger")
        self.assertEqual(refreshed["sources"][0]["capture_mode"], "logical")

        repaired = engine.inspect("pg-core-ledger")
        self.assertTrue(repaired["sources"][0]["cdc"]["publication_exists"])
        self.assertTrue(repaired["sources"][0]["cdc"]["publication_in_sync"])
        self.assertIn("public.invoices", repaired["sources"][0]["cdc"]["publication_tables"])
        self.assertIn("public.payments", repaired["sources"][0]["cdc"]["publication_tables"])

    def test_live_postgres_pgoutput_refresh_recreates_missing_slot(self) -> None:
        self._write_config(capture_mode="logical", backfill_mode="none")
        config = json.loads(self.config_path.read_text(encoding="utf-8"))
        config["sources"][0]["options"]["output_plugin"] = "pgoutput"
        self.config_path.write_text(json.dumps(config), encoding="utf-8")

        engine = AgentEngine(load_config(self.config_path))
        engine.validate()
        engine.bootstrap("pg-core-ledger")

        self._drop_replication_slot("denotary_test_slot")

        drifted = engine.inspect("pg-core-ledger")
        self.assertFalse(drifted["sources"][0]["cdc"]["slot_exists"])

        refreshed = engine.refresh_source("pg-core-ledger")
        self.assertEqual(refreshed["sources"][0]["capture_mode"], "logical")

        repaired = engine.inspect("pg-core-ledger")
        self.assertTrue(repaired["sources"][0]["cdc"]["slot_exists"])
        self.assertEqual(repaired["sources"][0]["cdc"]["plugin"], "pgoutput")

    def test_live_postgres_pgoutput_refresh_repairs_replica_identity_drift(self) -> None:
        self._write_config(capture_mode="logical", backfill_mode="none")
        config = json.loads(self.config_path.read_text(encoding="utf-8"))
        config["sources"][0]["options"]["output_plugin"] = "pgoutput"
        self.config_path.write_text(json.dumps(config), encoding="utf-8")

        engine = AgentEngine(load_config(self.config_path))
        engine.validate()
        engine.bootstrap("pg-core-ledger")

        self._set_replica_identity_default("invoices")

        drifted = engine.inspect("pg-core-ledger")
        self.assertFalse(drifted["sources"][0]["cdc"]["replica_identity_in_sync"])
        self.assertEqual(drifted["sources"][0]["cdc"]["replica_identity_expected"], "f")
        self.assertEqual(drifted["sources"][0]["cdc"]["replica_identity_by_table"]["public.invoices"], "d")

        refreshed = engine.refresh_source("pg-core-ledger")
        self.assertEqual(refreshed["sources"][0]["capture_mode"], "logical")

        repaired = engine.inspect("pg-core-ledger")
        self.assertTrue(repaired["sources"][0]["cdc"]["replica_identity_in_sync"])
        self.assertEqual(repaired["sources"][0]["cdc"]["replica_identity_by_table"]["public.invoices"], "f")

    def test_live_postgres_pgoutput_captures_insert_update_delete(self) -> None:
        self._write_config(capture_mode="logical", backfill_mode="none")
        config = json.loads(self.config_path.read_text(encoding="utf-8"))
        config["sources"][0]["options"]["output_plugin"] = "pgoutput"
        self.config_path.write_text(json.dumps(config), encoding="utf-8")

        engine = AgentEngine(load_config(self.config_path))
        validation = engine.validate()
        self.assertEqual(validation[0]["supports_cdc"], "true")

        bootstrap = engine.bootstrap("pg-core-ledger")
        self.assertEqual(bootstrap["sources"][0]["cdc"]["plugin"], "pgoutput")
        self.assertTrue(bootstrap["sources"][0]["cdc"]["publication_exists"])

        empty_result = engine.run_once()
        self.assertEqual(empty_result["processed"], 0)
        self.assertEqual(empty_result["failed"], 0)

        with psycopg.connect(
            host="127.0.0.1",
            port=55432,
            user="denotary",
            password="denotarypw",
            dbname="ledger",
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    insert into public.invoices (id, status, amount, updated_at)
                    values (6001, 'draft', 99.00, '2026-04-17T14:00:01Z')
                    """
                )
            connection.commit()

        insert_result = engine.run_once()
        self.assertEqual(insert_result["processed"], 1)
        self.assertEqual(insert_result["failed"], 0)

        with psycopg.connect(
            host="127.0.0.1",
            port=55432,
            user="denotary",
            password="denotarypw",
            dbname="ledger",
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    update public.invoices
                    set status = 'issued', updated_at = '2026-04-17T14:00:02Z'
                    where id = 6001
                    """
                )
            connection.commit()

        update_result = engine.run_once()
        self.assertEqual(update_result["processed"], 1)
        self.assertEqual(update_result["failed"], 0)

        with psycopg.connect(
            host="127.0.0.1",
            port=55432,
            user="denotary",
            password="denotarypw",
            dbname="ledger",
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute("delete from public.invoices where id = 6001")
            connection.commit()

        delete_result = engine.run_once()
        self.assertEqual(delete_result["processed"], 1)
        self.assertEqual(delete_result["failed"], 0)

        deliveries = engine.store.list_deliveries("pg-core-ledger")
        self.assertEqual(len(deliveries), 3)

    def test_live_postgres_pgoutput_stream_runtime_captures_insert(self) -> None:
        self._write_config(capture_mode="logical", backfill_mode="none")
        config = json.loads(self.config_path.read_text(encoding="utf-8"))
        config["sources"][0]["options"]["output_plugin"] = "pgoutput"
        config["sources"][0]["options"]["logical_runtime_mode"] = "stream"
        config["sources"][0]["options"]["logical_stream_timeout_sec"] = 2.0
        config["sources"][0]["options"]["logical_stream_idle_timeout_sec"] = 0.5
        self.config_path.write_text(json.dumps(config), encoding="utf-8")

        engine = AgentEngine(load_config(self.config_path))
        engine.validate()
        bootstrap = engine.bootstrap("pg-core-ledger")
        self.assertEqual(bootstrap["sources"][0]["cdc"]["plugin"], "pgoutput")

        with psycopg.connect(
            host="127.0.0.1",
            port=55432,
            user="denotary",
            password="denotarypw",
            dbname="ledger",
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    insert into public.invoices (id, status, amount, updated_at)
                    values (7001, 'draft', 44.00, '2026-04-17T15:00:01Z')
                    """
                )
            connection.commit()

        result = engine.run_once()
        self.assertEqual(result["processed"], 1)
        self.assertEqual(result["failed"], 0)
        deliveries = engine.store.list_deliveries("pg-core-ledger")
        self.assertEqual(len(deliveries), 1)

    def test_live_postgres_pgoutput_stream_preserves_multirow_same_transaction_with_small_row_limit(self) -> None:
        self._write_config(capture_mode="logical", backfill_mode="none", row_limit=1)
        config = json.loads(self.config_path.read_text(encoding="utf-8"))
        config["sources"][0]["options"]["output_plugin"] = "pgoutput"
        config["sources"][0]["options"]["logical_runtime_mode"] = "stream"
        config["sources"][0]["options"]["logical_stream_timeout_sec"] = 2.0
        config["sources"][0]["options"]["logical_stream_idle_timeout_sec"] = 0.5
        self.config_path.write_text(json.dumps(config), encoding="utf-8")

        engine = AgentEngine(load_config(self.config_path))
        engine.validate()
        engine.bootstrap("pg-core-ledger")

        with psycopg.connect(
            host="127.0.0.1",
            port=55432,
            user="denotary",
            password="denotarypw",
            dbname="ledger",
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    insert into public.invoices (id, status, amount, updated_at)
                    values
                        (7101, 'draft', 31.00, '2026-04-17T15:10:01Z'),
                        (7102, 'draft', 32.00, '2026-04-17T15:10:01Z')
                    """
                )
            connection.commit()

        first_result = engine.run_once()
        self.assertEqual(first_result["processed"], 1)
        self.assertEqual(first_result["failed"], 0)

        second_result = engine.run_once()
        self.assertEqual(second_result["processed"], 1)
        self.assertEqual(second_result["failed"], 0)

        third_result = engine.run_once()
        self.assertEqual(third_result["processed"], 0)
        self.assertEqual(third_result["failed"], 0)

        deliveries = engine.store.list_deliveries("pg-core-ledger")
        self.assertEqual(len(deliveries), 2)

    def test_live_postgres_pgoutput_stream_recovers_after_session_close(self) -> None:
        self._write_config(capture_mode="logical", backfill_mode="none")
        config = json.loads(self.config_path.read_text(encoding="utf-8"))
        config["sources"][0]["options"]["output_plugin"] = "pgoutput"
        config["sources"][0]["options"]["logical_runtime_mode"] = "stream"
        config["sources"][0]["options"]["logical_stream_timeout_sec"] = 2.0
        config["sources"][0]["options"]["logical_stream_idle_timeout_sec"] = 0.5
        self.config_path.write_text(json.dumps(config), encoding="utf-8")

        engine = AgentEngine(load_config(self.config_path))
        engine.validate()
        engine.bootstrap("pg-core-ledger")

        with psycopg.connect(
            host="127.0.0.1",
            port=55432,
            user="denotary",
            password="denotarypw",
            dbname="ledger",
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    insert into public.invoices (id, status, amount, updated_at)
                    values (7201, 'draft', 55.00, '2026-04-17T15:20:01Z')
                    """
                )
            connection.commit()

        first_result = engine.run_once()
        self.assertEqual(first_result["processed"], 1)
        self.assertEqual(first_result["failed"], 0)

        runtime = engine.runtimes()[0]
        runtime.adapter._close_replication_session(send_feedback=False)

        with psycopg.connect(
            host="127.0.0.1",
            port=55432,
            user="denotary",
            password="denotarypw",
            dbname="ledger",
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    insert into public.invoices (id, status, amount, updated_at)
                    values (7202, 'draft', 56.00, '2026-04-17T15:20:02Z')
                    """
                )
            connection.commit()

        second_result = engine.run_once()
        self.assertEqual(second_result["processed"], 1)
        self.assertEqual(second_result["failed"], 0)

        inspect = engine.inspect("pg-core-ledger")
        self.assertEqual(inspect["sources"][0]["cdc"]["runtime_mode"], "stream")
        self.assertEqual(inspect["sources"][0]["cdc"]["stream_connect_count"], 2)
        self.assertEqual(inspect["sources"][0]["cdc"]["stream_reconnect_count"], 1)
        self.assertTrue(inspect["sources"][0]["cdc"]["stream_last_connect_at"])
        self.assertTrue(inspect["sources"][0]["cdc"]["stream_last_reconnect_at"])
        self.assertEqual(inspect["sources"][0]["cdc"]["stream_last_reconnect_reason"], "session_reopen")
        self.assertEqual(inspect["sources"][0]["cdc"]["stream_failure_streak"], 0)
        self.assertFalse(inspect["sources"][0]["cdc"]["stream_backoff_active"])

        deliveries = engine.store.list_deliveries("pg-core-ledger")
        self.assertEqual(len(deliveries), 2)

    def test_live_postgres_pgoutput_stream_recovers_across_repeated_session_closes(self) -> None:
        self._write_config(capture_mode="logical", backfill_mode="none")
        config = json.loads(self.config_path.read_text(encoding="utf-8"))
        config["sources"][0]["options"]["output_plugin"] = "pgoutput"
        config["sources"][0]["options"]["logical_runtime_mode"] = "stream"
        config["sources"][0]["options"]["logical_stream_timeout_sec"] = 2.0
        config["sources"][0]["options"]["logical_stream_idle_timeout_sec"] = 0.5
        self.config_path.write_text(json.dumps(config), encoding="utf-8")

        engine = AgentEngine(load_config(self.config_path))
        engine.validate()
        engine.bootstrap("pg-core-ledger")

        def insert_invoice(invoice_id: int, amount: float, updated_at: str) -> None:
            with psycopg.connect(
                host="127.0.0.1",
                port=55432,
                user="denotary",
                password="denotarypw",
                dbname="ledger",
            ) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        insert into public.invoices (id, status, amount, updated_at)
                        values (%s, 'draft', %s, %s)
                        """,
                        (invoice_id, amount, updated_at),
                    )
                connection.commit()

        insert_invoice(7301, 61.00, "2026-04-17T15:30:01Z")
        first_result = engine.run_once()
        self.assertEqual(first_result["processed"], 1)
        self.assertEqual(first_result["failed"], 0)

        runtime = engine.runtimes()[0]
        runtime.adapter._close_replication_session(send_feedback=False)
        insert_invoice(7302, 62.00, "2026-04-17T15:30:02Z")
        second_result = engine.run_once()
        self.assertEqual(second_result["processed"], 1)
        self.assertEqual(second_result["failed"], 0)

        runtime.adapter._close_replication_session(send_feedback=False)
        insert_invoice(7303, 63.00, "2026-04-17T15:30:03Z")
        third_result = engine.run_once()
        self.assertEqual(third_result["processed"], 1)
        self.assertEqual(third_result["failed"], 0)

        inspect = engine.inspect("pg-core-ledger")
        self.assertEqual(inspect["sources"][0]["cdc"]["runtime_mode"], "stream")
        self.assertEqual(inspect["sources"][0]["cdc"]["stream_connect_count"], 3)
        self.assertEqual(inspect["sources"][0]["cdc"]["stream_reconnect_count"], 2)
        self.assertEqual(inspect["sources"][0]["cdc"]["stream_last_reconnect_reason"], "session_reopen")
        self.assertEqual(inspect["sources"][0]["cdc"]["stream_failure_streak"], 0)
        self.assertFalse(inspect["sources"][0]["cdc"]["stream_backoff_active"])

        health = engine.health()
        self.assertTrue(health["sources"][0]["ok"])
        self.assertEqual(health["sources"][0]["severity"], "healthy")

        deliveries = engine.store.list_deliveries("pg-core-ledger")
        self.assertEqual(len(deliveries), 3)
