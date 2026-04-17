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

        self._truncate_tables()
        self.config_path = Path(self.temp_dir.name) / "config.json"
        config = {
            "agent_name": "live-postgres-test",
            "log_level": "INFO",
            "denotary": {
                "ingress_url": f"http://127.0.0.1:{ingress_port}",
                "watcher_url": f"http://127.0.0.1:{watcher_port}",
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
                    "backfill_mode": "full",
                    "batch_size": 100,
                    "options": {
                        "watermark_column": "updated_at",
                        "commit_timestamp_column": "updated_at",
                        "row_limit": 100,
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
            connection.commit()

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
