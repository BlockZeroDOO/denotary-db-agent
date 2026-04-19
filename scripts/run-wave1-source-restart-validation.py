from __future__ import annotations

import argparse
import json
import socket
import subprocess
import tempfile
import threading
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import oracledb
import pymysql
import pytds
from pymongo import MongoClient

from denotary_db_agent.config import load_config
from denotary_db_agent.engine import AgentEngine


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TESTS_ROOT = PROJECT_ROOT / "tests"

import sys

sys.path.insert(0, str(TESTS_ROOT))

from mongodb_live_support import MONGODB_COMPOSE_FILE, MONGODB_URI, wait_for_mongodb_replica_set  # type: ignore
from oracle_live_support import ORACLE_COMPOSE_FILE, ORACLE_INIT_SQL, split_oracle_blocks  # type: ignore
from sqlserver_live_support import SQLSERVER_COMPOSE_FILE, SQLSERVER_INIT_SQL, split_tsql_batches  # type: ignore


MYSQL_COMPOSE_FILE = PROJECT_ROOT / "deploy" / "mysql-live" / "docker-compose.yml"
MARIADB_COMPOSE_FILE = PROJECT_ROOT / "deploy" / "mariadb-live" / "docker-compose.yml"
MYSQL_CONTAINER = "denotary-db-agent-mysql-live"
MARIADB_CONTAINER = "denotary-db-agent-mariadb-live"
SQLSERVER_CONTAINER = "denotary-db-agent-sqlserver-live"
ORACLE_CONTAINER = "denotary-db-agent-oracle-live"
MONGODB_CONTAINER = "denotary-db-agent-mongodb-live"


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as handle:
        handle.bind(("127.0.0.1", 0))
        return int(handle.getsockname()[1])


class MockState:
    counter = 0
    requests: dict[str, dict[str, Any]] = {}

    @classmethod
    def reset(cls) -> None:
        cls.counter = 0
        cls.requests = {}


class MockIngressHandler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802
        MockState.counter += 1
        body = self.rfile.read(int(self.headers.get("Content-Length", "0"))).decode("utf-8")
        payload = json.loads(body)
        request_id = f"request-{MockState.counter:04d}"
        trace_id = f"trace-{MockState.counter:04d}"
        response = {
            "request_id": request_id,
            "trace_id": trace_id,
            "external_ref_hash": f"{MockState.counter:064x}",
            "object_hash": f"{MockState.counter + 1000:064x}",
            "verification_account": "verif",
            "prepared_action": {
                "contract": "verifbill",
                "action": "submit",
                "data": {
                    "payer": payload["submitter"],
                    "submitter": payload["submitter"],
                    "schema_id": 1,
                    "policy_id": 1,
                    "object_hash": f"{MockState.counter + 1000:064x}",
                    "external_ref": f"{MockState.counter:064x}",
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


class MockChainHandler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802
        if self.path == "/v1/chain/get_info":
            payload = {
                "chain_id": "1" * 64,
                "last_irreversible_block_id": "0" * 64,
                "server_version": "mock-chain",
            }
        elif self.path == "/v1/chain/abi_json_to_bin":
            payload = {"binargs": "00"}
        elif self.path == "/v1/chain/push_transaction":
            payload = {
                "transaction_id": f"{MockState.counter + 5000:064x}",
                "processed": {"block_num": 900000 + MockState.counter},
            }
        else:
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        self._send_json(payload)

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
    def do_GET(self) -> None:  # noqa: N802
        prefix = "/v1/watch/"
        if not self.path.startswith(prefix):
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        request_id = self.path[len(prefix):]
        payload = MockState.requests.get(request_id)
        if payload is None:
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        self._send_json(payload)

    def do_POST(self) -> None:  # noqa: N802
        body = self.rfile.read(int(self.headers.get("Content-Length", "0"))).decode("utf-8")
        payload = json.loads(body) if body else {}
        if self.path == "/v1/watch/register":
            MockState.requests[payload["request_id"]] = {
                "request_id": payload["request_id"],
                "trace_id": payload["trace_id"],
                "mode": payload["mode"],
                "submitter": payload["submitter"],
                "contract": payload["contract"],
                "anchor": payload["anchor"],
                "status": "submitted",
                "inclusion_verified": False,
            }
            response = MockState.requests[payload["request_id"]]
        elif self.path.endswith("/included"):
            request_id = self.path.split("/")[-2]
            state = MockState.requests[request_id]
            state.update(
                {
                    "tx_id": payload["tx_id"],
                    "block_num": payload["block_num"],
                    "status": "included",
                    "inclusion_verified": True,
                }
            )
            response = state
        elif self.path.endswith("/poll"):
            request_id = self.path.split("/")[-2]
            state = MockState.requests[request_id]
            state.update(
                {
                    "status": "finalized",
                    "inclusion_verified": True,
                    "finalized_at": "2026-04-19T00:00:00Z",
                }
            )
            response = state
        elif self.path.endswith("/failed"):
            request_id = self.path.split("/")[-2]
            state = MockState.requests[request_id]
            state.update({"status": "failed", "failure_reason": payload["reason"]})
            response = state
        else:
            self.send_error(HTTPStatus.NOT_FOUND)
            return
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


class MockReceiptHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/healthz":
            payload = {"status": "ok", "service": "receipt-service"}
        elif self.path.startswith("/v1/receipts/"):
            request_id = self.path.split("/")[-1]
            state = MockState.requests[request_id]
            payload = {
                "request_id": request_id,
                "trace_id": state["trace_id"],
                "mode": "single",
                "submitter": state["submitter"],
                "contract": state["contract"],
                "object_hash": state["anchor"]["object_hash"],
                "external_ref_hash": state["anchor"]["external_ref_hash"],
                "tx_id": state["tx_id"],
                "block_num": state["block_num"],
                "finality_flag": True,
                "finalized_at": state["finalized_at"],
                "trust_state": "finalized_verified",
                "receipt_available": True,
                "inclusion_verified": True,
                "verification_policy": "single-provider",
                "verification_min_success": 1,
                "provider_disagreement": False,
            }
        else:
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        self._send_json(payload)

    def _send_json(self, payload: dict[str, Any], status: int = HTTPStatus.OK) -> None:
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, format: str, *args: Any) -> None:
        return


class MockAuditHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/healthz":
            payload = {"status": "ok", "service": "audit-api"}
        elif self.path.startswith("/v1/audit/chain/"):
            request_id = self.path.split("/")[-1]
            state = MockState.requests[request_id]
            payload = {
                "record": {
                    "request_id": request_id,
                    "trace_id": state["trace_id"],
                    "status": state["status"],
                    "tx_id": state["tx_id"],
                    "block_num": state["block_num"],
                },
                "receipt": {"request_id": request_id, "receipt_available": True},
                "proof_chain": [
                    {"stage": "request_registered", "status": "completed"},
                    {"stage": "transaction_included", "status": "completed"},
                    {"stage": "block_finalized", "status": "completed"},
                ],
            }
        else:
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        self._send_json(payload)

    def _send_json(self, payload: dict[str, Any], status: int = HTTPStatus.OK) -> None:
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, format: str, *args: Any) -> None:
        return


class MockServiceStack:
    def __init__(self) -> None:
        self.ingress_port = free_port()
        self.watcher_port = free_port()
        self.receipt_port = free_port()
        self.audit_port = free_port()
        self.chain_port = free_port()
        self.servers = [
            ThreadingHTTPServer(("127.0.0.1", self.ingress_port), MockIngressHandler),
            ThreadingHTTPServer(("127.0.0.1", self.watcher_port), MockWatcherHandler),
            ThreadingHTTPServer(("127.0.0.1", self.receipt_port), MockReceiptHandler),
            ThreadingHTTPServer(("127.0.0.1", self.audit_port), MockAuditHandler),
            ThreadingHTTPServer(("127.0.0.1", self.chain_port), MockChainHandler),
        ]
        self.threads = [threading.Thread(target=server.serve_forever, daemon=True) for server in self.servers]

    def start(self) -> None:
        MockState.reset()
        for thread in self.threads:
            thread.start()

    def stop(self) -> None:
        for server in self.servers:
            server.shutdown()
            server.server_close()
        for thread in self.threads:
            thread.join(timeout=2)


def docker_compose(compose_file: Path, *args: str) -> None:
    subprocess.run(
        ["docker", "compose", "-f", str(compose_file), *args],
        check=True,
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
    )


def docker_restart(container_name: str) -> None:
    subprocess.run(
        ["docker", "restart", container_name],
        check=True,
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
    )


def wait_mysql_like(port: int) -> None:
    deadline = time.time() + 90
    while time.time() < deadline:
        try:
            connection = pymysql.connect(
                host="127.0.0.1",
                port=port,
                user="denotary",
                password="denotarypw",
                database="ledger",
                connect_timeout=2,
                autocommit=True,
            )
            connection.close()
            return
        except Exception:
            time.sleep(1)
    raise RuntimeError(f"mysql-like container on port {port} did not become ready")


def wait_sqlserver() -> None:
    deadline = time.time() + 150
    while time.time() < deadline:
        try:
            connection = pytds.connect(
                dsn="127.0.0.1",
                port=51433,
                database="master",
                user="sa",
                password="StrongP@ssw0rd!",
                as_dict=True,
                autocommit=True,
                login_timeout=10,
                timeout=10,
            )
            with connection.cursor() as cursor:
                cursor.execute("select 1 as ok")
                cursor.fetchone()
            connection.close()
            return
        except Exception:
            time.sleep(1)
    raise RuntimeError("sqlserver live container did not become ready in time")


def wait_sqlserver_database(database: str) -> None:
    deadline = time.time() + 180
    while time.time() < deadline:
        try:
            connection = pytds.connect(
                dsn="127.0.0.1",
                port=51433,
                database=database,
                user="sa",
                password="StrongP@ssw0rd!",
                as_dict=True,
                autocommit=True,
                login_timeout=10,
                timeout=10,
            )
            with connection.cursor() as cursor:
                cursor.execute("select 1 as ok")
                cursor.fetchone()
            connection.close()
            return
        except Exception:
            time.sleep(2)
    raise RuntimeError(f"sqlserver database {database} did not become ready in time")


def wait_oracle_root() -> None:
    deadline = time.time() + 240
    while time.time() < deadline:
        try:
            connection = oracledb.connect(
                user="system",
                password="oraclepw",
                host="127.0.0.1",
                port=51521,
                service_name="FREE",
                tcp_connect_timeout=10,
            )
            connection.close()
            return
        except Exception:
            time.sleep(2)
    raise RuntimeError("oracle live container did not become ready in time")


def wait_oracle_pdb() -> None:
    deadline = time.time() + 240
    while time.time() < deadline:
        try:
            connection = oracledb.connect(
                user="denotary",
                password="denotarypw",
                host="127.0.0.1",
                port=51521,
                service_name="FREEPDB1",
                tcp_connect_timeout=10,
            )
            connection.close()
            return
        except Exception:
            time.sleep(2)
    raise RuntimeError("oracle FREEPDB1 did not become ready in time")


def enable_oracle_root_supplemental_logging() -> None:
    connection = oracledb.connect(
        user="system",
        password="oraclepw",
        host="127.0.0.1",
        port=51521,
        service_name="FREE",
        tcp_connect_timeout=10,
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute("alter database add supplemental log data")
        connection.commit()
    finally:
        connection.close()


def apply_sqlserver_init() -> None:
    connection = pytds.connect(
        dsn="127.0.0.1",
        port=51433,
        database="master",
        user="sa",
        password="StrongP@ssw0rd!",
        as_dict=True,
        autocommit=True,
        login_timeout=10,
        timeout=10,
    )
    try:
        with connection.cursor() as cursor:
            for batch in split_tsql_batches(SQLSERVER_INIT_SQL.read_text(encoding="utf-8")):
                cursor.execute(batch)
        connection.commit()
    finally:
        connection.close()


def apply_oracle_init() -> None:
    connection = oracledb.connect(
        user="denotary",
        password="denotarypw",
        host="127.0.0.1",
        port=51521,
        service_name="FREEPDB1",
        tcp_connect_timeout=10,
    )
    try:
        with connection.cursor() as cursor:
            for block in split_oracle_blocks(ORACLE_INIT_SQL.read_text(encoding="utf-8")):
                cursor.execute(block)
        connection.commit()
    finally:
        connection.close()


def build_denotary_config(mock_stack: MockServiceStack) -> dict[str, Any]:
    return {
        "ingress_url": f"http://127.0.0.1:{mock_stack.ingress_port}",
        "watcher_url": f"http://127.0.0.1:{mock_stack.watcher_port}",
        "watcher_auth_token": "token",
        "receipt_url": f"http://127.0.0.1:{mock_stack.receipt_port}",
        "audit_url": f"http://127.0.0.1:{mock_stack.audit_port}",
        "chain_rpc_url": f"http://127.0.0.1:{mock_stack.chain_port}",
        "submitter": "dbagentstest",
        "submitter_permission": "dnanchor",
        "submitter_private_key": "5HpHagT65TZzG1PH3CSu63k8DbpvD8s5ip4nEB3kEsreAnchuDf",
        "schema_id": 1,
        "policy_id": 1,
        "wait_for_finality": True,
        "finality_timeout_sec": 5,
        "finality_poll_interval_sec": 0.01,
    }


def write_config(
    temp_dir: Path,
    agent_name: str,
    source: dict[str, Any],
    mock_stack: MockServiceStack,
) -> Path:
    config = {
        "agent_name": agent_name,
        "log_level": "INFO",
        "denotary": build_denotary_config(mock_stack),
        "storage": {
            "state_db": str((temp_dir / "state.sqlite3").resolve()),
            "proof_dir": str((temp_dir / "proofs").resolve()),
        },
        "sources": [source],
    }
    path = temp_dir / "config.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    return path


def datetime_values(base: datetime, count: int) -> list[datetime]:
    return [base + timedelta(seconds=index) for index in range(count)]


def mysql_restart_drill() -> dict[str, Any]:
    docker_compose(MYSQL_COMPOSE_FILE, "down", "-v")
    docker_compose(MYSQL_COMPOSE_FILE, "up", "-d")
    wait_mysql_like(57306)
    with pymysql.connect(host="127.0.0.1", port=57306, user="denotary", password="denotarypw", database="ledger", autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute("delete from payments")
            cursor.execute("delete from invoices")
    admin = pymysql.connect(host="127.0.0.1", port=57306, user="root", password="rootpw", database="ledger", autocommit=True)
    try:
        with admin.cursor() as cursor:
            cursor.execute("show binary log status")
            row = cursor.fetchone()
            if not row:
                cursor.execute("show master status")
                row = cursor.fetchone()
    finally:
        admin.close()
    start_file = str(row[0] if not isinstance(row, dict) else row["File"])
    start_pos = int(row[1] if not isinstance(row, dict) else row["Position"])
    mock = MockServiceStack()
    mock.start()
    try:
        with tempfile.TemporaryDirectory() as temp:
            temp_dir = Path(temp)
            source = {
                "id": "mysql-restart-drill",
                "adapter": "mysql",
                "enabled": True,
                "source_instance": "recovery-drill",
                "database_name": "ledger",
                "include": {"ledger": ["invoices"]},
                "connection": {
                    "host": "127.0.0.1",
                    "port": 57306,
                    "username": "denotary",
                    "password": "denotarypw",
                    "database": "ledger",
                },
                "backfill_mode": "none",
                "batch_enabled": False,
                "options": {
                    "capture_mode": "binlog",
                    "watermark_column": "updated_at",
                    "commit_timestamp_column": "updated_at",
                    "row_limit": 20,
                    "binlog_server_id": 15101,
                    "binlog_start_file": start_file,
                    "binlog_start_pos": start_pos,
                },
            }
            config_path = write_config(temp_dir, "mysql-restart-drill", source, mock)
            engine = AgentEngine(load_config(config_path))
            engine.bootstrap("mysql-restart-drill")
            first_times = datetime_values(datetime(2026, 4, 19, 1, 0, 0), 3)
            with pymysql.connect(host="127.0.0.1", port=57306, user="denotary", password="denotarypw", database="ledger", autocommit=True) as connection:
                with connection.cursor() as cursor:
                    for index, stamp in enumerate(first_times, start=1):
                        cursor.execute(
                            "insert into invoices (id, status, amount, updated_at) values (%s, %s, %s, %s)",
                            (9100 + index, "issued", Decimal("100.00") + index, stamp.strftime("%Y-%m-%d %H:%M:%S")),
                        )
            first_result = engine.run_once()
            docker_restart(MYSQL_CONTAINER)
            wait_mysql_like(57306)
            second_times = datetime_values(datetime(2026, 4, 19, 1, 10, 0), 3)
            with pymysql.connect(host="127.0.0.1", port=57306, user="denotary", password="denotarypw", database="ledger", autocommit=True) as connection:
                with connection.cursor() as cursor:
                    for index, stamp in enumerate(second_times, start=1):
                        cursor.execute(
                            "insert into invoices (id, status, amount, updated_at) values (%s, %s, %s, %s)",
                            (9200 + index, "paid", Decimal("200.00") + index, stamp.strftime("%Y-%m-%d %H:%M:%S")),
                        )
            second_result = engine.run_once()
            deliveries = engine.store.list_deliveries("mysql-restart-drill")
            proofs = engine.store.list_proofs("mysql-restart-drill")
            return {
                "adapter": "mysql",
                "capture_mode": "binlog",
                "first_processed": first_result["processed"],
                "second_processed": second_result["processed"],
                "delivery_count": len(deliveries),
                "proof_count": len(proofs),
            }
    finally:
        mock.stop()
        docker_compose(MYSQL_COMPOSE_FILE, "down", "-v")


def mariadb_restart_drill() -> dict[str, Any]:
    docker_compose(MARIADB_COMPOSE_FILE, "down", "-v")
    docker_compose(MARIADB_COMPOSE_FILE, "up", "-d")
    wait_mysql_like(57307)
    with pymysql.connect(host="127.0.0.1", port=57307, user="denotary", password="denotarypw", database="ledger", autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute("delete from payments")
            cursor.execute("delete from invoices")
    admin = pymysql.connect(host="127.0.0.1", port=57307, user="root", password="rootpw", database="ledger", autocommit=True)
    try:
        with admin.cursor() as cursor:
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
        admin.close()
    start_file = str(row[0] if not isinstance(row, dict) else row["File"])
    start_pos = int(row[1] if not isinstance(row, dict) else row["Position"])
    mock = MockServiceStack()
    mock.start()
    try:
        with tempfile.TemporaryDirectory() as temp:
            temp_dir = Path(temp)
            source = {
                "id": "mariadb-restart-drill",
                "adapter": "mariadb",
                "enabled": True,
                "source_instance": "recovery-drill",
                "database_name": "ledger",
                "include": {"ledger": ["invoices"]},
                "connection": {
                    "host": "127.0.0.1",
                    "port": 57307,
                    "username": "denotary",
                    "password": "denotarypw",
                    "database": "ledger",
                },
                "backfill_mode": "none",
                "batch_enabled": False,
                "options": {
                    "capture_mode": "binlog",
                    "watermark_column": "updated_at",
                    "commit_timestamp_column": "updated_at",
                    "row_limit": 20,
                    "binlog_server_id": 15121,
                    "binlog_start_file": start_file,
                    "binlog_start_pos": start_pos,
                },
            }
            config_path = write_config(temp_dir, "mariadb-restart-drill", source, mock)
            engine = AgentEngine(load_config(config_path))
            engine.bootstrap("mariadb-restart-drill")
            first_times = datetime_values(datetime(2026, 4, 19, 2, 0, 0), 3)
            with pymysql.connect(host="127.0.0.1", port=57307, user="denotary", password="denotarypw", database="ledger", autocommit=True) as connection:
                with connection.cursor() as cursor:
                    for index, stamp in enumerate(first_times, start=1):
                        cursor.execute(
                            "insert into invoices (id, status, amount, updated_at) values (%s, %s, %s, %s)",
                            (10100 + index, "issued", Decimal("110.00") + index, stamp.strftime("%Y-%m-%d %H:%M:%S")),
                        )
            first_result = engine.run_once()
            docker_restart(MARIADB_CONTAINER)
            wait_mysql_like(57307)
            second_times = datetime_values(datetime(2026, 4, 19, 2, 10, 0), 3)
            with pymysql.connect(host="127.0.0.1", port=57307, user="denotary", password="denotarypw", database="ledger", autocommit=True) as connection:
                with connection.cursor() as cursor:
                    for index, stamp in enumerate(second_times, start=1):
                        cursor.execute(
                            "insert into invoices (id, status, amount, updated_at) values (%s, %s, %s, %s)",
                            (10200 + index, "paid", Decimal("210.00") + index, stamp.strftime("%Y-%m-%d %H:%M:%S")),
                        )
            second_result = engine.run_once()
            deliveries = engine.store.list_deliveries("mariadb-restart-drill")
            proofs = engine.store.list_proofs("mariadb-restart-drill")
            return {
                "adapter": "mariadb",
                "capture_mode": "binlog",
                "first_processed": first_result["processed"],
                "second_processed": second_result["processed"],
                "delivery_count": len(deliveries),
                "proof_count": len(proofs),
            }
    finally:
        mock.stop()
        docker_compose(MARIADB_COMPOSE_FILE, "down", "-v")


def sqlserver_restart_drill() -> dict[str, Any]:
    docker_compose(SQLSERVER_COMPOSE_FILE, "down", "-v")
    docker_compose(SQLSERVER_COMPOSE_FILE, "up", "-d")
    wait_sqlserver()
    apply_sqlserver_init()
    connection = pytds.connect(
        dsn="127.0.0.1",
        port=51433,
        database="ledger",
        user="sa",
        password="StrongP@ssw0rd!",
        as_dict=True,
        autocommit=True,
        login_timeout=10,
        timeout=10,
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute("delete from dbo.payments")
            cursor.execute("delete from dbo.invoices")
    finally:
        connection.close()
    mock = MockServiceStack()
    mock.start()
    try:
        with tempfile.TemporaryDirectory() as temp:
            temp_dir = Path(temp)
            source = {
                "id": "sqlserver-restart-drill",
                "adapter": "sqlserver",
                "enabled": True,
                "source_instance": "recovery-drill",
                "database_name": "ledger",
                "include": {"dbo": ["invoices"]},
                "connection": {
                    "host": "127.0.0.1",
                    "port": 51433,
                    "username": "sa",
                    "password": "StrongP@ssw0rd!",
                    "database": "ledger",
                },
                "backfill_mode": "none",
                "batch_enabled": False,
                "options": {
                    "capture_mode": "change_tracking",
                    "watermark_column": "updated_at",
                    "commit_timestamp_column": "updated_at",
                    "row_limit": 20,
                },
            }
            config_path = write_config(temp_dir, "sqlserver-restart-drill", source, mock)
            engine = AgentEngine(load_config(config_path))
            engine.bootstrap("sqlserver-restart-drill")
            baseline = engine.run_once()
            first_times = datetime_values(datetime(2026, 4, 19, 3, 0, 0), 3)
            connection = pytds.connect(
                dsn="127.0.0.1",
                port=51433,
                database="ledger",
                user="sa",
                password="StrongP@ssw0rd!",
                as_dict=True,
                autocommit=True,
                login_timeout=10,
                timeout=10,
            )
            try:
                with connection.cursor() as cursor:
                    for index, stamp in enumerate(first_times, start=1):
                        cursor.execute(
                            "insert into dbo.invoices (id, status, amount, updated_at) values (%s, %s, %s, %s)",
                            (11100 + index, "issued", Decimal("120.00") + index, stamp),
                        )
            finally:
                connection.close()
            first_result = engine.run_once()
            docker_restart(SQLSERVER_CONTAINER)
            wait_sqlserver()
            wait_sqlserver_database("ledger")
            second_times = datetime_values(datetime(2026, 4, 19, 3, 10, 0), 3)
            connection = pytds.connect(
                dsn="127.0.0.1",
                port=51433,
                database="ledger",
                user="sa",
                password="StrongP@ssw0rd!",
                as_dict=True,
                autocommit=True,
                login_timeout=10,
                timeout=10,
            )
            try:
                with connection.cursor() as cursor:
                    for index, stamp in enumerate(second_times, start=1):
                        cursor.execute(
                            "insert into dbo.invoices (id, status, amount, updated_at) values (%s, %s, %s, %s)",
                            (11200 + index, "paid", Decimal("220.00") + index, stamp),
                        )
            finally:
                connection.close()
            second_result = engine.run_once()
            deliveries = engine.store.list_deliveries("sqlserver-restart-drill")
            proofs = engine.store.list_proofs("sqlserver-restart-drill")
            return {
                "adapter": "sqlserver",
                "capture_mode": "change_tracking",
                "baseline_processed": baseline["processed"],
                "first_processed": first_result["processed"],
                "second_processed": second_result["processed"],
                "delivery_count": len(deliveries),
                "proof_count": len(proofs),
            }
    finally:
        mock.stop()
        docker_compose(SQLSERVER_COMPOSE_FILE, "down", "-v")


def oracle_restart_drill() -> dict[str, Any]:
    docker_compose(ORACLE_COMPOSE_FILE, "down", "-v")
    docker_compose(ORACLE_COMPOSE_FILE, "up", "-d")
    wait_oracle_root()
    wait_oracle_pdb()
    enable_oracle_root_supplemental_logging()
    apply_oracle_init()
    connection = oracledb.connect(
        user="denotary",
        password="denotarypw",
        host="127.0.0.1",
        port=51521,
        service_name="FREEPDB1",
        tcp_connect_timeout=10,
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute("delete from payments")
            cursor.execute("delete from invoices")
        connection.commit()
    finally:
        connection.close()
    mock = MockServiceStack()
    mock.start()
    try:
        with tempfile.TemporaryDirectory() as temp:
            temp_dir = Path(temp)
            source = {
                "id": "oracle-restart-drill",
                "adapter": "oracle",
                "enabled": True,
                "source_instance": "recovery-drill",
                "database_name": "ledger",
                "include": {"DENOTARY": ["INVOICES"]},
                "connection": {
                    "host": "127.0.0.1",
                    "port": 51521,
                    "username": "denotary",
                    "password": "denotarypw",
                    "service_name": "FREEPDB1",
                    "admin_username": "system",
                    "admin_password": "oraclepw",
                    "admin_service_name": "FREE",
                },
                "backfill_mode": "none",
                "batch_enabled": False,
                "options": {
                    "capture_mode": "logminer",
                    "watermark_column": "updated_at",
                    "commit_timestamp_column": "updated_at",
                    "row_limit": 20,
                },
            }
            config_path = write_config(temp_dir, "oracle-restart-drill", source, mock)
            engine = AgentEngine(load_config(config_path))
            engine.bootstrap("oracle-restart-drill")
            baseline = engine.run_once()
            first_times = datetime_values(datetime(2026, 4, 19, 4, 0, 0), 3)
            connection = oracledb.connect(
                user="denotary",
                password="denotarypw",
                host="127.0.0.1",
                port=51521,
                service_name="FREEPDB1",
                tcp_connect_timeout=10,
            )
            try:
                with connection.cursor() as cursor:
                    for index, stamp in enumerate(first_times, start=1):
                        cursor.execute(
                            "insert into invoices (id, status, amount, updated_at) values (:1, :2, :3, :4)",
                            [12100 + index, "issued", 130 + index, stamp],
                        )
                connection.commit()
            finally:
                connection.close()
            first_result = engine.run_once()
            docker_restart(ORACLE_CONTAINER)
            wait_oracle_root()
            wait_oracle_pdb()
            enable_oracle_root_supplemental_logging()
            second_times = datetime_values(datetime(2026, 4, 19, 4, 10, 0), 3)
            connection = oracledb.connect(
                user="denotary",
                password="denotarypw",
                host="127.0.0.1",
                port=51521,
                service_name="FREEPDB1",
                tcp_connect_timeout=10,
            )
            try:
                with connection.cursor() as cursor:
                    for index, stamp in enumerate(second_times, start=1):
                        cursor.execute(
                            "insert into invoices (id, status, amount, updated_at) values (:1, :2, :3, :4)",
                            [12200 + index, "paid", 230 + index, stamp],
                        )
                connection.commit()
            finally:
                connection.close()
            second_result = engine.run_once()
            deliveries = engine.store.list_deliveries("oracle-restart-drill")
            proofs = engine.store.list_proofs("oracle-restart-drill")
            return {
                "adapter": "oracle",
                "capture_mode": "logminer",
                "baseline_processed": baseline["processed"],
                "first_processed": first_result["processed"],
                "second_processed": second_result["processed"],
                "delivery_count": len(deliveries),
                "proof_count": len(proofs),
            }
    finally:
        mock.stop()
        docker_compose(ORACLE_COMPOSE_FILE, "down", "-v")


def mongodb_restart_drill() -> dict[str, Any]:
    docker_compose(MONGODB_COMPOSE_FILE, "down", "-v")
    docker_compose(MONGODB_COMPOSE_FILE, "up", "-d")
    wait_for_mongodb_replica_set()
    client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
    try:
        db = client["ledger"]
        existing = set(db.list_collection_names())
        if "invoices" not in existing:
            db.create_collection("invoices")
        if "payments" not in existing:
            db.create_collection("payments")
        db["payments"].delete_many({})
        db["invoices"].delete_many({})
    finally:
        client.close()
    mock = MockServiceStack()
    mock.start()
    try:
        with tempfile.TemporaryDirectory() as temp:
            temp_dir = Path(temp)
            source = {
                "id": "mongodb-restart-drill",
                "adapter": "mongodb",
                "enabled": True,
                "source_instance": "recovery-drill",
                "database_name": "ledger",
                "include": {"ledger": ["invoices"]},
                "connection": {"uri": MONGODB_URI},
                "backfill_mode": "none",
                "batch_enabled": False,
                "options": {
                    "capture_mode": "change_streams",
                    "watermark_column": "updated_at",
                    "commit_timestamp_column": "updated_at",
                    "row_limit": 20,
                },
            }
            config_path = write_config(temp_dir, "mongodb-restart-drill", source, mock)
            engine = AgentEngine(load_config(config_path))
            engine.bootstrap("mongodb-restart-drill")
            baseline = engine.run_once()
            client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
            try:
                db = client["ledger"]
                for index, stamp in enumerate(datetime_values(datetime(2026, 4, 19, 5, 0, 0, tzinfo=timezone.utc), 3), start=1):
                    db["invoices"].insert_one({"status": "issued", "amount": 140.0 + index, "updated_at": stamp})
            finally:
                client.close()
            time.sleep(1)
            first_result = engine.run_once()
            docker_restart(MONGODB_CONTAINER)
            wait_for_mongodb_replica_set()
            client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
            try:
                db = client["ledger"]
                for index, stamp in enumerate(datetime_values(datetime(2026, 4, 19, 5, 10, 0, tzinfo=timezone.utc), 3), start=1):
                    db["invoices"].insert_one({"status": "paid", "amount": 240.0 + index, "updated_at": stamp})
            finally:
                client.close()
            time.sleep(1)
            second_result = engine.run_once()
            deliveries = engine.store.list_deliveries("mongodb-restart-drill")
            proofs = engine.store.list_proofs("mongodb-restart-drill")
            return {
                "adapter": "mongodb",
                "capture_mode": "change_streams",
                "baseline_processed": baseline["processed"],
                "first_processed": first_result["processed"],
                "second_processed": second_result["processed"],
                "delivery_count": len(deliveries),
                "proof_count": len(proofs),
            }
    finally:
        mock.stop()
        docker_compose(MONGODB_COMPOSE_FILE, "down", "-v")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--adapter", choices=("mysql", "mariadb", "sqlserver", "oracle", "mongodb"))
    args = parser.parse_args()

    runners = {
        "mysql": mysql_restart_drill,
        "mariadb": mariadb_restart_drill,
        "sqlserver": sqlserver_restart_drill,
        "oracle": oracle_restart_drill,
        "mongodb": mongodb_restart_drill,
    }
    timeouts = {
        "mysql": 600,
        "mariadb": 600,
        "sqlserver": 900,
        "oracle": 900,
        "mongodb": 900,
    }

    if args.adapter:
        result = runners[args.adapter]()
        print(f"RESULT_JSON:{json.dumps(result)}")
        return

    results: list[dict[str, Any]] = []
    for adapter in ("mysql", "mariadb", "sqlserver", "oracle", "mongodb"):
        cmd = [sys.executable, __file__, "--adapter", adapter]
        try:
            completed = subprocess.run(
                cmd,
                cwd=str(PROJECT_ROOT),
                capture_output=True,
                text=True,
                timeout=timeouts[adapter],
                check=True,
            )
            result_line = next(
                (line for line in reversed(completed.stdout.splitlines()) if line.startswith("RESULT_JSON:")),
                None,
            )
            if result_line is None:
                results.append(
                    {
                        "adapter": adapter,
                        "status": "failed",
                        "error": "adapter drill did not emit RESULT_JSON",
                    }
                )
                continue
            payload = json.loads(result_line[len("RESULT_JSON:"):])
            payload["status"] = "passed"
            results.append(payload)
        except subprocess.TimeoutExpired:
            results.append({"adapter": adapter, "status": "timeout"})
        except subprocess.CalledProcessError as exc:
            output = (exc.stdout or "").splitlines()[-10:]
            errors = (exc.stderr or "").splitlines()[-10:]
            results.append(
                {
                    "adapter": adapter,
                    "status": "failed",
                    "stdout_tail": output,
                    "stderr_tail": errors,
                }
            )

    print(json.dumps({"results": results}, indent=2))


if __name__ == "__main__":
    main()
