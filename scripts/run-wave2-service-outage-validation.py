from __future__ import annotations

import argparse
import json
import os
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass, field
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from denotary_db_agent.config import load_config
from denotary_db_agent.engine import AgentEngine


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TESTS_ROOT = PROJECT_ROOT / "tests"
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "data" / "wave2-service-outage-validation-latest"
sys.path.insert(0, str(TESTS_ROOT))

from cassandra_live_support import create_session as create_cassandra_session  # type: ignore
from db2_live_support import create_connection as create_db2_connection, wait_for_db2_ready  # type: ignore
from elasticsearch_live_support import create_client as create_elasticsearch_client  # type: ignore
from redis_live_support import REDIS_PORT, REDIS_URL, redis, run_redis_compose, wait_for_redis  # type: ignore
from scylladb_live_support import create_session as create_scylladb_session  # type: ignore


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as handle:
        handle.bind(("127.0.0.1", 0))
        return int(handle.getsockname()[1])


class _temporary_env:
    def __init__(self, values: dict[str, str]) -> None:
        self.values = values
        self.original = {key: os.environ.get(key) for key in values}

    def __enter__(self) -> None:
        os.environ.update(self.values)
        return None

    def __exit__(self, exc_type, exc, tb) -> None:
        for key, previous in self.original.items():
            if previous is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = previous


def docker_compose(compose_file: Path, *args: str) -> None:
    subprocess.run(
        ["docker", "compose", "-f", str(compose_file), *args],
        cwd=str(PROJECT_ROOT),
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


DB2_ENV = {
    "DENOTARY_DB2_HOST": "127.0.0.1",
    "DENOTARY_DB2_PORT": "55000",
    "DENOTARY_DB2_USERNAME": "db2inst1",
    "DENOTARY_DB2_PASSWORD": "password",
    "DENOTARY_DB2_DATABASE": "DENOTARY",
    "DENOTARY_DB2_SCHEMA": "DB2INST1",
}

CASSANDRA_ENV = {
    "DENOTARY_CASSANDRA_HOST": "127.0.0.1",
    "DENOTARY_CASSANDRA_PORT": "59042",
    "DENOTARY_CASSANDRA_KEYSPACE": "denotary_agent",
}

SCYLLADB_ENV = {
    "DENOTARY_SCYLLADB_HOST": "127.0.0.1",
    "DENOTARY_SCYLLADB_PORT": "59043",
    "DENOTARY_SCYLLADB_KEYSPACE": "denotary_agent",
    "DENOTARY_SCYLLADB_USERNAME": "",
    "DENOTARY_SCYLLADB_PASSWORD": "",
}

ELASTICSEARCH_ENV = {
    "DENOTARY_ELASTICSEARCH_URL": "http://127.0.0.1:59200",
    "DENOTARY_ELASTICSEARCH_USERNAME": "",
    "DENOTARY_ELASTICSEARCH_PASSWORD": "",
    "DENOTARY_ELASTICSEARCH_VERIFY_CERTS": "false",
}


@dataclass
class FaultState:
    requests: dict[str, dict[str, Any]] = field(default_factory=dict)
    counter: int = 0
    fail_counts: dict[str, int] = field(default_factory=dict)

    def should_fail(self, key: str) -> bool:
        remaining = int(self.fail_counts.get(key) or 0)
        if remaining > 0:
            self.fail_counts[key] = remaining - 1
            return True
        return False


STATE = FaultState()


class BaseHandler(BaseHTTPRequestHandler):
    def _send_json(self, payload: dict[str, Any], status: int = HTTPStatus.OK) -> None:
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, format: str, *args: Any) -> None:
        return


class IngressHandler(BaseHandler):
    def do_POST(self) -> None:  # noqa: N802
        if STATE.should_fail("ingress.prepare"):
            self.send_error(HTTPStatus.SERVICE_UNAVAILABLE)
            return
        body = self.rfile.read(int(self.headers.get("Content-Length", "0"))).decode("utf-8")
        payload = json.loads(body)
        STATE.counter += 1
        request_id = f"wave2-outage-request-{STATE.counter:04d}"
        trace_id = f"wave2-outage-trace-{STATE.counter:04d}"
        response = {
            "request_id": request_id,
            "trace_id": trace_id,
            "external_ref_hash": f"{STATE.counter:064x}",
            "object_hash": f"{STATE.counter + 1000:064x}",
            "verification_account": "verif",
            "prepared_action": {
                "contract": "verifbill",
                "action": "submit",
                "data": {
                    "payer": payload["submitter"],
                    "submitter": payload["submitter"],
                    "schema_id": 1,
                    "policy_id": 1,
                    "object_hash": f"{STATE.counter + 1000:064x}",
                    "external_ref": f"{STATE.counter:064x}",
                },
            },
        }
        self._send_json(response)


class ChainHandler(BaseHandler):
    def do_POST(self) -> None:  # noqa: N802
        if self.path == "/v1/chain/get_info":
            self._send_json(
                {
                    "chain_id": "1" * 64,
                    "last_irreversible_block_id": "0" * 64,
                    "server_version": "mock-chain",
                }
            )
            return
        if self.path == "/v1/chain/abi_json_to_bin":
            self._send_json({"binargs": "00"})
            return
        if self.path == "/v1/chain/push_transaction":
            self._send_json(
                {
                    "transaction_id": f"{STATE.counter + 5000:064x}",
                    "processed": {"block_num": 975000 + STATE.counter},
                }
            )
            return
        self.send_error(HTTPStatus.NOT_FOUND)


class WatcherHandler(BaseHandler):
    def do_GET(self) -> None:  # noqa: N802
        prefix = "/v1/watch/"
        if not self.path.startswith(prefix):
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        request_id = self.path[len(prefix):]
        payload = STATE.requests.get(request_id)
        if payload is None:
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        self._send_json(payload)

    def do_POST(self) -> None:  # noqa: N802
        body = self.rfile.read(int(self.headers.get("Content-Length", "0"))).decode("utf-8")
        payload = json.loads(body) if body else {}
        if self.path == "/v1/watch/register":
            if STATE.should_fail("watcher.register"):
                self.send_error(HTTPStatus.SERVICE_UNAVAILABLE)
                return
            STATE.requests[payload["request_id"]] = {
                "request_id": payload["request_id"],
                "trace_id": payload["trace_id"],
                "mode": payload["mode"],
                "submitter": payload["submitter"],
                "contract": payload["contract"],
                "anchor": payload["anchor"],
                "status": "submitted",
                "inclusion_verified": False,
            }
            self._send_json(STATE.requests[payload["request_id"]])
            return
        if self.path.endswith("/included"):
            request_id = self.path.split("/")[-2]
            state = STATE.requests[request_id]
            state.update(
                {
                    "tx_id": payload["tx_id"],
                    "block_num": payload["block_num"],
                    "status": "included",
                    "inclusion_verified": True,
                }
            )
            self._send_json(state)
            return
        if self.path.endswith("/poll"):
            request_id = self.path.split("/")[-2]
            state = STATE.requests[request_id]
            state.update(
                {
                    "status": "finalized",
                    "inclusion_verified": True,
                    "finalized_at": "2026-04-20T18:00:00Z",
                }
            )
            self._send_json(state)
            return
        if self.path.endswith("/failed"):
            request_id = self.path.split("/")[-2]
            state = STATE.requests.setdefault(
                request_id,
                {
                    "request_id": request_id,
                    "trace_id": "",
                    "mode": "single",
                    "submitter": "",
                    "contract": "",
                    "anchor": {},
                },
            )
            state.update({"status": "failed", "failure_reason": payload["reason"]})
            self._send_json(state)
            return
        self.send_error(HTTPStatus.NOT_FOUND)


class ReceiptHandler(BaseHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/healthz":
            self._send_json({"status": "ok", "service": "receipt-service"})
            return
        if not self.path.startswith("/v1/receipts/"):
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        if STATE.should_fail("receipt.get"):
            self.send_error(HTTPStatus.SERVICE_UNAVAILABLE)
            return
        request_id = self.path.split("/")[-1]
        state = STATE.requests[request_id]
        self._send_json(
            {
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
        )


class AuditHandler(BaseHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/healthz":
            self._send_json({"status": "ok", "service": "audit-api"})
            return
        if not self.path.startswith("/v1/audit/chain/"):
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        if STATE.should_fail("audit.get_chain"):
            self.send_error(HTTPStatus.SERVICE_UNAVAILABLE)
            return
        request_id = self.path.split("/")[-1]
        state = STATE.requests[request_id]
        self._send_json(
            {
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
        )


class MockServiceStack:
    def __init__(self) -> None:
        self.ingress_port = free_port()
        self.watcher_port = free_port()
        self.receipt_port = free_port()
        self.audit_port = free_port()
        self.chain_port = free_port()
        self.servers = [
            ThreadingHTTPServer(("127.0.0.1", self.ingress_port), IngressHandler),
            ThreadingHTTPServer(("127.0.0.1", self.watcher_port), WatcherHandler),
            ThreadingHTTPServer(("127.0.0.1", self.receipt_port), ReceiptHandler),
            ThreadingHTTPServer(("127.0.0.1", self.audit_port), AuditHandler),
            ThreadingHTTPServer(("127.0.0.1", self.chain_port), ChainHandler),
        ]
        self.threads = [threading.Thread(target=server.serve_forever, daemon=True) for server in self.servers]

    def start(self) -> None:
        STATE.requests = {}
        STATE.counter = 0
        STATE.fail_counts = {}
        for thread in self.threads:
            thread.start()

    def stop(self) -> None:
        for server in self.servers:
            server.shutdown()
            server.server_close()
        for thread in self.threads:
            thread.join(timeout=2)


def initialize_sqlite(database_path: Path) -> None:
    connection = sqlite3.connect(str(database_path))
    try:
        connection.execute(
            """
            create table if not exists invoices (
                id integer primary key,
                status text not null,
                updated_at text not null
            )
            """
        )
        connection.execute("delete from invoices")
        connection.commit()
    finally:
        connection.close()


def insert_sqlite_invoice(database_path: Path, *, record_id: int) -> None:
    connection = sqlite3.connect(str(database_path))
    try:
        connection.execute(
            "insert into invoices (id, status, updated_at) values (?, ?, ?)",
            (record_id, "issued", f"2026-04-20T18:10:{record_id:02d}Z"),
        )
        connection.commit()
    finally:
        connection.close()


def redis_client():
    if redis is None:
        raise RuntimeError("redis package is required for Wave 2 Redis service-outage validation")
    return redis.Redis.from_url(REDIS_URL, decode_responses=False, socket_timeout=5)


def flush_redis() -> None:
    wait_for_redis()
    last_error: Exception | None = None
    for _ in range(3):
        client = redis_client()
        try:
            client.flushdb()
            return
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            wait_for_redis()
        finally:
            close = getattr(client, "close", None)
            if callable(close):
                close()
    if last_error is not None:
        raise last_error


def write_redis_key(key: str, value: str) -> None:
    client = redis_client()
    try:
        client.set(key, value)
    finally:
        close = getattr(client, "close", None)
        if callable(close):
            close()


def write_sqlite_config(temp_dir: Path, stack: MockServiceStack, database_path: Path) -> Path:
    config = {
        "agent_name": "wave2-sqlite-service-outage",
        "log_level": "INFO",
        "denotary": {
            "ingress_url": f"http://127.0.0.1:{stack.ingress_port}",
            "watcher_url": f"http://127.0.0.1:{stack.watcher_port}",
            "watcher_auth_token": "token",
            "receipt_url": f"http://127.0.0.1:{stack.receipt_port}",
            "audit_url": f"http://127.0.0.1:{stack.audit_port}",
            "chain_rpc_url": f"http://127.0.0.1:{stack.chain_port}",
            "submitter": "dbagentstest",
            "submitter_permission": "dnanchor",
            "submitter_private_key": "5HpHagT65TZzG1PH3CSu63k8DbpvD8s5ip4nEB3kEsreAnchuDf",
            "schema_id": 1,
            "policy_id": 1,
            "wait_for_finality": True,
            "finality_timeout_sec": 5,
            "finality_poll_interval_sec": 0.01,
        },
        "storage": {
            "state_db": str((temp_dir / "state.sqlite3").resolve()),
            "proof_dir": str((temp_dir / "proofs").resolve()),
        },
        "sources": [
            {
                "id": "sqlite-wave2-service-outage",
                "adapter": "sqlite",
                "enabled": True,
                "source_instance": "edge-device-1",
                "database_name": "ledger",
                "include": {"main": ["invoices"]},
                "checkpoint_policy": "after_ack",
                "backfill_mode": "full",
                "connection": {"path": str(database_path.resolve())},
                "options": {
                    "capture_mode": "watermark",
                    "watermark_column": "updated_at",
                    "commit_timestamp_column": "updated_at",
                    "primary_key_columns": ["id"],
                    "row_limit": 100,
                },
            }
        ],
    }
    path = temp_dir / "sqlite-config.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    return path


def write_redis_config(temp_dir: Path, stack: MockServiceStack) -> Path:
    config = {
        "agent_name": "wave2-redis-service-outage",
        "log_level": "INFO",
        "denotary": {
            "ingress_url": f"http://127.0.0.1:{stack.ingress_port}",
            "watcher_url": f"http://127.0.0.1:{stack.watcher_port}",
            "watcher_auth_token": "token",
            "receipt_url": f"http://127.0.0.1:{stack.receipt_port}",
            "audit_url": f"http://127.0.0.1:{stack.audit_port}",
            "chain_rpc_url": f"http://127.0.0.1:{stack.chain_port}",
            "submitter": "dbagentstest",
            "submitter_permission": "dnanchor",
            "submitter_private_key": "5HpHagT65TZzG1PH3CSu63k8DbpvD8s5ip4nEB3kEsreAnchuDf",
            "schema_id": 1,
            "policy_id": 1,
            "wait_for_finality": True,
            "finality_timeout_sec": 5,
            "finality_poll_interval_sec": 0.01,
        },
        "storage": {
            "state_db": str((temp_dir / "state.sqlite3").resolve()),
            "proof_dir": str((temp_dir / "proofs").resolve()),
        },
        "sources": [
            {
                "id": "redis-wave2-service-outage",
                "adapter": "redis",
                "enabled": True,
                "source_instance": "wave2-validation",
                "database_name": "db0",
                "include": {"0": ["orders:*"]},
                "checkpoint_policy": "after_ack",
                "backfill_mode": "full",
                "connection": {
                    "host": "127.0.0.1",
                    "port": REDIS_PORT,
                },
                "options": {
                    "capture_mode": "scan",
                    "row_limit": 100,
                    "scan_count": 100,
                },
            }
        ],
    }
    path = temp_dir / "redis-config.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    return path


def write_dry_run_config(
    temp_dir: Path,
    stack: MockServiceStack,
    *,
    source_id: str,
    adapter: str,
    source_instance: str,
    database_name: str,
    include: dict[str, list[str]],
    connection: dict[str, Any],
    options: dict[str, Any],
) -> Path:
    config = {
        "agent_name": f"{source_id}-service-outage",
        "log_level": "INFO",
        "denotary": {
            "ingress_url": f"http://127.0.0.1:{stack.ingress_port}",
            "watcher_url": f"http://127.0.0.1:{stack.watcher_port}",
            "watcher_auth_token": "token",
            "receipt_url": f"http://127.0.0.1:{stack.receipt_port}",
            "audit_url": f"http://127.0.0.1:{stack.audit_port}",
            "chain_rpc_url": f"http://127.0.0.1:{stack.chain_port}",
            "submitter": "dbagentstest",
            "submitter_permission": "dnanchor",
            "submitter_private_key": "5HpHagT65TZzG1PH3CSu63k8DbpvD8s5ip4nEB3kEsreAnchuDf",
            "schema_id": 1,
            "policy_id": 1,
            "wait_for_finality": True,
            "finality_timeout_sec": 5,
            "finality_poll_interval_sec": 0.01,
        },
        "storage": {
            "state_db": str((temp_dir / "state.sqlite3").resolve()),
            "proof_dir": str((temp_dir / "proofs").resolve()),
        },
        "sources": [
            {
                "id": source_id,
                "adapter": adapter,
                "enabled": True,
                "source_instance": source_instance,
                "database_name": database_name,
                "include": include,
                "checkpoint_policy": "after_ack",
                "backfill_mode": "full",
                "connection": connection,
                "options": options,
            }
        ],
    }
    path = temp_dir / f"{source_id}.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    return path


def build_db2_config(temp_dir: Path, stack: MockServiceStack, *, include_event: bool) -> Path:
    options: dict[str, Any] = {
        "capture_mode": "watermark",
        "watermark_column": "UPDATED_AT",
        "commit_timestamp_column": "UPDATED_AT",
        "primary_key_columns": ["ID"],
    }
    if include_event:
        options["dry_run_events"] = [
            {
                "schema_or_namespace": "DB2INST1",
                "table_or_collection": "INVOICES",
                "operation": "snapshot",
                "primary_key": {"ID": 101},
                "change_version": "DB2INST1.INVOICES:2026-04-21T15:00:00Z:101",
                "checkpoint_token": '{"DB2INST1.INVOICES":{"watermark":"2026-04-21T15:00:00Z","pk":[101]}}',
                "commit_timestamp": "2026-04-21T15:00:00Z",
                "after": {"ID": 101, "STATUS": "issued", "UPDATED_AT": "2026-04-21T15:00:00Z"},
                "before": None,
                "metadata": {"capture_mode": "watermark-poll"},
            }
        ]
    return write_dry_run_config(
        temp_dir,
        stack,
        source_id="db2-wave2-service-outage",
        adapter="db2",
        source_instance="erp-eu-1",
        database_name="DENOTARY",
        include={"DB2INST1": ["INVOICES"]},
        connection={
            "host": "127.0.0.1",
            "port": 55000,
            "username": "db2inst1",
            "password": "password",
            "database": "DENOTARY",
        },
        options=options,
    )


def build_cassandra_config(temp_dir: Path, stack: MockServiceStack, *, include_event: bool) -> Path:
    options: dict[str, Any] = {
        "capture_mode": "watermark",
        "watermark_column": "updated_at",
        "commit_timestamp_column": "updated_at",
        "primary_key_columns": ["id"],
    }
    if include_event:
        options["dry_run_events"] = [
            {
                "schema_or_namespace": "ledger",
                "table_or_collection": "invoices",
                "operation": "snapshot",
                "primary_key": {"id": 101},
                "change_version": "ledger.invoices:2026-04-21T15:10:00Z:101",
                "checkpoint_token": '{"ledger.invoices":{"watermark":"2026-04-21T15:10:00Z","pk":[101]}}',
                "commit_timestamp": "2026-04-21T15:10:00Z",
                "after": {"id": 101, "status": "issued", "updated_at": "2026-04-21T15:10:00Z"},
                "before": None,
                "metadata": {"capture_mode": "watermark-poll"},
            }
        ]
    return write_dry_run_config(
        temp_dir,
        stack,
        source_id="cassandra-wave2-service-outage",
        adapter="cassandra",
        source_instance="events-eu-1",
        database_name="denotary_agent",
        include={"denotary_agent": ["invoices"]},
        connection={
            "host": "127.0.0.1",
            "port": 59042,
        },
        options=options,
    )


def build_scylladb_config(temp_dir: Path, stack: MockServiceStack, *, include_event: bool) -> Path:
    options: dict[str, Any] = {
        "capture_mode": "watermark",
        "watermark_column": "updated_at",
        "commit_timestamp_column": "updated_at",
        "primary_key_columns": ["id"],
    }
    if include_event:
        options["dry_run_events"] = [
            {
                "schema_or_namespace": "ledger",
                "table_or_collection": "invoices",
                "operation": "snapshot",
                "primary_key": {"id": 101},
                "change_version": "ledger.invoices:2026-04-21T15:20:00Z:101",
                "checkpoint_token": '{"ledger.invoices":{"watermark":"2026-04-21T15:20:00Z","pk":[101]}}',
                "commit_timestamp": "2026-04-21T15:20:00Z",
                "after": {"id": 101, "status": "issued", "updated_at": "2026-04-21T15:20:00Z"},
                "before": None,
                "metadata": {"capture_mode": "watermark-poll"},
            }
        ]
    return write_dry_run_config(
        temp_dir,
        stack,
        source_id="scylladb-wave2-service-outage",
        adapter="scylladb",
        source_instance="cluster-eu-1",
        database_name="denotary_agent",
        include={"denotary_agent": ["invoices"]},
        connection={
            "host": "127.0.0.1",
            "port": 59043,
        },
        options=options,
    )


def build_elasticsearch_config(temp_dir: Path, stack: MockServiceStack, *, include_event: bool) -> Path:
    options: dict[str, Any] = {
        "capture_mode": "watermark",
        "watermark_field": "updated_at",
        "commit_timestamp_field": "updated_at",
        "primary_key_field": "record_id",
    }
    if include_event:
        options["dry_run_events"] = [
            {
                "schema_or_namespace": "default",
                "table_or_collection": "orders",
                "operation": "snapshot",
                "primary_key": {"record_id": "101"},
                "change_version": "default.orders:2026-04-21T15:30:00Z:101",
                "checkpoint_token": '{"default.orders":{"watermark":"2026-04-21T15:30:00Z","pk":"101"}}',
                "commit_timestamp": "2026-04-21T15:30:00Z",
                "after": {"record_id": "101", "status": "issued", "updated_at": "2026-04-21T15:30:00Z"},
                "before": None,
                "metadata": {"capture_mode": "watermark-poll"},
            }
        ]
    return write_dry_run_config(
        temp_dir,
        stack,
        source_id="elasticsearch-wave2-service-outage",
        adapter="elasticsearch",
        source_instance="search-eu-1",
        database_name="search",
        include={"default": ["orders"]},
        connection={"url": "http://127.0.0.1:59200", "verify_certs": False},
        options=options,
    )


def wait_for_cassandra_ready(timeout_sec: float = 180.0) -> None:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            cluster, _session = create_cassandra_session()
            cluster.shutdown()
            return
        except Exception:
            time.sleep(2)
    raise RuntimeError("cassandra did not become ready in time")


def wait_for_scylladb_ready(timeout_sec: float = 300.0) -> None:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        result = subprocess.run(
            ["docker", "exec", "denotary-db-agent-scylladb-live", "cqlsh", "-e", "describe keyspaces"],
            cwd=str(PROJECT_ROOT),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        if result.returncode == 0:
            return
        time.sleep(5)
    raise RuntimeError("scylladb did not become ready in time")


def wait_for_elasticsearch_ready(timeout_sec: float = 180.0) -> None:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            client = create_elasticsearch_client()
            try:
                if client.ping():
                    return
            finally:
                client.close()
        except Exception:
            pass
        time.sleep(2)
    raise RuntimeError("elasticsearch did not become ready in time")


def prepare_db2_source() -> None:
    connection = create_db2_connection()
    try:
        with connection.cursor() as cursor:
            try:
                cursor.execute(
                    """
                    create table DB2INST1.INVOICES (
                        ID integer not null,
                        STATUS varchar(64),
                        UPDATED_AT timestamp,
                        primary key (ID)
                    )
                    """
                )
            except Exception:
                connection.rollback()
            else:
                connection.commit()
    finally:
        connection.close()


def prepare_cassandra_source() -> None:
    cluster, session = create_cassandra_session()
    try:
        session.execute(
            """
            create keyspace if not exists denotary_agent
            with replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
            """
        )
        session.execute(
            """
            create table if not exists denotary_agent.invoices (
                id int,
                status text,
                updated_at timestamp,
                primary key (id)
            )
            """
        )
    finally:
        cluster.shutdown()


def prepare_scylladb_source() -> None:
    cluster, session = create_scylladb_session()
    try:
        session.execute(
            """
            create keyspace if not exists denotary_agent
            with replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
            """
        )
        session.execute(
            """
            create table if not exists denotary_agent.invoices (
                id int,
                status text,
                updated_at timestamp,
                primary key (id)
            )
            """
        )
    finally:
        cluster.shutdown()


def prepare_elasticsearch_source() -> None:
    client = create_elasticsearch_client()
    try:
        if not client.indices.exists(index="orders"):
            client.indices.create(
                index="orders",
                mappings={
                    "properties": {
                        "record_id": {"type": "keyword"},
                        "status": {"type": "keyword"},
                        "updated_at": {"type": "date"},
                    }
                },
            )
        client.indices.refresh(index="orders")
    finally:
        client.close()


def assert_recovery(result: dict[str, Any]) -> None:
    if result["first"]["processed"] != 0 or result["first"]["failed"] != 1:
        raise RuntimeError(f"{result['adapter']} {result['scenario']} first attempt did not fail as expected")
    if result["second"]["processed"] != 1 or result["second"]["failed"] != 0:
        raise RuntimeError(f"{result['adapter']} {result['scenario']} second attempt did not recover as expected")
    if result["proof_count"] < 1:
        raise RuntimeError(f"{result['adapter']} {result['scenario']} did not export proof after recovery")


def run_sqlite_scenario(name: str, fail_key: str) -> dict[str, Any]:
    stack = MockServiceStack()
    stack.start()
    STATE.fail_counts = {fail_key: 3}
    try:
        with tempfile.TemporaryDirectory() as temp:
            temp_dir = Path(temp)
            database_path = temp_dir / "ledger.sqlite3"
            initialize_sqlite(database_path)
            config_path = write_sqlite_config(temp_dir, stack, database_path)
            engine = AgentEngine(load_config(config_path))
            try:
                engine.bootstrap("sqlite-wave2-service-outage")
                baseline = engine.run_once()
                insert_sqlite_invoice(database_path, record_id=1)
                first = engine.run_once()
                second = engine.run_once()
                result = {
                    "adapter": "sqlite",
                    "scenario": name,
                    "failed_component": fail_key,
                    "baseline_processed": baseline["processed"],
                    "first": first,
                    "second": second,
                    "delivery_count": len(engine.store.list_deliveries("sqlite-wave2-service-outage")),
                    "proof_count": len(engine.store.list_proofs("sqlite-wave2-service-outage")),
                    "dlq_count": len(engine.store.list_dlq("sqlite-wave2-service-outage")),
                }
                if result["baseline_processed"] != 0:
                    raise RuntimeError("sqlite outage validation expected a zero-event baseline run")
                assert_recovery(result)
                return result
            finally:
                engine.close()
    finally:
        stack.stop()


def run_redis_scenario(name: str, fail_key: str) -> dict[str, Any]:
    run_redis_compose("down", "-v")
    run_redis_compose("up", "-d")
    wait_for_redis()
    flush_redis()
    stack = MockServiceStack()
    stack.start()
    STATE.fail_counts = {fail_key: 3}
    try:
        with tempfile.TemporaryDirectory() as temp:
            temp_dir = Path(temp)
            config_path = write_redis_config(temp_dir, stack)
            engine = AgentEngine(load_config(config_path))
            try:
                engine.bootstrap("redis-wave2-service-outage")
                baseline = engine.run_once()
                write_redis_key("orders:0001", "issued")
                first = engine.run_once()
                second = engine.run_once()
                result = {
                    "adapter": "redis",
                    "scenario": name,
                    "failed_component": fail_key,
                    "baseline_processed": baseline["processed"],
                    "first": first,
                    "second": second,
                    "delivery_count": len(engine.store.list_deliveries("redis-wave2-service-outage")),
                    "proof_count": len(engine.store.list_proofs("redis-wave2-service-outage")),
                    "dlq_count": len(engine.store.list_dlq("redis-wave2-service-outage")),
                }
                if result["baseline_processed"] != 0:
                    raise RuntimeError("redis outage validation expected a zero-event baseline run")
                assert_recovery(result)
                return result
            finally:
                engine.close()
    finally:
        stack.stop()
        run_redis_compose("down", "-v")


def run_dry_run_scenario(
    adapter: str,
    name: str,
    fail_key: str,
    config_builder: Any,
) -> dict[str, Any]:
    stack = MockServiceStack()
    stack.start()
    STATE.fail_counts = {fail_key: 3}
    try:
        with tempfile.TemporaryDirectory() as temp:
            temp_dir = Path(temp)
            source_id = f"{adapter}-wave2-service-outage"
            baseline_config_path = config_builder(temp_dir, stack, include_event=False)
            engine = AgentEngine(load_config(baseline_config_path))
            try:
                engine.bootstrap(source_id)
                baseline = engine.run_once()
            finally:
                engine.close()

            active_config_path = config_builder(temp_dir, stack, include_event=True)
            engine = AgentEngine(load_config(active_config_path))
            try:
                engine.bootstrap(source_id)
                first = engine.run_once()
                second = engine.run_once()
                result = {
                    "adapter": adapter,
                    "scenario": name,
                    "failed_component": fail_key,
                    "baseline_processed": baseline["processed"],
                    "first": first,
                    "second": second,
                    "delivery_count": len(engine.store.list_deliveries(source_id)),
                    "proof_count": len(engine.store.list_proofs(source_id)),
                    "dlq_count": len(engine.store.list_dlq(source_id)),
                }
                if result["baseline_processed"] != 0:
                    raise RuntimeError(f"{adapter} outage validation expected a zero-event baseline run")
                assert_recovery(result)
                return result
            finally:
                engine.close()
    finally:
        stack.stop()


def run_dry_run_adapter_with_docker(
    *,
    adapter: str,
    scenarios: list[tuple[str, str]],
    config_builder: Any,
    compose_file: Path,
    env_updates: dict[str, str],
    readiness: Any,
    prepare_source: Any,
) -> list[dict[str, Any]]:
    docker_compose(compose_file, "down", "-v")
    docker_compose(compose_file, "up", "-d")
    results: list[dict[str, Any]] = []
    try:
        with _temporary_env(env_updates):
            readiness()
            prepare_source()
            for name, fail_key in scenarios:
                try:
                    payload = run_dry_run_scenario(adapter, name, fail_key, config_builder)
                    payload["status"] = "passed"
                    results.append(payload)
                except Exception as exc:  # noqa: BLE001
                    results.append(
                        {
                            "adapter": adapter,
                            "scenario": name,
                            "status": "failed",
                            "error": str(exc),
                        }
                    )
        return results
    finally:
        docker_compose(compose_file, "down", "-v")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Wave 2 local service-outage validation")
    parser.add_argument(
        "--adapter",
        choices=("sqlite", "redis", "db2", "cassandra", "scylladb", "elasticsearch", "all"),
        default="all",
        help="adapter to validate",
    )
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()

    scenarios = [
        ("ingress_prepare_outage", "ingress.prepare"),
        ("watcher_register_outage", "watcher.register"),
        ("receipt_fetch_outage", "receipt.get"),
        ("audit_fetch_outage", "audit.get_chain"),
    ]
    adapter_runners: dict[str, Any] = {
        "sqlite": lambda: [
            {**run_sqlite_scenario(name, fail_key), "status": "passed"} for name, fail_key in scenarios
        ],
        "redis": lambda: [
            {**run_redis_scenario(name, fail_key), "status": "passed"} for name, fail_key in scenarios
        ],
        "db2": lambda: run_dry_run_adapter_with_docker(
            adapter="db2",
            scenarios=scenarios,
            config_builder=build_db2_config,
            compose_file=PROJECT_ROOT / "deploy" / "db2-live" / "docker-compose.yml",
            env_updates=DB2_ENV,
            readiness=lambda: wait_for_db2_ready(timeout_sec=900.0, consecutive_successes=3, success_interval_sec=5.0),
            prepare_source=prepare_db2_source,
        ),
        "cassandra": lambda: run_dry_run_adapter_with_docker(
            adapter="cassandra",
            scenarios=scenarios,
            config_builder=build_cassandra_config,
            compose_file=PROJECT_ROOT / "deploy" / "cassandra-live" / "docker-compose.yml",
            env_updates=CASSANDRA_ENV,
            readiness=wait_for_cassandra_ready,
            prepare_source=prepare_cassandra_source,
        ),
        "scylladb": lambda: run_dry_run_adapter_with_docker(
            adapter="scylladb",
            scenarios=scenarios,
            config_builder=build_scylladb_config,
            compose_file=PROJECT_ROOT / "deploy" / "scylladb-live" / "docker-compose.yml",
            env_updates=SCYLLADB_ENV,
            readiness=wait_for_scylladb_ready,
            prepare_source=prepare_scylladb_source,
        ),
        "elasticsearch": lambda: run_dry_run_adapter_with_docker(
            adapter="elasticsearch",
            scenarios=scenarios,
            config_builder=build_elasticsearch_config,
            compose_file=PROJECT_ROOT / "deploy" / "elasticsearch-live" / "docker-compose.yml",
            env_updates=ELASTICSEARCH_ENV,
            readiness=wait_for_elasticsearch_ready,
            prepare_source=prepare_elasticsearch_source,
        ),
    }
    adapters = list(adapter_runners) if args.adapter == "all" else [args.adapter]
    results: list[dict[str, Any]] = []
    for adapter in adapters:
        try:
            results.extend(adapter_runners[adapter]())
        except Exception as exc:  # noqa: BLE001
            results.append({"adapter": adapter, "scenario": "adapter_runner", "status": "failed", "error": str(exc)})
    payload = {"adapters": adapters, "results": results}
    output_root = args.output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    (output_root / "summary.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
