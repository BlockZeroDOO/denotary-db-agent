from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from denotary_db_agent.config import load_config
from denotary_db_agent.engine import AgentEngine


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TESTS_ROOT = PROJECT_ROOT / "tests"
sys.path.insert(0, str(TESTS_ROOT))

from cassandra_live_support import (  # type: ignore
    agent_connection_config,
    cassandra_keyspace,
    create_session,
    free_port,
    unique_table_name,
    wait_for_table_visibility,
)


CASSANDRA_ENV = {
    "DENOTARY_CASSANDRA_HOST": "127.0.0.1",
    "DENOTARY_CASSANDRA_PORT": "59042",
    "DENOTARY_CASSANDRA_KEYSPACE": "denotary_agent",
}


class IncrementingIngressHandler(BaseHTTPRequestHandler):
    counter = 0

    def do_POST(self) -> None:  # noqa: N802
        IncrementingIngressHandler.counter += 1
        body = self.rfile.read(int(self.headers.get("Content-Length", "0"))).decode("utf-8")
        payload = json.loads(body)
        response = {
            "request_id": f"wave2-cassandra-request-{IncrementingIngressHandler.counter}",
            "trace_id": f"wave2-cassandra-trace-{IncrementingIngressHandler.counter}",
            "external_ref_hash": f"{IncrementingIngressHandler.counter:064x}",
            "object_hash": f"{IncrementingIngressHandler.counter + 1000:064x}",
            "verification_account": "verif",
            "prepared_action": {
                "contract": "verifbill",
                "action": "submit",
                "data": {
                    "payer": payload["submitter"],
                    "submitter": payload["submitter"],
                    "schema_id": 1,
                    "policy_id": 1,
                    "object_hash": f"{IncrementingIngressHandler.counter + 1000:064x}",
                    "external_ref": f"{IncrementingIngressHandler.counter:064x}",
                },
            },
        }
        encoded = json.dumps(response).encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, format: str, *args: Any) -> None:
        return


class MockState:
    requests: dict[str, dict[str, Any]] = {}


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
                    "finalized_at": "2026-04-21T12:00:00Z",
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
            self._send_json({"status": "ok", "service": "receipt-service"})
            return
        if not self.path.startswith("/v1/receipts/"):
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        request_id = self.path.split("/")[-1]
        state = MockState.requests[request_id]
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
            self._send_json({"status": "ok", "service": "audit-api"})
            return
        if not self.path.startswith("/v1/audit/chain/"):
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        request_id = self.path.split("/")[-1]
        state = MockState.requests[request_id]
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
                "transaction_id": f"{IncrementingIngressHandler.counter + 5000:064x}",
                "processed": {"block_num": 983000 + IncrementingIngressHandler.counter},
            }
        else:
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(HTTPStatus.OK)
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
        IncrementingIngressHandler.counter = 0
        MockState.requests = {}
        self.servers = [
            ThreadingHTTPServer(("127.0.0.1", self.ingress_port), IncrementingIngressHandler),
            ThreadingHTTPServer(("127.0.0.1", self.watcher_port), MockWatcherHandler),
            ThreadingHTTPServer(("127.0.0.1", self.receipt_port), MockReceiptHandler),
            ThreadingHTTPServer(("127.0.0.1", self.audit_port), MockAuditHandler),
            ThreadingHTTPServer(("127.0.0.1", self.chain_port), MockChainHandler),
        ]
        self.threads = [threading.Thread(target=server.serve_forever, daemon=True) for server in self.servers]

    def start(self) -> None:
        for thread in self.threads:
            thread.start()

    def stop(self) -> None:
        for server in self.servers:
            server.shutdown()
            server.server_close()
        for thread in self.threads:
            thread.join(timeout=2)


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


def _docker_compose(*args: str) -> None:
    compose_file = PROJECT_ROOT / "deploy" / "cassandra-live" / "docker-compose.yml"
    command = ["docker", "compose", "-f", str(compose_file), *args]
    subprocess.run(command, cwd=str(PROJECT_ROOT), check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def _wait_for_cassandra(timeout_sec: float = 180.0) -> None:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            cluster, _session = create_session()
            cluster.shutdown()
            return
        except Exception:
            time.sleep(2)
    raise RuntimeError("cassandra did not become ready in time")


def create_table(table_name: str) -> None:
    cluster, session = create_session()
    try:
        session.execute(
            """
            create keyspace if not exists denotary_agent
            with replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
            """
        )
        session.execute(
            f"""
            create table {cassandra_keyspace()}.{table_name} (
                id int,
                status text,
                updated_at timestamp,
                primary key (id)
            )
            """
        )
    finally:
        cluster.shutdown()
    wait_for_table_visibility(table_name)


def drop_table(table_name: str) -> None:
    cluster, session = create_session()
    try:
        session.execute(f"drop table if exists {cassandra_keyspace()}.{table_name}")
    finally:
        cluster.shutdown()


def insert_row(table_name: str, *, row_id: int, status: str, updated_at: str) -> None:
    cluster, session = create_session()
    try:
        session.execute(
            f"""
            insert into {cassandra_keyspace()}.{table_name} (id, status, updated_at)
            values (%s, %s, %s)
            """,
            (row_id, status, updated_at),
        )
    finally:
        cluster.shutdown()


def build_config(temp_dir: Path, stack: MockServiceStack, *, source_id: str, table_name: str) -> Path:
    config = {
        "agent_name": f"{source_id}-validation",
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
                "adapter": "cassandra",
                "enabled": True,
                "source_instance": "cluster-eu-1",
                "database_name": cassandra_keyspace(),
                "include": {
                    cassandra_keyspace(): [table_name],
                },
                "connection": agent_connection_config(),
                "backfill_mode": "full",
                "batch_size": 100,
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
    path = temp_dir / "config.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    return path


def restart_validation() -> dict[str, Any]:
    stack = MockServiceStack()
    stack.start()
    try:
        with tempfile.TemporaryDirectory() as temp:
            temp_dir = Path(temp)
            table_name = unique_table_name("denotary_wave2_restart")
            create_table(table_name)
            source_id = "cassandra-wave2-restart"
            config_path = build_config(temp_dir, stack, source_id=source_id, table_name=table_name)
            try:
                engine = AgentEngine(load_config(config_path))
                try:
                    engine.bootstrap(source_id)
                    baseline = engine.run_once()
                    for index in range(1, 4):
                        insert_row(
                            table_name,
                            row_id=index,
                            status="issued",
                            updated_at=f"2026-04-21T12:10:0{index}Z",
                        )
                    first = engine.run_once()
                finally:
                    engine.close()

                engine = AgentEngine(load_config(config_path))
                try:
                    engine.bootstrap(source_id)
                    for index in range(4, 7):
                        insert_row(
                            table_name,
                            row_id=index,
                            status="paid",
                            updated_at=f"2026-04-21T12:20:0{index - 3}Z",
                        )
                    second = engine.run_once()
                    deliveries = engine.store.list_deliveries(source_id)
                    proofs = engine.store.list_proofs(source_id)
                    dlq = engine.store.list_dlq(source_id)
                finally:
                    engine.close()
            finally:
                drop_table(table_name)

            result = {
                "adapter": "cassandra",
                "validation": "restart",
                "baseline_processed": baseline["processed"],
                "first_processed": first["processed"],
                "second_processed": second["processed"],
                "delivery_count": len(deliveries),
                "proof_count": len(proofs),
                "dlq_count": len(dlq),
            }
            if result["baseline_processed"] != 0:
                raise RuntimeError("cassandra restart validation expected a zero-event baseline run")
            if result["first_processed"] != 3 or result["second_processed"] != 3:
                raise RuntimeError("cassandra restart validation did not process 3 + 3 events")
            if result["delivery_count"] != 6 or result["proof_count"] != 6 or result["dlq_count"] != 0:
                raise RuntimeError("cassandra restart validation produced unexpected delivery/proof/DLQ counts")
            return result
    finally:
        stack.stop()


def short_soak_validation(*, cycles: int, events_per_cycle: int) -> dict[str, Any]:
    stack = MockServiceStack()
    stack.start()
    try:
        with tempfile.TemporaryDirectory() as temp:
            temp_dir = Path(temp)
            table_name = unique_table_name("denotary_wave2_short_soak")
            create_table(table_name)
            source_id = "cassandra-wave2-short-soak"
            config_path = build_config(temp_dir, stack, source_id=source_id, table_name=table_name)
            try:
                engine = AgentEngine(load_config(config_path))
                try:
                    engine.bootstrap(source_id)
                    baseline = engine.run_once()
                    cycle_results: list[dict[str, int]] = []
                    clock = datetime(2026, 4, 21, 12, 30, 0, tzinfo=timezone.utc)
                    counter = 0
                    for cycle in range(cycles):
                        for _ in range(events_per_cycle):
                            counter += 1
                            updated_at = (clock + timedelta(seconds=counter)).strftime("%Y-%m-%dT%H:%M:%SZ")
                            insert_row(
                                table_name,
                                row_id=counter,
                                status=f"cycle-{cycle + 1}",
                                updated_at=updated_at,
                            )
                        cycle_result = engine.run_once()
                        cycle_results.append({"processed": cycle_result["processed"], "failed": cycle_result["failed"]})
                    deliveries = engine.store.list_deliveries(source_id)
                    proofs = engine.store.list_proofs(source_id)
                    dlq = engine.store.list_dlq(source_id)
                finally:
                    engine.close()
            finally:
                drop_table(table_name)

            total_processed = sum(item["processed"] for item in cycle_results)
            total_failed = sum(item["failed"] for item in cycle_results)
            expected_total = cycles * events_per_cycle
            result = {
                "adapter": "cassandra",
                "validation": "short-soak",
                "cycles": cycles,
                "events_per_cycle": events_per_cycle,
                "baseline_processed": baseline["processed"],
                "cycle_results": cycle_results,
                "total_processed": total_processed,
                "total_failed": total_failed,
                "delivery_count": len(deliveries),
                "proof_count": len(proofs),
                "dlq_count": len(dlq),
            }
            if result["baseline_processed"] != 0:
                raise RuntimeError("cassandra short-soak validation expected a zero-event baseline run")
            if total_processed != expected_total or total_failed != 0:
                raise RuntimeError("cassandra short-soak validation did not process the expected number of events")
            if result["delivery_count"] != expected_total or result["proof_count"] != expected_total or result["dlq_count"] != 0:
                raise RuntimeError("cassandra short-soak validation produced unexpected delivery/proof/DLQ counts")
            return result
    finally:
        stack.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Wave 2 Apache Cassandra validation")
    parser.add_argument("--mode", choices=("restart", "short-soak"), required=True)
    parser.add_argument("--cycles", type=int, default=5)
    parser.add_argument("--events-per-cycle", type=int, default=3)
    args = parser.parse_args()

    with _temporary_env(CASSANDRA_ENV):
        _docker_compose("down", "-v")
        _docker_compose("up", "-d")
        try:
            _wait_for_cassandra()
            if args.mode == "restart":
                result = restart_validation()
            else:
                result = short_soak_validation(cycles=args.cycles, events_per_cycle=args.events_per_cycle)
        finally:
            _docker_compose("down", "-v")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
