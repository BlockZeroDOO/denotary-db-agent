from __future__ import annotations

import argparse
import importlib.util
import json
import sys
import tempfile
import threading
import time
from datetime import datetime, timezone
from http.server import ThreadingHTTPServer
from pathlib import Path
from typing import Any

from denotary_db_agent.config import load_config
from denotary_db_agent.engine import AgentEngine


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TESTS_ROOT = PROJECT_ROOT / "tests"

sys.path.insert(0, str(TESTS_ROOT))

from redis_live_support import REDIS_PORT, REDIS_URL, redis, run_redis_compose, wait_for_redis  # type: ignore


def _load_module(module_path: Path, module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


FULL_CYCLE = _load_module(TESTS_ROOT / "test_redis_full_cycle_live_integration.py", "wave2_redis_full_cycle")


class IncrementingIngressHandler(FULL_CYCLE.BaseHTTPRequestHandler):
    counter = 0

    def do_POST(self) -> None:  # noqa: N802
        IncrementingIngressHandler.counter += 1
        body = self.rfile.read(int(self.headers.get("Content-Length", "0"))).decode("utf-8")
        payload = json.loads(body)
        response = {
            "request_id": f"wave2-redis-request-{IncrementingIngressHandler.counter}",
            "trace_id": f"wave2-redis-trace-{IncrementingIngressHandler.counter}",
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
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, format: str, *args: Any) -> None:
        return


class MockServiceStack:
    def __init__(self) -> None:
        self.ingress_port = FULL_CYCLE.free_port()
        self.watcher_port = FULL_CYCLE.free_port()
        self.receipt_port = FULL_CYCLE.free_port()
        self.audit_port = FULL_CYCLE.free_port()
        self.chain_port = FULL_CYCLE.free_port()
        FULL_CYCLE.MockState.requests = {}
        IncrementingIngressHandler.counter = 0
        self.servers = [
            ThreadingHTTPServer(("127.0.0.1", self.ingress_port), IncrementingIngressHandler),
            ThreadingHTTPServer(("127.0.0.1", self.watcher_port), FULL_CYCLE.MockWatcherHandler),
            ThreadingHTTPServer(("127.0.0.1", self.receipt_port), FULL_CYCLE.MockReceiptHandler),
            ThreadingHTTPServer(("127.0.0.1", self.audit_port), FULL_CYCLE.MockAuditHandler),
            ThreadingHTTPServer(("127.0.0.1", self.chain_port), FULL_CYCLE.MockChainHandler),
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


def _redis_client():
    if redis is None:
        raise RuntimeError("redis package is required for Wave 2 Redis validation")
    return redis.Redis.from_url(REDIS_URL, decode_responses=False, socket_timeout=5)


def flushdb() -> None:
    wait_for_redis()
    last_error: Exception | None = None
    for _ in range(3):
        client = _redis_client()
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


def write_key(key: str, value: Any) -> None:
    client = _redis_client()
    try:
        if isinstance(value, dict):
            client.hset(key, mapping=value)
        else:
            client.set(key, value)
    finally:
        close = getattr(client, "close", None)
        if callable(close):
            close()


def build_config(temp_dir: Path, mock_stack: MockServiceStack, *, source_id: str) -> Path:
    config = {
        "agent_name": f"{source_id}-validation",
        "log_level": "INFO",
        "denotary": {
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
        },
        "storage": {
            "state_db": str((temp_dir / "state.sqlite3").resolve()),
            "proof_dir": str((temp_dir / "proofs").resolve()),
        },
        "sources": [
            {
                "id": source_id,
                "adapter": "redis",
                "enabled": True,
                "source_instance": "wave2-validation",
                "database_name": "db0",
                "include": {"0": ["orders:*"]},
                "connection": {
                    "host": "127.0.0.1",
                    "port": REDIS_PORT,
                },
                "backfill_mode": "full",
                "batch_enabled": False,
                "batch_size": 100,
                "options": {
                    "capture_mode": "scan",
                    "row_limit": 100,
                    "scan_count": 100,
                },
            }
        ],
    }
    path = temp_dir / "config.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    return path


def restart_validation() -> dict[str, Any]:
    run_redis_compose("down", "-v")
    run_redis_compose("up", "-d")
    wait_for_redis()
    flushdb()
    mock = MockServiceStack()
    mock.start()
    try:
        with tempfile.TemporaryDirectory() as temp:
            temp_dir = Path(temp)
            source_id = "redis-wave2-restart"
            config_path = build_config(temp_dir, mock, source_id=source_id)
            engine = AgentEngine(load_config(config_path))
            try:
                engine.bootstrap(source_id)
                baseline = engine.run_once()
                for index in range(1, 4):
                    write_key(f"orders:{index:04d}", f"issued-{index}")
                first = engine.run_once()
                run_redis_compose("restart", "redis")
                wait_for_redis()
                for index in range(4, 7):
                    write_key(f"orders:{index:04d}", {"status": "paid", "sequence": str(index)})
                second = engine.run_once()
                deliveries = engine.store.list_deliveries(source_id)
                proofs = engine.store.list_proofs(source_id)
                dlq = engine.store.list_dlq(source_id)
                result = {
                    "adapter": "redis",
                    "validation": "restart",
                    "baseline_processed": baseline["processed"],
                    "first_processed": first["processed"],
                    "second_processed": second["processed"],
                    "delivery_count": len(deliveries),
                    "proof_count": len(proofs),
                    "dlq_count": len(dlq),
                }
                if result["baseline_processed"] != 0:
                    raise RuntimeError("redis restart validation expected a zero-event baseline run")
                if result["first_processed"] != 3 or result["second_processed"] != 3:
                    raise RuntimeError("redis restart validation did not process 3 + 3 events")
                if result["delivery_count"] != 6 or result["proof_count"] != 6 or result["dlq_count"] != 0:
                    raise RuntimeError("redis restart validation produced unexpected delivery/proof/DLQ counts")
                return result
            finally:
                engine.close()
    finally:
        mock.stop()
        run_redis_compose("down", "-v")


def short_soak_validation(*, cycles: int, events_per_cycle: int) -> dict[str, Any]:
    run_redis_compose("down", "-v")
    run_redis_compose("up", "-d")
    wait_for_redis()
    flushdb()
    mock = MockServiceStack()
    mock.start()
    try:
        with tempfile.TemporaryDirectory() as temp:
            temp_dir = Path(temp)
            source_id = "redis-wave2-short-soak"
            config_path = build_config(temp_dir, mock, source_id=source_id)
            engine = AgentEngine(load_config(config_path))
            try:
                engine.bootstrap(source_id)
                baseline = engine.run_once()
                total_processed = baseline["processed"]
                total_failed = baseline["failed"]
                cycle_results: list[dict[str, int]] = []
                for cycle in range(cycles):
                    for index in range(1, events_per_cycle + 1):
                        key_id = cycle * 100 + index
                        value = (
                            f"issued-{key_id}"
                            if cycle % 2 == 0
                            else {"status": "paid", "sequence": str(key_id), "updated_at": datetime.now(timezone.utc).isoformat()}
                        )
                        write_key(f"orders:{key_id:04d}", value)
                    result = engine.run_once()
                    cycle_results.append(result)
                    total_processed += result["processed"]
                    total_failed += result["failed"]
                deliveries = engine.store.list_deliveries(source_id)
                proofs = engine.store.list_proofs(source_id)
                dlq = engine.store.list_dlq(source_id)
                expected_total = cycles * events_per_cycle
                payload = {
                    "adapter": "redis",
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
                if payload["baseline_processed"] != 0:
                    raise RuntimeError("redis short-soak validation expected a zero-event baseline run")
                if payload["total_processed"] != expected_total:
                    raise RuntimeError(f"redis short-soak processed {payload['total_processed']} events, expected {expected_total}")
                if payload["total_failed"] != 0:
                    raise RuntimeError("redis short-soak reported failures")
                if payload["delivery_count"] != expected_total or payload["proof_count"] != expected_total or payload["dlq_count"] != 0:
                    raise RuntimeError("redis short-soak produced unexpected delivery/proof/DLQ counts")
                return payload
            finally:
                engine.close()
    finally:
        mock.stop()
        run_redis_compose("down", "-v")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Wave 2 Redis validation.")
    parser.add_argument("--mode", choices=("restart", "short-soak"), default="restart")
    parser.add_argument("--cycles", type=int, default=5)
    parser.add_argument("--events-per-cycle", type=int, default=3)
    args = parser.parse_args()

    if args.mode == "restart":
        payload = restart_validation()
    else:
        payload = short_soak_validation(cycles=args.cycles, events_per_cycle=args.events_per_cycle)
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
