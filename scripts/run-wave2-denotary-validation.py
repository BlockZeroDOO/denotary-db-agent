from __future__ import annotations

import argparse
import json
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from denotary_db_agent.config import load_config
from denotary_db_agent.engine import AgentEngine


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TESTS_ROOT = PROJECT_ROOT / "tests"
DENOTARY_ROOT = Path(r"C:\projects\deNotary")
sys.path.insert(0, str(TESTS_ROOT))

from redis_live_support import REDIS_PORT, REDIS_URL, redis, run_redis_compose, wait_for_redis  # type: ignore


PYTHON_EXE = Path(sys.executable)
DENOTARY_RPC_URL = "https://history.denotary.io"
VERIFICATION_ACCOUNT = "verif"
BILLING_ACCOUNT = "verifbill"
SUBMITTER = "dbagentstest"
SUBMITTER_PERMISSION = "dnanchor"
ENV_FILE = PROJECT_ROOT / "data" / "denotary-live-wallet" / "agent.secrets.env"
SCHEMA_ID = 1
POLICY_ID = 1
WATCHER_TOKEN = "wave2-denotary-token"
DATA_ROOT = PROJECT_ROOT / "data"


def _healthcheck(url: str, timeout_sec: float = 2.0) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=timeout_sec) as response:
            return 200 <= response.status < 300
    except (urllib.error.URLError, TimeoutError):
        return False


class OffchainStack:
    def __init__(self, state_db: Path) -> None:
        self.state_db = state_db
        self.ingress_port = 18080
        self.watcher_port = 18081
        self.receipt_port = 18082
        self.audit_port = 18083
        self.processes: list[subprocess.Popen[str]] = []
        self.ingress_url = ""
        self.watcher_url = ""
        self.receipt_url = ""
        self.audit_url = ""

    def start(self) -> None:
        self.ingress_port = self._reserve_free_port()
        self.watcher_port = self._reserve_free_port()
        self.receipt_port = self._reserve_free_port()
        self.audit_port = self._reserve_free_port()
        self.ingress_url = f"http://127.0.0.1:{self.ingress_port}"
        self.watcher_url = f"http://127.0.0.1:{self.watcher_port}"
        self.receipt_url = f"http://127.0.0.1:{self.receipt_port}"
        self.audit_url = f"http://127.0.0.1:{self.audit_port}"
        self.state_db.parent.mkdir(parents=True, exist_ok=True)

        ingress_command = [
            str(PYTHON_EXE),
            str(DENOTARY_ROOT / "services" / "ingress_api.py"),
            "--host",
            "127.0.0.1",
            "--port",
            str(self.ingress_port),
            "--verification-account",
            VERIFICATION_ACCOUNT,
            "--billing-account",
            BILLING_ACCOUNT,
        ]
        watcher_command = [
            str(PYTHON_EXE),
            str(DENOTARY_ROOT / "services" / "finality_watcher.py"),
            "--host",
            "127.0.0.1",
            "--port",
            str(self.watcher_port),
            "--rpc-url",
            DENOTARY_RPC_URL,
            "--state-backend",
            "sqlite",
            "--state-db",
            str(self.state_db),
            "--poll-interval-sec",
            "1",
            "--auth-token",
            WATCHER_TOKEN,
            "--verification-policy",
            "single-provider",
            "--verification-min-success",
            "1",
        ]
        receipt_command = [
            str(PYTHON_EXE),
            str(DENOTARY_ROOT / "services" / "receipt_service.py"),
            "--host",
            "127.0.0.1",
            "--port",
            str(self.receipt_port),
            "--state-backend",
            "sqlite",
            "--state-db",
            str(self.state_db),
            "--privacy-mode",
            "full",
        ]
        audit_command = [
            str(PYTHON_EXE),
            str(DENOTARY_ROOT / "services" / "audit_api.py"),
            "--host",
            "127.0.0.1",
            "--port",
            str(self.audit_port),
            "--state-backend",
            "sqlite",
            "--state-db",
            str(self.state_db),
            "--privacy-mode",
            "full",
        ]

        self.processes.append(self._start_process(ingress_command))
        self.processes.append(self._start_process(watcher_command))
        self._wait_urls([f"{self.ingress_url}/healthz", f"{self.watcher_url}/healthz"])
        deadline = time.time() + 10
        while time.time() < deadline and not self.state_db.exists():
            time.sleep(0.2)
        self.processes.append(self._start_process(receipt_command))
        self.processes.append(self._start_process(audit_command))
        self._wait_healthy()

    def _start_process(self, command: list[str]) -> subprocess.Popen[str]:
        return subprocess.Popen(
            command,
            cwd=str(DENOTARY_ROOT),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            text=True,
        )

    def _reserve_free_port(self) -> int:
        import socket

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as handle:
            handle.bind(("127.0.0.1", 0))
            return int(handle.getsockname()[1])

    def _wait_urls(self, urls: list[str], timeout_sec: float = 30.0) -> None:
        deadline = time.time() + timeout_sec
        while time.time() < deadline:
            if all(_healthcheck(url) for url in urls):
                return
            time.sleep(0.5)
        raise RuntimeError("local deNotary services did not become healthy in time")

    def _wait_healthy(self, timeout_sec: float = 30.0) -> None:
        self._wait_urls(
            [
                f"{self.ingress_url}/healthz",
                f"{self.watcher_url}/healthz",
                f"{self.receipt_url}/healthz",
                f"{self.audit_url}/healthz",
            ],
            timeout_sec=timeout_sec,
        )

    def stop(self) -> None:
        for process in reversed(self.processes):
            if process.poll() is None:
                process.terminate()
        deadline = time.time() + 10
        for process in reversed(self.processes):
            if process.poll() is None:
                remaining = max(0.1, deadline - time.time())
                try:
                    process.wait(timeout=remaining)
                except subprocess.TimeoutExpired:
                    process.kill()
        self.processes = []


def _redis_client():
    if redis is None:
        raise RuntimeError("redis package is required for Redis denotary validation")
    return redis.Redis.from_url(REDIS_URL, decode_responses=False, socket_timeout=5)


def flush_redis() -> None:
    wait_for_redis()
    client = _redis_client()
    try:
        client.flushdb()
    finally:
        close = getattr(client, "close", None)
        if callable(close):
            close()


def write_redis_key(key: str, value: str) -> None:
    client = _redis_client()
    try:
        client.set(key, value)
    finally:
        close = getattr(client, "close", None)
        if callable(close):
            close()


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


def insert_sqlite_invoice(database_path: Path, *, record_id: int, status: str, updated_at: str) -> None:
    connection = sqlite3.connect(str(database_path))
    try:
        connection.execute(
            "insert into invoices (id, status, updated_at) values (?, ?, ?)",
            (record_id, status, updated_at),
        )
        connection.commit()
    finally:
        connection.close()


def build_run_marker(prefix: str) -> dict[str, str | int]:
    nonce = uuid4().hex[:12]
    timestamp = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return {
        "record_id": int(time.time()),
        "status": f"{prefix}-{nonce}",
        "timestamp": timestamp,
        "key": f"{prefix}:{nonce}",
        "value": f"{prefix}-value-{nonce}",
    }


def load_proof_bundle(proof_entry: dict[str, Any]) -> dict[str, Any]:
    export_path = Path(str(proof_entry["export_path"]))
    return json.loads(export_path.read_text(encoding="utf-8"))


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def build_denotary_config_for_stack(stack: OffchainStack) -> dict[str, Any]:
    return {
        "ingress_url": stack.ingress_url,
        "watcher_url": stack.watcher_url,
        "watcher_auth_token": WATCHER_TOKEN,
        "receipt_url": stack.receipt_url,
        "audit_url": stack.audit_url,
        "chain_rpc_url": DENOTARY_RPC_URL,
        "submitter": SUBMITTER,
        "submitter_permission": SUBMITTER_PERMISSION,
        "broadcast_backend": "private_key_env",
        "submitter_private_key_env": "DENOTARY_SUBMITTER_PRIVATE_KEY",
        "env_file": str(ENV_FILE.resolve()),
        "schema_id": SCHEMA_ID,
        "policy_id": POLICY_ID,
        "billing_account": BILLING_ACCOUNT,
        "wait_for_finality": True,
        "finality_timeout_sec": 120,
        "finality_poll_interval_sec": 1.0,
    }


def build_sqlite_config(temp_dir: Path, database_path: Path, stack: OffchainStack) -> Path:
    config = {
        "agent_name": "wave2-sqlite-denotary-validation",
        "log_level": "INFO",
        "denotary": build_denotary_config_for_stack(stack),
        "storage": {
            "state_db": str((temp_dir / "state.sqlite3").resolve()),
            "proof_dir": str((temp_dir / "proofs").resolve()),
        },
        "sources": [
            {
                "id": "sqlite-wave2-denotary",
                "adapter": "sqlite",
                "enabled": True,
                "source_instance": "edge-device-denotary",
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
    path = temp_dir / "sqlite-denotary-config.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    return path


def build_redis_config(temp_dir: Path, stack: OffchainStack) -> Path:
    config = {
        "agent_name": "wave2-redis-denotary-validation",
        "log_level": "INFO",
        "denotary": build_denotary_config_for_stack(stack),
        "storage": {
            "state_db": str((temp_dir / "state.sqlite3").resolve()),
            "proof_dir": str((temp_dir / "proofs").resolve()),
        },
        "sources": [
            {
                "id": "redis-wave2-denotary",
                "adapter": "redis",
                "enabled": True,
                "source_instance": "cache-denotary",
                "database_name": "db0",
                "include": {"0": ["orders:*"]},
                "checkpoint_policy": "after_ack",
                "backfill_mode": "full",
                "connection": {"host": "127.0.0.1", "port": REDIS_PORT},
                "options": {
                    "capture_mode": "scan",
                    "row_limit": 100,
                    "scan_count": 100,
                },
            }
        ],
    }
    path = temp_dir / "redis-denotary-config.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    return path


def run_sqlite_validation(temp_dir: Path) -> dict[str, Any]:
    temp_dir.mkdir(parents=True, exist_ok=True)
    database_path = temp_dir / "ledger.sqlite3"
    state_db = temp_dir / "offchain-state.sqlite3"
    initialize_sqlite(database_path)
    stack = OffchainStack(state_db)
    stack.start()
    try:
        config_path = build_sqlite_config(temp_dir, database_path, stack)
        engine = AgentEngine(load_config(config_path))
        try:
            marker = build_run_marker("sqlite-denotary")
            doctor = engine.doctor("sqlite-wave2-denotary")
            if doctor["overall"]["severity"] == "critical":
                raise RuntimeError(f"sqlite denotary doctor failed: {doctor['errors']}")
            baseline = engine.run_once()
            insert_sqlite_invoice(
                database_path,
                record_id=int(marker["record_id"]),
                status=str(marker["status"]),
                updated_at=str(marker["timestamp"]),
            )
            result = engine.run_once()
            proofs = engine.store.list_proofs("sqlite-wave2-denotary")
            deliveries = engine.store.list_deliveries("sqlite-wave2-denotary")
            if result["processed"] != 1 or result["failed"] != 0:
                raise RuntimeError("sqlite denotary run did not process exactly one event")
            if len(proofs) != 1 or len(deliveries) != 1:
                raise RuntimeError("sqlite denotary run did not export exactly one proof bundle")
            proof = proofs[0]
            proof_bundle = load_proof_bundle(proof)
            receipt = dict(proof_bundle.get("receipt") or {})
            delivery = deliveries[0]
            return {
                "adapter": "sqlite",
                "status": "passed",
                "baseline_processed": baseline["processed"],
                "processed": result["processed"],
                "failed": result["failed"],
                "request_id": str(proof["request_id"]),
                "tx_id": str(delivery["tx_id"] or receipt.get("tx_id") or ""),
                "block_num": int(receipt.get("block_num") or 0),
                "proof_path": proof["export_path"],
                "broadcast_backend": doctor["signer"]["effective_broadcast_backend"],
                "finality_mode": "finalized_exported",
            }
        finally:
            engine.close()
    finally:
        stack.stop()


def run_redis_validation(temp_dir: Path) -> dict[str, Any]:
    temp_dir.mkdir(parents=True, exist_ok=True)
    run_redis_compose("down", "-v")
    run_redis_compose("up", "-d")
    wait_for_redis()
    flush_redis()
    state_db = temp_dir / "offchain-state.sqlite3"
    stack = OffchainStack(state_db)
    stack.start()
    try:
        config_path = build_redis_config(temp_dir, stack)
        engine = AgentEngine(load_config(config_path))
        try:
            marker = build_run_marker("orders")
            doctor = engine.doctor("redis-wave2-denotary")
            if doctor["overall"]["severity"] == "critical":
                raise RuntimeError(f"redis denotary doctor failed: {doctor['errors']}")
            baseline = engine.run_once()
            write_redis_key(str(marker["key"]), str(marker["value"]))
            result = engine.run_once()
            proofs = engine.store.list_proofs("redis-wave2-denotary")
            deliveries = engine.store.list_deliveries("redis-wave2-denotary")
            if result["processed"] != 1 or result["failed"] != 0:
                raise RuntimeError("redis denotary run did not process exactly one event")
            if len(proofs) != 1 or len(deliveries) != 1:
                raise RuntimeError("redis denotary run did not export exactly one proof bundle")
            proof = proofs[0]
            proof_bundle = load_proof_bundle(proof)
            receipt = dict(proof_bundle.get("receipt") or {})
            delivery = deliveries[0]
            return {
                "adapter": "redis",
                "status": "passed",
                "baseline_processed": baseline["processed"],
                "processed": result["processed"],
                "failed": result["failed"],
                "request_id": str(proof["request_id"]),
                "tx_id": str(delivery["tx_id"] or receipt.get("tx_id") or ""),
                "block_num": int(receipt.get("block_num") or 0),
                "proof_path": proof["export_path"],
                "broadcast_backend": doctor["signer"]["effective_broadcast_backend"],
                "finality_mode": "finalized_exported",
            }
        finally:
            engine.close()
    finally:
        stack.stop()
        run_redis_compose("down", "-v")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Wave 2 denotary validation")
    parser.add_argument("--adapter", choices=("sqlite", "redis", "all"), default="all")
    parser.add_argument("--output-root", default="", help="Optional directory for persistent run artifacts.")
    args = parser.parse_args()

    adapters = ["sqlite", "redis"] if args.adapter == "all" else [args.adapter]
    run_root = (
        Path(args.output_root).resolve()
        if args.output_root
        else (DATA_ROOT / f"wave2-denotary-validation-{utc_stamp().lower()}").resolve()
    )
    run_root.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, Any]] = []
    for adapter in adapters:
        try:
            adapter_root = run_root / adapter
            payload = run_sqlite_validation(adapter_root) if adapter == "sqlite" else run_redis_validation(adapter_root)
            results.append(payload)
        except Exception as exc:  # noqa: BLE001
            results.append({"adapter": adapter, "status": "failed", "error": str(exc)})
    summary = {
        "network": "denotary",
        "rpc_url": DENOTARY_RPC_URL,
        "submitter": SUBMITTER,
        "submitter_permission": SUBMITTER_PERMISSION,
        "run_root": str(run_root),
        "results": results,
    }
    (run_root / "summary.json").write_text(
        json.dumps(summary, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
