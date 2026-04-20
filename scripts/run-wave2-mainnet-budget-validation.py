from __future__ import annotations

import argparse
import importlib.util
import json
import math
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from denotary_db_agent.config import load_config
from denotary_db_agent.engine import AgentEngine


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = PROJECT_ROOT / "data"

APPROX_SINGLE_EVENT_BYTES = 88
DEFAULT_TARGET_KIB = 25
DEFAULT_BATCH_SIZE = 100


def _load_script(module_path: Path, module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


DENOTARY_VALIDATION = _load_script(
    PROJECT_ROOT / "scripts" / "run-wave2-denotary-validation.py",
    "wave2_denotary_validation",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Wave 2 denotary mainnet budget validation.")
    parser.add_argument("--adapter", choices=("sqlite", "redis", "all"), default="all")
    parser.add_argument("--target-kib", type=int, default=DEFAULT_TARGET_KIB)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--output-root", default="", help="Optional persistent run directory.")
    return parser.parse_args()


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _approx_batch_kib(batch_size: int) -> int:
    return max(1, math.ceil((APPROX_SINGLE_EVENT_BYTES * batch_size) / 1024))


def _cycle_count_for_budget(target_kib: int, batch_size: int) -> int:
    return max(1, math.ceil(target_kib / _approx_batch_kib(batch_size)))


def _proof_payload(proof_entry: dict[str, Any]) -> dict[str, Any]:
    export_path = Path(str(proof_entry["export_path"]))
    return json.loads(export_path.read_text(encoding="utf-8"))


def _write_redis_value(key: str, value: Any) -> None:
    client = DENOTARY_VALIDATION._redis_client()
    try:
        if isinstance(value, dict):
            client.hset(key, mapping=value)
        else:
            client.set(key, value)
    finally:
        close = getattr(client, "close", None)
        if callable(close):
            close()


def _build_sqlite_config(
    temp_dir: Path,
    database_path: Path,
    stack: Any,
    *,
    source_id: str,
    batch_size: int,
) -> Path:
    config = {
        "agent_name": source_id,
        "log_level": "INFO",
        "denotary": {
            **DENOTARY_VALIDATION.build_denotary_config_for_stack(stack),
            "policy_id": 2,
        },
        "storage": {
            "state_db": str((temp_dir / "state.sqlite3").resolve()),
            "proof_dir": str((temp_dir / "proofs").resolve()),
        },
        "sources": [
            {
                "id": source_id,
                "adapter": "sqlite",
                "enabled": True,
                "source_instance": "wave2-sqlite-mainnet-budget",
                "database_name": "ledger",
                "include": {"main": ["invoices"]},
                "checkpoint_policy": "after_ack",
                "backfill_mode": "full",
                "batch_enabled": True,
                "batch_size": batch_size,
                "connection": {"path": str(database_path.resolve())},
                "options": {
                    "capture_mode": "watermark",
                    "watermark_column": "updated_at",
                    "commit_timestamp_column": "updated_at",
                    "primary_key_columns": ["id"],
                    "row_limit": batch_size,
                },
            }
        ],
    }
    path = temp_dir / "sqlite-mainnet-budget-config.json"
    path.write_text(json.dumps(config, indent=2), encoding="utf-8")
    return path


def _build_redis_config(
    temp_dir: Path,
    stack: Any,
    *,
    source_id: str,
    batch_size: int,
) -> Path:
    config = {
        "agent_name": source_id,
        "log_level": "INFO",
        "denotary": {
            **DENOTARY_VALIDATION.build_denotary_config_for_stack(stack),
            "policy_id": 2,
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
                "source_instance": "wave2-redis-mainnet-budget",
                "database_name": "db0",
                "include": {"0": ["orders:*"]},
                "checkpoint_policy": "after_ack",
                "backfill_mode": "full",
                "batch_enabled": True,
                "batch_size": batch_size,
                "connection": {"host": "127.0.0.1", "port": DENOTARY_VALIDATION.REDIS_PORT},
                "options": {
                    "capture_mode": "scan",
                    "row_limit": batch_size,
                    "scan_count": batch_size,
                },
            }
        ],
    }
    path = temp_dir / "redis-mainnet-budget-config.json"
    path.write_text(json.dumps(config, indent=2), encoding="utf-8")
    return path


def _finalize_result(
    *,
    adapter: str,
    capture_mode: str,
    target_kib: int,
    batch_size: int,
    cycles: int,
    source_id: str,
    engine: AgentEngine,
    config_path: Path,
) -> dict[str, Any]:
    deliveries = engine.store.list_deliveries(source_id)
    proofs = engine.store.list_proofs(source_id)
    dlq = engine.store.list_dlq(source_id)
    if not proofs:
        raise RuntimeError(f"{adapter} budget run did not export any proof bundles")
    latest_proof = proofs[0]
    proof_payload = _proof_payload(latest_proof)
    return {
        "adapter": adapter,
        "status": "passed",
        "capture_mode": capture_mode,
        "target_kib": target_kib,
        "approx_batch_kib": _approx_batch_kib(batch_size),
        "approx_total_kib": _approx_batch_kib(batch_size) * cycles,
        "batch_size": batch_size,
        "cycles": cycles,
        "delivery_count": len(deliveries),
        "proof_count": len(proofs),
        "dlq_count": len(dlq),
        "request_id": latest_proof["request_id"],
        "proof_path": latest_proof["export_path"],
        "tx_id": proof_payload["receipt"]["tx_id"],
        "block_num": proof_payload["receipt"]["block_num"],
        "broadcast_backend": proof_payload.get("metadata", {}).get("broadcast_backend", "private_key_env"),
        "finality_mode": "finalized_exported",
        "config_path": str(config_path),
        "report": engine.report(source_id),
    }


def run_sqlite_budget(*, run_root: Path, target_kib: int, batch_size: int) -> dict[str, Any]:
    adapter_root = run_root / "sqlite"
    adapter_root.mkdir(parents=True, exist_ok=True)
    database_path = adapter_root / "ledger.sqlite3"
    state_db = adapter_root / "offchain-state.sqlite3"
    source_id = "sqlite-wave2-denotary-budget"
    cycles = _cycle_count_for_budget(target_kib, batch_size)
    DENOTARY_VALIDATION.initialize_sqlite(database_path)
    stack = DENOTARY_VALIDATION.OffchainStack(state_db)
    stack.start()
    try:
        config_path = _build_sqlite_config(
            adapter_root,
            database_path,
            stack,
            source_id=source_id,
            batch_size=batch_size,
        )
        engine = AgentEngine(load_config(config_path))
        try:
            engine.bootstrap(source_id)
            baseline = engine.run_once()
            if baseline["processed"] != 0 or baseline["failed"] != 0:
                raise RuntimeError(f"sqlite budget baseline was not idle: {baseline}")
            base_time = datetime.now(timezone.utc).replace(microsecond=0)
            base_id = int(base_time.timestamp())
            for cycle in range(cycles):
                cycle_time = base_time + timedelta(minutes=cycle * 5)
                for index in range(1, batch_size + 1):
                    record_id = base_id + cycle * 1000 + index
                    DENOTARY_VALIDATION.insert_sqlite_invoice(
                        database_path,
                        record_id=record_id,
                        status=f"budget-{cycle + 1}-{index}",
                        updated_at=(cycle_time + timedelta(seconds=index)).isoformat().replace("+00:00", "Z"),
                    )
                result = engine.run_once()
                if result["processed"] != batch_size or result["failed"] != 0:
                    raise RuntimeError(f"sqlite budget cycle {cycle + 1} produced unexpected result: {result}")
            payload = _finalize_result(
                adapter="sqlite",
                capture_mode="watermark",
                target_kib=target_kib,
                batch_size=batch_size,
                cycles=cycles,
                source_id=source_id,
                engine=engine,
                config_path=config_path,
            )
            if payload["proof_count"] != cycles or payload["delivery_count"] != cycles or payload["dlq_count"] != 0:
                raise RuntimeError(f"sqlite budget run exported unexpected counts: {payload}")
            return payload
        finally:
            engine.close()
    finally:
        stack.stop()


def run_redis_budget(*, run_root: Path, target_kib: int, batch_size: int) -> dict[str, Any]:
    adapter_root = run_root / "redis"
    adapter_root.mkdir(parents=True, exist_ok=True)
    state_db = adapter_root / "offchain-state.sqlite3"
    source_id = "redis-wave2-denotary-budget"
    cycles = _cycle_count_for_budget(target_kib, batch_size)
    DENOTARY_VALIDATION.run_redis_compose("down", "-v")
    DENOTARY_VALIDATION.run_redis_compose("up", "-d")
    DENOTARY_VALIDATION.wait_for_redis()
    DENOTARY_VALIDATION.flush_redis()
    stack = DENOTARY_VALIDATION.OffchainStack(state_db)
    stack.start()
    try:
        config_path = _build_redis_config(
            adapter_root,
            stack,
            source_id=source_id,
            batch_size=batch_size,
        )
        engine = AgentEngine(load_config(config_path))
        try:
            engine.bootstrap(source_id)
            baseline = engine.run_once()
            if baseline["processed"] != 0 or baseline["failed"] != 0:
                raise RuntimeError(f"redis budget baseline was not idle: {baseline}")
            run_nonce = utc_stamp().lower()
            for cycle in range(cycles):
                for index in range(1, batch_size + 1):
                    key = f"orders:{run_nonce}:{cycle + 1:02d}:{index:03d}"
                    if index % 2 == 0:
                        _write_redis_value(
                            key,
                            {
                                "status": f"paid-{cycle + 1}-{index}",
                                "updated_at": datetime.now(timezone.utc).isoformat(),
                                "sequence": str(index),
                            },
                        )
                    else:
                        _write_redis_value(key, f"issued-{cycle + 1}-{index}")
                result = engine.run_once()
                if result["processed"] != batch_size or result["failed"] != 0:
                    raise RuntimeError(f"redis budget cycle {cycle + 1} produced unexpected result: {result}")
            payload = _finalize_result(
                adapter="redis",
                capture_mode="scan",
                target_kib=target_kib,
                batch_size=batch_size,
                cycles=cycles,
                source_id=source_id,
                engine=engine,
                config_path=config_path,
            )
            if payload["proof_count"] != cycles or payload["delivery_count"] != cycles or payload["dlq_count"] != 0:
                raise RuntimeError(f"redis budget run exported unexpected counts: {payload}")
            return payload
        finally:
            engine.close()
    finally:
        stack.stop()
        DENOTARY_VALIDATION.run_redis_compose("down", "-v")


def main() -> None:
    args = parse_args()
    adapters = ["sqlite", "redis"] if args.adapter == "all" else [args.adapter]
    run_root = (
        Path(args.output_root).resolve()
        if args.output_root
        else (DATA_ROOT / f"wave2-mainnet-budget-{utc_stamp().lower()}").resolve()
    )
    run_root.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, Any]] = []
    for adapter in adapters:
        try:
            if adapter == "sqlite":
                results.append(run_sqlite_budget(run_root=run_root, target_kib=args.target_kib, batch_size=args.batch_size))
            else:
                results.append(run_redis_budget(run_root=run_root, target_kib=args.target_kib, batch_size=args.batch_size))
        except Exception as exc:  # noqa: BLE001
            results.append({"adapter": adapter, "status": "failed", "error": str(exc)})
    summary = {
        "network": "denotary",
        "target_kib_per_adapter": args.target_kib,
        "batch_size": args.batch_size,
        "approx_batch_kib": _approx_batch_kib(args.batch_size),
        "approx_cycles": _cycle_count_for_budget(args.target_kib, args.batch_size),
        "run_root": str(run_root),
        "results": results,
    }
    (run_root / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
