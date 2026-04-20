from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from denotary_db_agent.config import load_config
from denotary_db_agent.engine import AgentEngine


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = PROJECT_ROOT / "data"
BAD_SERVICE_URL = "http://127.0.0.1:1"
SCENARIOS = {
    "ingress": "ingress_url",
    "watcher": "watcher_url",
    "receipt": "receipt_url",
    "audit": "audit_url",
}


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
    "wave2_denotary_validation_service_outage",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Wave 2 denotary mainnet service-outage validation.")
    parser.add_argument("--adapter", choices=("sqlite", "redis", "all"), default="all")
    parser.add_argument("--scenario", choices=sorted(SCENARIOS))
    parser.add_argument("--bad-service-url", default=BAD_SERVICE_URL)
    parser.add_argument("--output-root", default="", help="Optional persistent run directory.")
    return parser.parse_args()


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _load_proof_bundle(proof_entry: dict[str, Any]) -> dict[str, Any]:
    export_path = Path(str(proof_entry["export_path"]))
    return json.loads(export_path.read_text(encoding="utf-8"))


def _build_sqlite_config(
    adapter_root: Path,
    database_path: Path,
    stack: Any,
    *,
    source_id: str,
    broken_field: str | None,
    bad_service_url: str,
    suffix: str,
) -> Path:
    denotary = DENOTARY_VALIDATION.build_denotary_config_for_stack(stack)
    if broken_field:
        denotary[broken_field] = bad_service_url
    config = {
        "agent_name": f"wave2-sqlite-denotary-outage-{suffix}",
        "log_level": "INFO",
        "denotary": denotary,
        "storage": {
            "state_db": str((adapter_root / "state.sqlite3").resolve()),
            "proof_dir": str((adapter_root / "proofs").resolve()),
        },
        "sources": [
            {
                "id": source_id,
                "adapter": "sqlite",
                "enabled": True,
                "source_instance": "edge-device-denotary-outage",
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
    path = adapter_root / f"sqlite-{suffix}.json"
    path.write_text(json.dumps(config, indent=2), encoding="utf-8")
    return path


def _build_redis_config(
    adapter_root: Path,
    stack: Any,
    *,
    source_id: str,
    broken_field: str | None,
    bad_service_url: str,
    suffix: str,
) -> Path:
    denotary = DENOTARY_VALIDATION.build_denotary_config_for_stack(stack)
    if broken_field:
        denotary[broken_field] = bad_service_url
    config = {
        "agent_name": f"wave2-redis-denotary-outage-{suffix}",
        "log_level": "INFO",
        "denotary": denotary,
        "storage": {
            "state_db": str((adapter_root / "state.sqlite3").resolve()),
            "proof_dir": str((adapter_root / "proofs").resolve()),
        },
        "sources": [
            {
                "id": source_id,
                "adapter": "redis",
                "enabled": True,
                "source_instance": "cache-denotary-outage",
                "database_name": "db0",
                "include": {"0": ["orders:*"]},
                "checkpoint_policy": "after_ack",
                "backfill_mode": "full",
                "connection": {"host": "127.0.0.1", "port": DENOTARY_VALIDATION.REDIS_PORT},
                "options": {
                    "capture_mode": "scan",
                    "row_limit": 100,
                    "scan_count": 100,
                },
            }
        ],
    }
    path = adapter_root / f"redis-{suffix}.json"
    path.write_text(json.dumps(config, indent=2), encoding="utf-8")
    return path


def run_sqlite_scenario(
    *,
    adapter_root: Path,
    scenario: str,
    broken_field: str,
    bad_service_url: str,
) -> dict[str, Any]:
    adapter_root.mkdir(parents=True, exist_ok=True)
    database_path = adapter_root / "ledger.sqlite3"
    state_db = adapter_root / "offchain-state.sqlite3"
    source_id = "sqlite-wave2-denotary-outage"
    DENOTARY_VALIDATION.initialize_sqlite(database_path)
    stack = DENOTARY_VALIDATION.OffchainStack(state_db)
    stack.start()
    try:
        broken_config_path = _build_sqlite_config(
            adapter_root,
            database_path,
            stack,
            source_id=source_id,
            broken_field=broken_field,
            bad_service_url=bad_service_url,
            suffix=f"{scenario}-broken",
        )
        healthy_config_path = _build_sqlite_config(
            adapter_root,
            database_path,
            stack,
            source_id=source_id,
            broken_field=None,
            bad_service_url=bad_service_url,
            suffix=f"{scenario}-healthy",
        )
        marker = DENOTARY_VALIDATION.build_run_marker(f"sqlite-{scenario}")
        broken_engine = AgentEngine(load_config(broken_config_path))
        try:
            baseline = broken_engine.run_once()
            DENOTARY_VALIDATION.insert_sqlite_invoice(
                database_path,
                record_id=int(marker["record_id"]),
                status=str(marker["status"]),
                updated_at=str(marker["timestamp"]),
            )
            first = broken_engine.run_once()
            first_deliveries = broken_engine.store.list_deliveries(source_id)
            first_proofs = broken_engine.store.list_proofs(source_id)
            first_dlq = broken_engine.store.list_dlq(source_id)
        finally:
            broken_engine.close()

        healthy_engine = AgentEngine(load_config(healthy_config_path))
        try:
            second = healthy_engine.run_once()
            deliveries = healthy_engine.store.list_deliveries(source_id)
            proofs = healthy_engine.store.list_proofs(source_id)
            dlq = healthy_engine.store.list_dlq(source_id)
        finally:
            healthy_engine.close()

        if int(baseline.get("processed") or 0) != 0 or int(baseline.get("failed") or 0) != 0:
            raise RuntimeError(f"sqlite {scenario} baseline was not idle: {baseline}")
        if int(first.get("processed") or 0) != 0 or int(first.get("failed") or 0) != 1:
            raise RuntimeError(f"sqlite {scenario} first run did not fail as expected: {first}")
        if int(second.get("processed") or 0) != 1 or int(second.get("failed") or 0) != 0:
            raise RuntimeError(f"sqlite {scenario} recovery run did not succeed as expected: {second}")
        if not proofs:
            raise RuntimeError(f"sqlite {scenario} did not export a proof after recovery")
        latest_proof = proofs[0]
        proof_payload = _load_proof_bundle(latest_proof)
        return {
            "adapter": "sqlite",
            "scenario": scenario,
            "failed_component": broken_field,
            "status": "passed",
            "baseline_processed": baseline["processed"],
            "first_run": first,
            "second_run": second,
            "first_delivery_count": len(first_deliveries),
            "first_proof_count": len(first_proofs),
            "first_dlq_count": len(first_dlq),
            "delivery_count": len(deliveries),
            "proof_count": len(proofs),
            "dlq_count": len(dlq),
            "request_id": latest_proof["request_id"],
            "tx_id": proof_payload["receipt"]["tx_id"],
            "block_num": proof_payload["receipt"]["block_num"],
            "proof_path": latest_proof["export_path"],
            "broadcast_backend": "private_key_env",
            "finality_mode": "finalized_exported",
        }
    finally:
        stack.stop()


def run_redis_scenario(
    *,
    adapter_root: Path,
    scenario: str,
    broken_field: str,
    bad_service_url: str,
) -> dict[str, Any]:
    adapter_root.mkdir(parents=True, exist_ok=True)
    state_db = adapter_root / "offchain-state.sqlite3"
    source_id = "redis-wave2-denotary-outage"
    DENOTARY_VALIDATION.run_redis_compose("down", "-v")
    DENOTARY_VALIDATION.run_redis_compose("up", "-d")
    DENOTARY_VALIDATION.wait_for_redis()
    DENOTARY_VALIDATION.flush_redis()
    stack = DENOTARY_VALIDATION.OffchainStack(state_db)
    stack.start()
    try:
        broken_config_path = _build_redis_config(
            adapter_root,
            stack,
            source_id=source_id,
            broken_field=broken_field,
            bad_service_url=bad_service_url,
            suffix=f"{scenario}-broken",
        )
        healthy_config_path = _build_redis_config(
            adapter_root,
            stack,
            source_id=source_id,
            broken_field=None,
            bad_service_url=bad_service_url,
            suffix=f"{scenario}-healthy",
        )
        marker = DENOTARY_VALIDATION.build_run_marker("orders")
        broken_engine = AgentEngine(load_config(broken_config_path))
        try:
            baseline = broken_engine.run_once()
            DENOTARY_VALIDATION.write_redis_key(str(marker["key"]), f"{marker['value']}-{scenario}")
            first = broken_engine.run_once()
            first_deliveries = broken_engine.store.list_deliveries(source_id)
            first_proofs = broken_engine.store.list_proofs(source_id)
            first_dlq = broken_engine.store.list_dlq(source_id)
        finally:
            broken_engine.close()

        healthy_engine = AgentEngine(load_config(healthy_config_path))
        try:
            second = healthy_engine.run_once()
            deliveries = healthy_engine.store.list_deliveries(source_id)
            proofs = healthy_engine.store.list_proofs(source_id)
            dlq = healthy_engine.store.list_dlq(source_id)
        finally:
            healthy_engine.close()

        if int(baseline.get("processed") or 0) != 0 or int(baseline.get("failed") or 0) != 0:
            raise RuntimeError(f"redis {scenario} baseline was not idle: {baseline}")
        if int(first.get("processed") or 0) != 0 or int(first.get("failed") or 0) != 1:
            raise RuntimeError(f"redis {scenario} first run did not fail as expected: {first}")
        if int(second.get("processed") or 0) != 1 or int(second.get("failed") or 0) != 0:
            raise RuntimeError(f"redis {scenario} recovery run did not succeed as expected: {second}")
        if not proofs:
            raise RuntimeError(f"redis {scenario} did not export a proof after recovery")
        latest_proof = proofs[0]
        proof_payload = _load_proof_bundle(latest_proof)
        return {
            "adapter": "redis",
            "scenario": scenario,
            "failed_component": broken_field,
            "status": "passed",
            "baseline_processed": baseline["processed"],
            "first_run": first,
            "second_run": second,
            "first_delivery_count": len(first_deliveries),
            "first_proof_count": len(first_proofs),
            "first_dlq_count": len(first_dlq),
            "delivery_count": len(deliveries),
            "proof_count": len(proofs),
            "dlq_count": len(dlq),
            "request_id": latest_proof["request_id"],
            "tx_id": proof_payload["receipt"]["tx_id"],
            "block_num": proof_payload["receipt"]["block_num"],
            "proof_path": latest_proof["export_path"],
            "broadcast_backend": "private_key_env",
            "finality_mode": "finalized_exported",
        }
    finally:
        stack.stop()
        DENOTARY_VALIDATION.run_redis_compose("down", "-v")


def main() -> None:
    args = parse_args()
    adapters = ["sqlite", "redis"] if args.adapter == "all" else [args.adapter]
    scenarios = [args.scenario] if args.scenario else list(SCENARIOS)
    run_root = (
        Path(args.output_root).resolve()
        if args.output_root
        else (DATA_ROOT / f"wave2-mainnet-service-outage-{utc_stamp().lower()}").resolve()
    )
    run_root.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, Any]] = []
    for adapter in adapters:
        for scenario in scenarios:
            try:
                broken_field = SCENARIOS[scenario]
                adapter_root = run_root / adapter / scenario
                if adapter == "sqlite":
                    results.append(
                        run_sqlite_scenario(
                            adapter_root=adapter_root,
                            scenario=scenario,
                            broken_field=broken_field,
                            bad_service_url=args.bad_service_url,
                        )
                    )
                else:
                    results.append(
                        run_redis_scenario(
                            adapter_root=adapter_root,
                            scenario=scenario,
                            broken_field=broken_field,
                            bad_service_url=args.bad_service_url,
                        )
                    )
            except Exception as exc:  # noqa: BLE001
                results.append({"adapter": adapter, "scenario": scenario, "status": "failed", "error": str(exc)})
    summary = {
        "network": "denotary",
        "rpc_url": DENOTARY_VALIDATION.DENOTARY_RPC_URL,
        "submitter": DENOTARY_VALIDATION.SUBMITTER,
        "submitter_permission": DENOTARY_VALIDATION.SUBMITTER_PERMISSION,
        "run_root": str(run_root),
        "results": results,
    }
    (run_root / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
