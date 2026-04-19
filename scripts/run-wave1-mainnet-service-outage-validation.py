from __future__ import annotations

import argparse
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from denotary_db_agent.config import load_config
from denotary_db_agent.engine import AgentEngine


BAD_SERVICE_URL = "http://127.0.0.1:1"
SCENARIOS = {
    "ingress": "ingress_url",
    "watcher": "watcher_url",
    "receipt": "receipt_url",
    "audit": "audit_url",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Wave 1 mainnet service-outage validation.")
    parser.add_argument("--config", required=True, help="Path to a live denotary mainnet agent config.")
    parser.add_argument("--scenario", choices=sorted(SCENARIOS), help="Run only one outage scenario.")
    parser.add_argument("--bad-service-url", default=BAD_SERVICE_URL)
    return parser.parse_args()


def build_dry_run_event(*, scenario: str) -> dict[str, Any]:
    stamp = datetime.now(timezone.utc)
    token = stamp.strftime("%Y%m%d%H%M%S%f")
    return {
        "schema_or_namespace": "public",
        "table_or_collection": "service_outage_validation",
        "operation": "insert",
        "primary_key": {"id": token},
        "change_version": f"mainnet-outage:{scenario}:{token}",
        "checkpoint_token": f"mainnet-outage:{scenario}:{token}",
        "commit_timestamp": stamp.isoformat(),
        "after": {"id": token, "scenario": scenario, "status": "issued"},
        "before": None,
        "metadata": {"validation": "wave1-mainnet-service-outage", "scenario": scenario},
    }


def build_runtime_config(
    *,
    base_config: dict[str, Any],
    temp_dir: Path,
    scenario: str,
    broken_field: str | None,
    bad_service_url: str,
    suffix: str,
) -> Path:
    payload = json.loads(json.dumps(base_config))
    payload["agent_name"] = f"{payload.get('agent_name', 'denotary-db-agent')}-mainnet-outage-{scenario}"
    payload["storage"] = {
        "state_db": str(temp_dir / "agent-state.sqlite3"),
        "proof_dir": str(temp_dir / "proofs"),
    }
    if broken_field:
        payload["denotary"][broken_field] = bad_service_url

    payload["sources"] = [
        {
            "id": f"mainnet-outage-{scenario}",
            "adapter": "mysql",
            "enabled": True,
            "source_instance": "denotary-mainnet-service-outage",
            "database_name": "ledger",
            "include": {"ledger": ["invoices"]},
            "connection": {
                "host": "127.0.0.1",
                "port": 3306,
                "username": "denotary",
                "database": "ledger",
            },
            "backfill_mode": "none",
            "batch_enabled": False,
            "options": {
                "capture_mode": "watermark",
                "dry_run_events": [build_dry_run_event(scenario=scenario)],
            },
        }
    ]

    config_path = temp_dir / f"{scenario}-{suffix}.json"
    config_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return config_path


def run_scenario(*, base_config: dict[str, Any], scenario: str, bad_service_url: str) -> dict[str, Any]:
    broken_field = SCENARIOS[scenario]
    with tempfile.TemporaryDirectory() as temp:
        temp_dir = Path(temp)
        broken_config_path = build_runtime_config(
            base_config=base_config,
            temp_dir=temp_dir,
            scenario=scenario,
            broken_field=broken_field,
            bad_service_url=bad_service_url,
            suffix="broken",
        )
        healthy_config_path = build_runtime_config(
            base_config=base_config,
            temp_dir=temp_dir,
            scenario=scenario,
            broken_field=None,
            bad_service_url=bad_service_url,
            suffix="healthy",
        )

        broken_engine = AgentEngine(load_config(broken_config_path))
        try:
            first = broken_engine.run_once()
            source_id = broken_engine.config.sources[0].id
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

        finalized_delivery = next((item for item in deliveries if item.get("status") == "finalized_exported"), None)
        latest_proof = proofs[0] if proofs else None

        if int(first.get("processed") or 0) != 0 or int(first.get("failed") or 0) != 1:
            raise RuntimeError(f"{scenario} outage first run did not fail as expected: {first}")
        if int(second.get("processed") or 0) != 1 or int(second.get("failed") or 0) != 0:
            raise RuntimeError(f"{scenario} outage recovery run did not succeed as expected: {second}")
        if not finalized_delivery:
            raise RuntimeError(f"{scenario} outage did not produce a finalized_exported delivery")
        if not latest_proof:
            raise RuntimeError(f"{scenario} outage did not export a proof bundle")

        return {
            "scenario": scenario,
            "failed_component": broken_field,
            "first_run": first,
            "second_run": second,
            "first_delivery_count": len(first_deliveries),
            "first_proof_count": len(first_proofs),
            "first_dlq_count": len(first_dlq),
            "delivery_count": len(deliveries),
            "proof_count": len(proofs),
            "dlq_count": len(dlq),
            "request_id": latest_proof.get("request_id"),
            "proof_path": latest_proof.get("export_path"),
            "status": "passed",
        }


def main() -> None:
    args = parse_args()
    base_config = json.loads(Path(args.config).read_text(encoding="utf-8"))
    scenarios = [args.scenario] if args.scenario else list(SCENARIOS)
    results: list[dict[str, Any]] = []
    for scenario in scenarios:
        try:
            results.append(run_scenario(base_config=base_config, scenario=scenario, bad_service_url=args.bad_service_url))
        except Exception as exc:  # noqa: BLE001
            results.append({"scenario": scenario, "status": "failed", "error": str(exc)})
    print(json.dumps({"config": args.config, "results": results}, indent=2))


if __name__ == "__main__":
    main()
