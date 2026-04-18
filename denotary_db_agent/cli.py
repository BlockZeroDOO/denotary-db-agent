from __future__ import annotations

import argparse
import json

from denotary_db_agent.config import load_config
from denotary_db_agent.diagnostics_snapshots import (
    default_evidence_manifest_path,
    export_diagnostics_snapshot,
    export_named_snapshot,
    prune_missing_evidence_entries,
    read_evidence_manifest,
)
from denotary_db_agent.engine import AgentEngine


def attach_snapshot_metadata(
    payload: dict,
    *,
    snapshot_path,
    removed,
    state_db: str,
) -> dict:
    payload["snapshot_path"] = str(snapshot_path)
    payload["pruned_snapshot_paths"] = [str(item) for item in removed]
    payload["manifest_path"] = str(default_evidence_manifest_path(state_db))
    return payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="denotary-db-agent")
    parser.add_argument("--config", required=True, help="Path to agent config JSON")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run the agent")
    run_parser.add_argument("--once", action="store_true", help="Run one snapshot/backfill pass and exit")
    run_parser.add_argument("--interval-sec", type=float, default=5.0, help="Polling interval for continuous run mode")

    subparsers.add_parser("validate", help="Validate config and adapter connectivity")
    subparsers.add_parser("status", help="Show source status")
    subparsers.add_parser("health", help="Show service and source health")
    doctor_parser = subparsers.add_parser("doctor", help="Run a live preflight report for deploy readiness")
    doctor_parser.add_argument("--source", help="Source id")
    doctor_parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit with status 1 when doctor reports critical or error severity",
    )
    doctor_parser.add_argument("--output", help="Write doctor JSON to this file")
    doctor_parser.add_argument(
        "--save-snapshot",
        action="store_true",
        help="Save doctor snapshot to a timestamped JSON file under the local runtime directory",
    )
    doctor_parser.add_argument(
        "--snapshot-retention",
        type=int,
        default=20,
        help="When saving doctor snapshots, keep only the newest N matching files (default: 20)",
    )
    metrics_parser = subparsers.add_parser("metrics", help="Show compact export-friendly metrics")
    metrics_parser.add_argument("--source", help="Source id")
    artifacts_parser = subparsers.add_parser("artifacts", help="Show saved evidence artifacts from the local manifest")
    artifacts_parser.add_argument("--source", help="Source id")
    artifacts_parser.add_argument("--kind", choices=["diagnostics", "doctor", "report"], help="Artifact kind")
    artifacts_parser.add_argument(
        "--latest",
        type=int,
        help="Return only the newest N artifacts after filters are applied",
    )
    artifacts_parser.add_argument(
        "--prune-missing",
        action="store_true",
        help="Remove manifest entries whose snapshot files no longer exist",
    )
    report_parser = subparsers.add_parser("report", help="Export a compact rollout evidence bundle")
    report_parser.add_argument("--source", help="Source id")
    report_parser.add_argument("--output", help="Write report JSON to this file")
    report_parser.add_argument(
        "--save-snapshot",
        action="store_true",
        help="Save report snapshot to a timestamped JSON file under the local runtime directory",
    )
    report_parser.add_argument(
        "--snapshot-retention",
        type=int,
        default=20,
        help="When saving report snapshots, keep only the newest N matching files (default: 20)",
    )
    diagnostics_parser = subparsers.add_parser("diagnostics", help="Show compact stream/runtime diagnostics")
    diagnostics_parser.add_argument("--source", help="Source id")
    diagnostics_parser.add_argument("--output", help="Write diagnostics JSON to this file")
    diagnostics_parser.add_argument(
        "--save-snapshot",
        action="store_true",
        help="Save diagnostics snapshot to a timestamped JSON file under the local runtime directory",
    )
    diagnostics_parser.add_argument(
        "--snapshot-retention",
        type=int,
        default=20,
        help="When saving diagnostics snapshots, keep only the newest N matching files (default: 20)",
    )
    bootstrap_parser = subparsers.add_parser("bootstrap", help="Install or refresh source-side runtime artifacts")
    bootstrap_parser.add_argument("--source", help="Source id")
    inspect_parser = subparsers.add_parser("inspect", help="Inspect source configuration and live runtime state")
    inspect_parser.add_argument("--source", help="Source id")
    refresh_parser = subparsers.add_parser("refresh", help="Refresh source runtime artifacts and store runtime signature")
    refresh_parser.add_argument("--source", help="Source id")
    pause_parser = subparsers.add_parser("pause", help="Pause a source without editing config")
    pause_parser.add_argument("--source", required=True, help="Source id")
    resume_parser = subparsers.add_parser("resume", help="Resume a paused source")
    resume_parser.add_argument("--source", required=True, help="Source id")

    replay_parser = subparsers.add_parser("replay", help="Reset checkpoint for a source")
    replay_parser.add_argument("--source", required=True, help="Source id")

    checkpoint_parser = subparsers.add_parser("checkpoint", help="List or reset checkpoints")
    checkpoint_parser.add_argument("--source", help="Source id")
    checkpoint_parser.add_argument("--reset", action="store_true", help="Reset the source checkpoint")

    proof_parser = subparsers.add_parser("proof", help="Show stored proof bundle metadata")
    proof_parser.add_argument("--request-id", required=True, help="Request id")
    return parser

def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    config = load_config(args.config)
    manifest_retention = int(getattr(config.storage, "evidence_manifest_retention", 200))
    if args.command == "artifacts":
        pruned_missing: list[dict] = []
        if args.prune_missing:
            _, pruned_missing = prune_missing_evidence_entries(config.storage.state_db)
        manifest = read_evidence_manifest(config.storage.state_db)
        artifacts = manifest.get("artifacts", [])
        if args.source:
            artifacts = [item for item in artifacts if str(item.get("source_id") or "") == args.source]
        if args.kind:
            artifacts = [item for item in artifacts if str(item.get("kind") or "") == args.kind]
        artifacts = sorted(artifacts, key=lambda item: str(item.get("created_at") or ""), reverse=True)
        if args.latest is not None:
            if args.latest < 1:
                raise SystemExit("--latest must be at least 1")
            artifacts = artifacts[: args.latest]
        print(
            json.dumps(
                {
                    "manifest_path": str(default_evidence_manifest_path(config.storage.state_db)),
                    "pruned_missing_count": len(pruned_missing),
                    "pruned_missing_paths": [str(item.get("path") or "") for item in pruned_missing],
                    "artifact_count": len(artifacts),
                    "artifacts": artifacts,
                },
                indent=2,
            )
        )
        return 0
    engine = AgentEngine(config)

    if args.command == "validate":
        print(json.dumps({"ok": True, "sources": engine.validate()}, indent=2))
        return 0
    if args.command == "status":
        print(json.dumps(engine.status(), indent=2))
        return 0
    if args.command == "health":
        print(json.dumps(engine.health(), indent=2))
        return 0
    if args.command == "doctor":
        report = engine.doctor(args.source)
        if args.output:
            snapshot_path, removed = export_named_snapshot(
                report,
                state_db=config.storage.state_db,
                source_id=args.source,
                prefix="doctor",
                output_path=args.output,
                retention=args.snapshot_retention,
                manifest_retention=manifest_retention,
            )
            attach_snapshot_metadata(report, snapshot_path=snapshot_path, removed=removed, state_db=config.storage.state_db)
        elif args.save_snapshot:
            snapshot_path, removed = export_named_snapshot(
                report,
                state_db=config.storage.state_db,
                source_id=args.source,
                prefix="doctor",
                retention=args.snapshot_retention,
                manifest_retention=manifest_retention,
            )
            attach_snapshot_metadata(report, snapshot_path=snapshot_path, removed=removed, state_db=config.storage.state_db)
        print(json.dumps(report, indent=2))
        if args.strict and report.get("overall", {}).get("severity") in {"critical", "error"}:
            return 1
        return 0
    if args.command == "metrics":
        print(json.dumps(engine.metrics(args.source), indent=2))
        return 0
    if args.command == "report":
        report = engine.report(args.source)
        if args.output:
            snapshot_path, removed = export_named_snapshot(
                report,
                state_db=config.storage.state_db,
                source_id=args.source,
                prefix="report",
                output_path=args.output,
                retention=args.snapshot_retention,
                manifest_retention=manifest_retention,
            )
            attach_snapshot_metadata(report, snapshot_path=snapshot_path, removed=removed, state_db=config.storage.state_db)
        elif args.save_snapshot:
            snapshot_path, removed = export_named_snapshot(
                report,
                state_db=config.storage.state_db,
                source_id=args.source,
                prefix="report",
                retention=args.snapshot_retention,
                manifest_retention=manifest_retention,
            )
            attach_snapshot_metadata(report, snapshot_path=snapshot_path, removed=removed, state_db=config.storage.state_db)
        print(json.dumps(report, indent=2))
        return 0
    if args.command == "diagnostics":
        diagnostics = engine.diagnostics(args.source)
        if args.output:
            snapshot_path, removed = export_diagnostics_snapshot(
                diagnostics,
                state_db=config.storage.state_db,
                source_id=args.source,
                output_path=args.output,
                retention=args.snapshot_retention,
                manifest_retention=manifest_retention,
            )
            attach_snapshot_metadata(diagnostics, snapshot_path=snapshot_path, removed=removed, state_db=config.storage.state_db)
        elif args.save_snapshot:
            snapshot_path, removed = export_diagnostics_snapshot(
                diagnostics,
                state_db=config.storage.state_db,
                source_id=args.source,
                retention=args.snapshot_retention,
                manifest_retention=manifest_retention,
            )
            attach_snapshot_metadata(diagnostics, snapshot_path=snapshot_path, removed=removed, state_db=config.storage.state_db)
        print(json.dumps(diagnostics, indent=2))
        return 0
    if args.command == "bootstrap":
        print(json.dumps(engine.bootstrap(args.source), indent=2))
        return 0
    if args.command == "inspect":
        print(json.dumps(engine.inspect(args.source), indent=2))
        return 0
    if args.command == "refresh":
        print(json.dumps(engine.refresh_source(args.source), indent=2))
        return 0
    if args.command == "pause":
        engine.pause_source(args.source)
        print(json.dumps({"ok": True, "source": args.source, "action": "paused"}, indent=2))
        return 0
    if args.command == "resume":
        engine.resume_source(args.source)
        print(json.dumps({"ok": True, "source": args.source, "action": "resumed"}, indent=2))
        return 0
    if args.command == "run":
        if args.once:
            print(json.dumps(engine.run_once(), indent=2))
            return 0
        print(json.dumps({"status": "running", "interval_sec": args.interval_sec}, indent=2), flush=True)
        print(json.dumps(engine.run_forever(args.interval_sec), indent=2))
        return 0
    if args.command == "replay":
        engine.reset_checkpoint(args.source)
        print(json.dumps({"ok": True, "source": args.source, "action": "checkpoint_reset"}, indent=2))
        return 0
    if args.command == "checkpoint":
        if args.reset:
            if not args.source:
                raise SystemExit("--source is required with --reset")
            engine.reset_checkpoint(args.source)
            print(json.dumps({"ok": True, "source": args.source, "action": "checkpoint_reset"}, indent=2))
            return 0
        checkpoints = engine.checkpoint_summary()
        if args.source:
            checkpoints = [item for item in checkpoints if item["source_id"] == args.source]
        print(json.dumps({"checkpoints": checkpoints}, indent=2))
        return 0
    if args.command == "proof":
        proof = engine.store.get_proof(args.request_id)
        print(json.dumps({"proof": proof}, indent=2))
        return 0
    raise SystemExit("unsupported command")
