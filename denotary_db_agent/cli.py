from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from denotary_db_agent.config import load_config
from denotary_db_agent.engine import AgentEngine


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
    diagnostics_parser = subparsers.add_parser("diagnostics", help="Show compact stream/runtime diagnostics")
    diagnostics_parser.add_argument("--source", help="Source id")
    diagnostics_parser.add_argument("--output", help="Write diagnostics JSON to this file")
    diagnostics_parser.add_argument(
        "--save-snapshot",
        action="store_true",
        help="Save diagnostics snapshot to a timestamped JSON file under the local runtime directory",
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


def default_diagnostics_snapshot_path(config_path: str, state_db: str, source_id: str | None) -> Path:
    del config_path
    base_dir = Path(state_db).resolve().parent / "diagnostics"
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    suffix = source_id or "all"
    return base_dir / f"diagnostics-{suffix}-{timestamp}.json"


def write_json_snapshot(payload: dict, output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    config = load_config(args.config)
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
    if args.command == "diagnostics":
        diagnostics = engine.diagnostics(args.source)
        if args.output:
            snapshot_path = write_json_snapshot(diagnostics, args.output)
            diagnostics["snapshot_path"] = str(snapshot_path)
        elif args.save_snapshot:
            snapshot_path = write_json_snapshot(
                diagnostics,
                default_diagnostics_snapshot_path(args.config, config.storage.state_db, args.source),
            )
            diagnostics["snapshot_path"] = str(snapshot_path)
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
