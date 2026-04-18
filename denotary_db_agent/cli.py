from __future__ import annotations

import argparse
import json

from denotary_db_agent.config import load_config
from denotary_db_agent.diagnostics_snapshots import (
    default_evidence_manifest_path,
    export_snapshot_bundle,
    prune_missing_evidence_entries,
    read_evidence_manifest,
)
from denotary_db_agent.engine import AgentEngine


EVIDENCE_COMMANDS = {
    "doctor": {
        "engine_method": "doctor",
        "strict_on_severity": {"critical", "error"},
        "help": "Run a live preflight report for deploy readiness",
        "supports_strict": True,
    },
    "report": {
        "engine_method": "report",
        "help": "Export a compact rollout evidence bundle",
    },
    "diagnostics": {
        "engine_method": "diagnostics",
        "help": "Show compact stream/runtime diagnostics",
    },
}

JSON_ENGINE_COMMANDS = {
    "validate": {"engine_method": "validate", "wrap_key": "sources"},
    "status": {"engine_method": "status"},
    "health": {"engine_method": "health"},
    "metrics": {"engine_method": "metrics", "source_arg": True},
    "bootstrap": {"engine_method": "bootstrap", "source_arg": True},
    "inspect": {"engine_method": "inspect", "source_arg": True},
    "refresh": {"engine_method": "refresh_source", "source_arg": True},
}

SOURCE_ACTION_COMMANDS = {
    "pause": {"engine_method": "pause_source", "action": "paused"},
    "resume": {"engine_method": "resume_source", "action": "resumed"},
    "replay": {"engine_method": "reset_checkpoint", "action": "checkpoint_reset"},
}

ENGINE_DISPATCH_COMMANDS = {
    **{name: {"handler": "run_json_engine_command"} for name in JSON_ENGINE_COMMANDS},
    **{name: {"handler": "run_evidence_command"} for name in EVIDENCE_COMMANDS},
    **{name: {"handler": "run_source_action_command"} for name in SOURCE_ACTION_COMMANDS},
    "run": {"handler": "run_run_command"},
    "checkpoint": {"handler": "run_checkpoint_command"},
    "proof": {"handler": "run_proof_command"},
}


def add_evidence_parser(subparsers, command_name: str, command: dict) -> None:
    parser = subparsers.add_parser(command_name, help=command["help"])
    parser.add_argument("--source", help="Source id")
    if command.get("supports_strict"):
        parser.add_argument(
            "--strict",
            action="store_true",
            help=f"Exit with status 1 when {command_name} reports critical or error severity",
        )
    parser.add_argument("--output", help=f"Write {command_name} JSON to this file")
    parser.add_argument(
        "--save-snapshot",
        action="store_true",
        help=f"Save {command_name} snapshot to a timestamped JSON file under the local runtime directory",
    )
    parser.add_argument(
        "--snapshot-retention",
        type=int,
        default=20,
        help=f"When saving {command_name} snapshots, keep only the newest N matching files (default: 20)",
    )


def maybe_export_snapshot(
    payload: dict,
    *,
    state_db: str,
    source_id: str | None,
    prefix: str | None = None,
    output_path: str | None,
    save_snapshot: bool,
    retention: int,
    manifest_retention: int,
) -> dict:
    if not output_path and not save_snapshot:
        return payload
    metadata = export_snapshot_bundle(
        payload=payload,
        state_db=state_db,
        source_id=source_id,
        prefix=prefix,
        output_path=output_path if output_path else None,
        retention=retention,
        manifest_retention=manifest_retention,
    )
    payload.update(metadata)
    return payload


def run_evidence_command(
    engine: AgentEngine,
    args,
    *,
    command_name: str,
    state_db: str,
    manifest_retention: int,
) -> int:
    command = EVIDENCE_COMMANDS[command_name]
    payload = getattr(engine, command["engine_method"])(args.source)
    maybe_export_snapshot(
        payload,
        state_db=state_db,
        source_id=args.source,
        prefix=command_name,
        output_path=getattr(args, "output", None),
        save_snapshot=bool(getattr(args, "save_snapshot", False)),
        retention=int(getattr(args, "snapshot_retention", 20)),
        manifest_retention=manifest_retention,
    )
    print(json.dumps(payload, indent=2))
    strict_on_severity = command.get("strict_on_severity")
    if strict_on_severity and getattr(args, "strict", False):
        if payload.get("overall", {}).get("severity") in strict_on_severity:
            return 1
    return 0


def print_json(payload: dict) -> int:
    print(json.dumps(payload, indent=2))
    return 0


def run_json_engine_command(engine: AgentEngine, args, *, command_name: str, **_: object) -> int:
    command = JSON_ENGINE_COMMANDS[command_name]
    method = getattr(engine, command["engine_method"])
    payload = method(args.source) if command.get("source_arg") else method()
    if "wrap_key" in command:
        payload = {"ok": True, command["wrap_key"]: payload}
    return print_json(payload)


def run_source_action_command(engine: AgentEngine, args, *, command_name: str, **_: object) -> int:
    command = SOURCE_ACTION_COMMANDS[command_name]
    getattr(engine, command["engine_method"])(args.source)
    return print_json({"ok": True, "source": args.source, "action": command["action"]})


def run_artifacts_command(args, *, state_db: str) -> int:
    pruned_missing: list[dict] = []
    if args.prune_missing:
        _, pruned_missing = prune_missing_evidence_entries(state_db)
    manifest = read_evidence_manifest(state_db)
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
    return print_json(
        {
            "manifest_path": str(default_evidence_manifest_path(state_db)),
            "pruned_missing_count": len(pruned_missing),
            "pruned_missing_paths": [str(item.get("path") or "") for item in pruned_missing],
            "artifact_count": len(artifacts),
            "artifacts": artifacts,
        }
    )


def run_run_command(engine: AgentEngine, args, **_: object) -> int:
    if args.once:
        return print_json(engine.run_once())
    print(json.dumps({"status": "running", "interval_sec": args.interval_sec}, indent=2), flush=True)
    return print_json(engine.run_forever(args.interval_sec))


def run_checkpoint_command(engine: AgentEngine, args, **_: object) -> int:
    if args.reset:
        if not args.source:
            raise SystemExit("--source is required with --reset")
        engine.reset_checkpoint(args.source)
        return print_json({"ok": True, "source": args.source, "action": "checkpoint_reset"})
    checkpoints = engine.checkpoint_summary()
    if args.source:
        checkpoints = [item for item in checkpoints if item["source_id"] == args.source]
    return print_json({"checkpoints": checkpoints})


def run_proof_command(engine: AgentEngine, args, **_: object) -> int:
    proof = engine.store.get_proof(args.request_id)
    return print_json({"proof": proof})


def dispatch_engine_command(
    engine: AgentEngine,
    args,
    *,
    state_db: str,
    manifest_retention: int,
) -> int:
    command = ENGINE_DISPATCH_COMMANDS[args.command]
    handler = globals()[command["handler"]]
    return handler(
        engine,
        args,
        command_name=args.command,
        state_db=state_db,
        manifest_retention=manifest_retention,
    )


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
    for command_name, command in EVIDENCE_COMMANDS.items():
        add_evidence_parser(subparsers, command_name, command)
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
        return run_artifacts_command(args, state_db=config.storage.state_db)
    engine = AgentEngine(config)
    if args.command in ENGINE_DISPATCH_COMMANDS:
        return dispatch_engine_command(
            engine,
            args,
            state_db=config.storage.state_db,
            manifest_retention=manifest_retention,
        )
    raise SystemExit("unsupported command")
