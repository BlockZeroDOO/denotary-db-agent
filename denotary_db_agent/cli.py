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


COMMAND_SPECS = {
    "validate": {
        "kind": "json_engine",
        "engine_method": "validate",
        "wrap_key": "sources",
        "help": "Validate config and adapter connectivity",
    },
    "status": {
        "kind": "json_engine",
        "engine_method": "status",
        "help": "Show source status",
    },
    "health": {
        "kind": "json_engine",
        "engine_method": "health",
        "help": "Show service and source health",
    },
    "metrics": {
        "kind": "json_engine",
        "engine_method": "metrics",
        "source_arg": True,
        "help": "Show compact export-friendly metrics",
    },
    "bootstrap": {
        "kind": "json_engine",
        "engine_method": "bootstrap",
        "source_arg": True,
        "help": "Install or refresh source-side runtime artifacts",
    },
    "inspect": {
        "kind": "json_engine",
        "engine_method": "inspect",
        "source_arg": True,
        "help": "Inspect source configuration and live runtime state",
    },
    "refresh": {
        "kind": "json_engine",
        "engine_method": "refresh_source",
        "source_arg": True,
        "help": "Refresh source runtime artifacts and store runtime signature",
    },
    "pause": {
        "kind": "source_action",
        "engine_method": "pause_source",
        "action": "paused",
        "help": "Pause a source without editing config",
        "source_required": True,
    },
    "resume": {
        "kind": "source_action",
        "engine_method": "resume_source",
        "action": "resumed",
        "help": "Resume a paused source",
        "source_required": True,
    },
    "replay": {
        "kind": "source_action",
        "engine_method": "reset_checkpoint",
        "action": "checkpoint_reset",
        "help": "Reset checkpoint for a source",
        "source_required": True,
    },
    "doctor": {
        "kind": "evidence",
        "engine_method": "doctor",
        "help": "Run a live preflight report for deploy readiness",
        "behavior": {
            "supports_strict": True,
            "strict_on_severity": {"critical", "error"},
        },
    },
    "report": {
        "kind": "evidence",
        "engine_method": "report",
        "help": "Export a compact rollout evidence bundle",
    },
    "diagnostics": {
        "kind": "evidence",
        "engine_method": "diagnostics",
        "help": "Show compact stream/runtime diagnostics",
    },
    "run": {
        "kind": "run",
        "help": "Run the agent",
    },
    "checkpoint": {
        "kind": "checkpoint",
        "help": "List or reset checkpoints",
    },
    "proof": {
        "kind": "proof",
        "help": "Show stored proof bundle metadata",
    },
    "artifacts": {
        "kind": "artifacts",
        "help": "Show saved evidence artifacts from the local manifest",
    },
}

OPTION_SPECS = {
    "source": {
        "flags": ("--source",),
        "kwargs": {"help": "Source id"},
    },
    "output": {
        "flags": ("--output",),
        "kwargs": {"help": None},
    },
    "save_snapshot": {
        "flags": ("--save-snapshot",),
        "kwargs": {"action": "store_true", "help": None},
    },
    "snapshot_retention": {
        "flags": ("--snapshot-retention",),
        "kwargs": {"type": int, "default": 20, "help": None},
    },
    "strict": {
        "flags": ("--strict",),
        "kwargs": {"action": "store_true", "help": None},
    },
    "latest": {
        "flags": ("--latest",),
        "kwargs": {"type": int, "help": "Return only the newest N artifacts after filters are applied"},
    },
    "prune_missing": {
        "flags": ("--prune-missing",),
        "kwargs": {"action": "store_true", "help": "Remove manifest entries whose snapshot files no longer exist"},
    },
    "kind": {
        "flags": ("--kind",),
        "kwargs": {"choices": ["diagnostics", "doctor", "report"], "help": "Artifact kind"},
    },
    "once": {
        "flags": ("--once",),
        "kwargs": {"action": "store_true", "help": "Run one snapshot/backfill pass and exit"},
    },
    "interval_sec": {
        "flags": ("--interval-sec",),
        "kwargs": {"type": float, "default": 5.0, "help": "Polling interval for continuous run mode"},
    },
    "reset": {
        "flags": ("--reset",),
        "kwargs": {"action": "store_true", "help": "Reset the source checkpoint"},
    },
    "request_id": {
        "flags": ("--request-id",),
        "kwargs": {"required": True, "help": "Request id"},
    },
}


def build_command_behavior(command_name: str, command: dict) -> dict:
    behavior = {"output_mode": "json"}
    behavior.update(command.get("behavior", {}))
    if command["kind"] == "evidence":
        behavior.setdefault("supports_snapshot_export", True)
        behavior.setdefault("supports_output_path", True)
        behavior.setdefault("snapshot_prefix", command_name)
    return behavior

EVIDENCE_COMMANDS = {name: command for name, command in COMMAND_SPECS.items() if command["kind"] == "evidence"}
JSON_ENGINE_COMMANDS = {name: command for name, command in COMMAND_SPECS.items() if command["kind"] == "json_engine"}
SOURCE_ACTION_COMMANDS = {name: command for name, command in COMMAND_SPECS.items() if command["kind"] == "source_action"}
ENGINE_DISPATCH_COMMANDS = {
    name: {"kind": command["kind"]}
    for name, command in COMMAND_SPECS.items()
    if command["kind"] != "artifacts"
}
COMMAND_BEHAVIORS = {
    name: build_command_behavior(name, command)
    for name, command in COMMAND_SPECS.items()
}


def add_evidence_parser(subparsers, command_name: str, command: dict) -> None:
    parser = subparsers.add_parser(command_name, help=command["help"])
    behavior = COMMAND_BEHAVIORS[command_name]
    add_option(parser, "source")
    if behavior.get("supports_strict"):
        add_option(
            parser,
            "strict",
            help=f"Exit with status 1 when {command_name} reports critical or error severity",
        )
    if behavior.get("supports_output_path"):
        add_option(parser, "output", help=f"Write {command_name} JSON to this file")
    if behavior.get("supports_snapshot_export"):
        add_option(
            parser,
            "save_snapshot",
            help=f"Save {command_name} snapshot to a timestamped JSON file under the local runtime directory",
        )
        add_option(
            parser,
            "snapshot_retention",
            help=f"When saving {command_name} snapshots, keep only the newest N matching files (default: 20)",
        )


def add_option(parser, name: str, **overrides: object) -> None:
    option = OPTION_SPECS[name]
    kwargs = dict(option["kwargs"])
    kwargs.update(overrides)
    parser.add_argument(*option["flags"], **kwargs)


def add_json_engine_parser(subparsers, command_name: str, command: dict) -> None:
    parser = subparsers.add_parser(command_name, help=command["help"])
    if command.get("source_arg"):
        add_option(parser, "source")


def add_source_action_parser(subparsers, command_name: str, command: dict) -> None:
    parser = subparsers.add_parser(command_name, help=command["help"])
    add_option(parser, "source", required=bool(command.get("source_required")))


def add_run_parser(subparsers, command_name: str, command: dict) -> None:
    parser = subparsers.add_parser(command_name, help=command["help"])
    add_option(parser, "once")
    add_option(parser, "interval_sec")


def add_checkpoint_parser(subparsers, command_name: str, command: dict) -> None:
    parser = subparsers.add_parser(command_name, help=command["help"])
    add_option(parser, "source")
    add_option(parser, "reset")


def add_proof_parser(subparsers, command_name: str, command: dict) -> None:
    parser = subparsers.add_parser(command_name, help=command["help"])
    add_option(parser, "request_id")


def add_artifacts_parser(subparsers, command_name: str, command: dict) -> None:
    parser = subparsers.add_parser(command_name, help=command["help"])
    add_option(parser, "source")
    add_option(parser, "kind")
    add_option(parser, "latest")
    add_option(parser, "prune_missing")


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


def build_command_result(
    command_name: str,
    payload: dict,
    *,
    exit_code: int = 0,
    flush: bool = False,
) -> dict:
    return {
        "command_name": command_name,
        "payload": payload,
        "exit_code": exit_code,
        "flush": flush,
    }


def run_evidence_command(
    engine: AgentEngine,
    args,
    *,
    command_name: str,
    state_db: str,
    manifest_retention: int,
) -> dict:
    command = COMMAND_SPECS[command_name]
    behavior = COMMAND_BEHAVIORS[command_name]
    payload = getattr(engine, command["engine_method"])(args.source)
    maybe_export_snapshot(
        payload,
        state_db=state_db,
        source_id=args.source,
        prefix=behavior.get("snapshot_prefix"),
        output_path=getattr(args, "output", None) if behavior.get("supports_output_path") else None,
        save_snapshot=bool(getattr(args, "save_snapshot", False)) if behavior.get("supports_snapshot_export") else False,
        retention=int(getattr(args, "snapshot_retention", 20)),
        manifest_retention=manifest_retention,
    )
    exit_code = evaluate_command_exit_policy(command_name, args, payload)
    return build_command_result(command_name, payload, exit_code=exit_code)


def emit_command_output(command_name: str, payload: dict, *, flush: bool = False) -> int:
    behavior = COMMAND_BEHAVIORS[command_name]
    if behavior.get("output_mode") == "json":
        print(json.dumps(payload, indent=2), flush=flush)
        return 0
    raise ValueError(f"unsupported output mode for {command_name}: {behavior.get('output_mode')}")


def emit_command_result(result: dict) -> int:
    emit_command_output(result["command_name"], result["payload"], flush=bool(result.get("flush")))
    return int(result.get("exit_code", 0))


def evaluate_command_exit_policy(command_name: str, args, payload: dict) -> int:
    strict_on_severity = COMMAND_BEHAVIORS[command_name].get("strict_on_severity")
    if strict_on_severity and getattr(args, "strict", False):
        if payload.get("overall", {}).get("severity") in strict_on_severity:
            return 1
    return 0


def run_json_engine_command(engine: AgentEngine, args, *, command_name: str, **_: object) -> dict:
    command = COMMAND_SPECS[command_name]
    method = getattr(engine, command["engine_method"])
    payload = method(args.source) if command.get("source_arg") else method()
    if "wrap_key" in command:
        payload = {"ok": True, command["wrap_key"]: payload}
    return build_command_result(command_name, payload)


def run_source_action_command(engine: AgentEngine, args, *, command_name: str, **_: object) -> dict:
    command = COMMAND_SPECS[command_name]
    getattr(engine, command["engine_method"])(args.source)
    return build_command_result(command_name, {"ok": True, "source": args.source, "action": command["action"]})


def run_artifacts_command(args, *, state_db: str) -> dict:
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
    return build_command_result(
        "artifacts",
        {
            "manifest_path": str(default_evidence_manifest_path(state_db)),
            "pruned_missing_count": len(pruned_missing),
            "pruned_missing_paths": [str(item.get("path") or "") for item in pruned_missing],
            "artifact_count": len(artifacts),
            "artifacts": artifacts,
        }
    )


def run_run_command(engine: AgentEngine, args, **_: object) -> dict:
    if args.once:
        return build_command_result("run", engine.run_once())
    return build_command_result(
        "run",
        {
            "status": "running",
            "interval_sec": args.interval_sec,
            "run_result": engine.run_forever(args.interval_sec),
        },
    )


def run_checkpoint_command(engine: AgentEngine, args, **_: object) -> dict:
    if args.reset:
        if not args.source:
            raise SystemExit("--source is required with --reset")
        engine.reset_checkpoint(args.source)
        return build_command_result("checkpoint", {"ok": True, "source": args.source, "action": "checkpoint_reset"})
    checkpoints = engine.checkpoint_summary()
    if args.source:
        checkpoints = [item for item in checkpoints if item["source_id"] == args.source]
    return build_command_result("checkpoint", {"checkpoints": checkpoints})


def run_proof_command(engine: AgentEngine, args, **_: object) -> dict:
    proof = engine.store.get_proof(args.request_id)
    return build_command_result("proof", {"proof": proof})


COMMAND_KIND_HANDLERS = {
    "json_engine": run_json_engine_command,
    "evidence": run_evidence_command,
    "source_action": run_source_action_command,
    "run": run_run_command,
    "checkpoint": run_checkpoint_command,
    "proof": run_proof_command,
}


def dispatch_engine_command(
    engine: AgentEngine,
    args,
    *,
    state_db: str,
    manifest_retention: int,
) -> dict:
    command = COMMAND_SPECS[args.command]
    handler = COMMAND_KIND_HANDLERS[command["kind"]]
    return handler(
        engine,
        args,
        command_name=args.command,
        state_db=state_db,
        manifest_retention=manifest_retention,
    )


def command_uses_engine(command_name: str) -> bool:
    return COMMAND_SPECS[command_name]["kind"] != "artifacts"


def execute_command(args, config) -> dict:
    manifest_retention = int(getattr(config.storage, "evidence_manifest_retention", 200))
    if command_uses_engine(args.command):
        engine = AgentEngine(config)
        return dispatch_engine_command(
            engine,
            args,
            state_db=config.storage.state_db,
            manifest_retention=manifest_retention,
        )
    return run_artifacts_command(args, state_db=config.storage.state_db)


def add_command_parser(subparsers, command_name: str, command: dict) -> None:
    kind = command["kind"]
    if kind == "json_engine":
        add_json_engine_parser(subparsers, command_name, command)
        return
    if kind == "evidence":
        add_evidence_parser(subparsers, command_name, command)
        return
    if kind == "source_action":
        add_source_action_parser(subparsers, command_name, command)
        return
    if kind == "run":
        add_run_parser(subparsers, command_name, command)
        return
    if kind == "checkpoint":
        add_checkpoint_parser(subparsers, command_name, command)
        return
    if kind == "proof":
        add_proof_parser(subparsers, command_name, command)
        return
    if kind == "artifacts":
        add_artifacts_parser(subparsers, command_name, command)
        return
    raise ValueError(f"unsupported command kind: {kind}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="denotary-db-agent")
    parser.add_argument("--config", required=True, help="Path to agent config JSON")
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command_name, command in COMMAND_SPECS.items():
        add_command_parser(subparsers, command_name, command)
    return parser

def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    config = load_config(args.config)
    return emit_command_result(execute_command(args, config))
