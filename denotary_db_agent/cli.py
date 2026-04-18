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

COMMAND_KIND_BEHAVIOR_DEFAULTS = {
    "default": {
        "output_mode": "json",
    },
    "evidence": {
        "supports_snapshot_export": True,
        "supports_output_path": True,
    },
}

COMMAND_KIND_COMMAND_DEFAULTS = {
    "default": {
        "behavior": {},
    },
    "json_engine": {
        "source_arg": False,
    },
    "source_action": {
        "source_required": False,
    },
}

COMMAND_KIND_OPTION_LAYOUTS = {
    "json_engine": (
        {"name": "source", "predicate": lambda command_name, command: bool(command.get("source_arg"))},
    ),
    "evidence": (
        {"name": "source"},
        {
            "name": "strict",
            "predicate": lambda command_name, command: bool(command.get("behavior", {}).get("supports_strict")),
            "help": lambda command_name: f"Exit with status 1 when {command_name} reports critical or error severity",
        },
        {
            "name": "output",
            "predicate": lambda command_name, command: bool(command.get("behavior", {}).get("supports_output_path")),
            "help": lambda command_name: f"Write {command_name} JSON to this file",
        },
        {
            "name": "save_snapshot",
            "predicate": lambda command_name, command: bool(command.get("behavior", {}).get("supports_snapshot_export")),
            "help": lambda command_name: f"Save {command_name} snapshot to a timestamped JSON file under the local runtime directory",
        },
        {
            "name": "snapshot_retention",
            "predicate": lambda command_name, command: bool(command.get("behavior", {}).get("supports_snapshot_export")),
            "help": lambda command_name: f"When saving {command_name} snapshots, keep only the newest N matching files (default: 20)",
        },
    ),
    "source_action": (
        {"name": "source", "required": lambda command_name, command: bool(command.get("source_required"))},
    ),
    "run": (
        {"name": "once"},
        {"name": "interval_sec"},
    ),
    "checkpoint": (
        {"name": "source"},
        {"name": "reset"},
    ),
    "proof": (
        {"name": "request_id"},
    ),
    "artifacts": (
        {"name": "source"},
        {"name": "kind"},
        {"name": "latest"},
        {"name": "prune_missing"},
    ),
}


def build_resolved_command_spec(command_name: str, command: dict) -> dict:
    resolved = dict(COMMAND_KIND_COMMAND_DEFAULTS["default"])
    resolved.update(COMMAND_KIND_COMMAND_DEFAULTS.get(command["kind"], {}))
    resolved.update(command)
    behavior = dict(COMMAND_KIND_BEHAVIOR_DEFAULTS["default"])
    behavior.update(COMMAND_KIND_BEHAVIOR_DEFAULTS.get(resolved["kind"], {}))
    behavior.update(resolved.get("behavior", {}))
    if resolved["kind"] == "evidence":
        behavior.setdefault("supports_snapshot_export", True)
        behavior.setdefault("supports_output_path", True)
        behavior.setdefault("snapshot_prefix", command_name)
    resolved["behavior"] = behavior
    return resolved


def select_commands(*, kind: str | None = None, exclude_kind: str | None = None) -> dict[str, dict]:
    selected: dict[str, dict] = {}
    for name, command in RESOLVED_COMMAND_SPECS.items():
        if kind is not None and command["kind"] != kind:
            continue
        if exclude_kind is not None and command["kind"] == exclude_kind:
            continue
        selected[name] = command
    return selected


def map_commands(command_names: dict[str, dict], mapper) -> dict[str, dict]:
    return {
        name: mapper(name, command)
        for name, command in command_names.items()
    }


def build_engine_dispatch_commands() -> dict[str, dict]:
    return {
        name: {"kind": command["kind"]}
        for name, command in select_commands(exclude_kind="artifacts").items()
    }


def build_kind_registry(*, kind: str, mapper=None) -> dict[str, dict]:
    selected = select_commands(kind=kind)
    if mapper is None:
        return selected
    return map_commands(selected, mapper)


RESOLVED_COMMAND_SPECS = map_commands(COMMAND_SPECS, build_resolved_command_spec)
COMMAND_GROUP_SPECS = {
    "evidence": {
        "selector": {"kind": "evidence"},
    },
    "json_engine": {
        "selector": {"kind": "json_engine"},
    },
    "source_action": {
        "selector": {"kind": "source_action"},
    },
    "engine_dispatch": {
        "selector": {"exclude_kind": "artifacts"},
        "mapper": lambda name, command: {"kind": command["kind"]},
    },
}


def build_command_group(group_name: str) -> dict[str, dict]:
    try:
        group = COMMAND_GROUP_SPECS[group_name]
    except KeyError as exc:
        raise KeyError(f"unknown command group: {group_name}") from exc
    selected = select_commands(**group["selector"])
    mapper = group.get("mapper")
    if mapper is None:
        return selected
    return map_commands(selected, mapper)


def build_command_group_builders() -> dict[str, object]:
    return {
        group_name: (lambda current_group_name=group_name: build_command_group(current_group_name))
        for group_name in COMMAND_GROUP_SPECS
    }


def build_command_groups() -> dict[str, dict[str, dict]]:
    return {
        group_name: build_command_group(group_name)
        for group_name in COMMAND_GROUP_SPECS
    }


COMMAND_GROUP_BUILDERS = build_command_group_builders()
COMMAND_GROUPS = build_command_groups()
EVIDENCE_COMMANDS = COMMAND_GROUPS["evidence"]
JSON_ENGINE_COMMANDS = COMMAND_GROUPS["json_engine"]
SOURCE_ACTION_COMMANDS = COMMAND_GROUPS["source_action"]
ENGINE_DISPATCH_COMMANDS = COMMAND_GROUPS["engine_dispatch"]


def get_command_spec(command_name: str) -> dict:
    return RESOLVED_COMMAND_SPECS[command_name]


def get_command_behavior(command_name: str) -> dict:
    return get_command_spec(command_name)["behavior"]


def get_command_group(group_name: str) -> dict[str, dict]:
    return build_command_group(group_name)


def add_option(parser, name: str, **overrides: object) -> None:
    option = OPTION_SPECS[name]
    kwargs = dict(option["kwargs"])
    kwargs.update(overrides)
    parser.add_argument(*option["flags"], **kwargs)


def add_kind_parser_options(parser, command_name: str, command: dict) -> None:
    for option in COMMAND_KIND_OPTION_LAYOUTS.get(command["kind"], ()):
        predicate = option.get("predicate")
        if predicate is not None and not predicate(command_name, command):
            continue
        overrides: dict[str, object] = {}
        if "required" in option:
            required = option["required"]
            overrides["required"] = required(command_name, command) if callable(required) else required
        if "help" in option:
            help_text = option["help"]
            overrides["help"] = help_text(command_name) if callable(help_text) else help_text
        add_option(parser, option["name"], **overrides)


def add_generic_parser(subparsers, command_name: str, command: dict) -> None:
    parser = subparsers.add_parser(command_name, help=command["help"])
    add_kind_parser_options(parser, command_name, command)


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
    command = get_command_spec(command_name)
    behavior = get_command_behavior(command_name)
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
    behavior = get_command_behavior(command_name)
    if behavior.get("output_mode") == "json":
        print(json.dumps(payload, indent=2), flush=flush)
        return 0
    raise ValueError(f"unsupported output mode for {command_name}: {behavior.get('output_mode')}")


def emit_command_result(result: dict) -> int:
    emit_command_output(result["command_name"], result["payload"], flush=bool(result.get("flush")))
    return int(result.get("exit_code", 0))


def evaluate_command_exit_policy(command_name: str, args, payload: dict) -> int:
    strict_on_severity = get_command_behavior(command_name).get("strict_on_severity")
    if strict_on_severity and getattr(args, "strict", False):
        if payload.get("overall", {}).get("severity") in strict_on_severity:
            return 1
    return 0


def run_json_engine_command(engine: AgentEngine, args, *, command_name: str, **_: object) -> dict:
    command = get_command_spec(command_name)
    method = getattr(engine, command["engine_method"])
    payload = method(args.source) if command.get("source_arg") else method()
    if "wrap_key" in command:
        payload = {"ok": True, command["wrap_key"]: payload}
    return build_command_result(command_name, payload)


def run_source_action_command(engine: AgentEngine, args, *, command_name: str, **_: object) -> dict:
    command = get_command_spec(command_name)
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


def build_kind_specs() -> dict[str, dict]:
    return {
        "json_engine": {
            "parser_builder": add_generic_parser,
            "handler": run_json_engine_command,
            "uses_engine": True,
        },
        "evidence": {
            "parser_builder": add_generic_parser,
            "handler": run_evidence_command,
            "uses_engine": True,
        },
        "source_action": {
            "parser_builder": add_generic_parser,
            "handler": run_source_action_command,
            "uses_engine": True,
        },
        "run": {
            "parser_builder": add_generic_parser,
            "handler": run_run_command,
            "uses_engine": True,
        },
        "checkpoint": {
            "parser_builder": add_generic_parser,
            "handler": run_checkpoint_command,
            "uses_engine": True,
        },
        "proof": {
            "parser_builder": add_generic_parser,
            "handler": run_proof_command,
            "uses_engine": True,
        },
        "artifacts": {
            "parser_builder": add_generic_parser,
            "uses_engine": False,
        },
    }


def build_kind_component_map(component: str, *, include_engineless: bool = True) -> dict[str, object]:
    mapped: dict[str, object] = {}
    for kind, spec in COMMAND_KIND_SPECS.items():
        if not include_engineless and not spec.get("uses_engine", False):
            continue
        if component in spec:
            mapped[kind] = spec[component]
    return mapped


COMMAND_KIND_SPECS = build_kind_specs()
COMMAND_KIND_PARSER_BUILDERS = build_kind_component_map("parser_builder")
COMMAND_KIND_HANDLERS = build_kind_component_map("handler", include_engineless=False)


def get_command_kind_spec(command_name: str) -> dict:
    return COMMAND_KIND_SPECS[get_command_spec(command_name)["kind"]]


def dispatch_engine_command(
    engine: AgentEngine,
    args,
    *,
    state_db: str,
    manifest_retention: int,
) -> dict:
    handler = get_command_kind_spec(args.command)["handler"]
    return handler(
        engine,
        args,
        command_name=args.command,
        state_db=state_db,
        manifest_retention=manifest_retention,
    )


def command_uses_engine(command_name: str) -> bool:
    return bool(get_command_kind_spec(command_name)["uses_engine"])


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
    try:
        builder = get_command_kind_spec(command_name)["parser_builder"]
    except KeyError as exc:
        raise ValueError(f"unsupported command kind: {kind}") from exc
    builder(subparsers, command_name, command)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="denotary-db-agent")
    parser.add_argument("--config", required=True, help="Path to agent config JSON")
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command_name in RESOLVED_COMMAND_SPECS:
        add_command_parser(subparsers, command_name, get_command_spec(command_name))
    return parser

def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    config = load_config(args.config)
    return emit_command_result(execute_command(args, config))
