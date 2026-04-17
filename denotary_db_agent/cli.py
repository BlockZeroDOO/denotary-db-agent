from __future__ import annotations

import argparse
import json

from denotary_db_agent.config import load_config
from denotary_db_agent.engine import AgentEngine


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="denotary-db-agent")
    parser.add_argument("--config", required=True, help="Path to agent config JSON")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run the agent")
    run_parser.add_argument("--once", action="store_true", help="Run one snapshot/backfill pass and exit")

    subparsers.add_parser("validate", help="Validate config and adapter connectivity")
    subparsers.add_parser("status", help="Show source status")

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
    engine = AgentEngine(config)

    if args.command == "validate":
        print(json.dumps({"ok": True, "sources": engine.validate()}, indent=2))
        return 0
    if args.command == "status":
        print(json.dumps(engine.status(), indent=2))
        return 0
    if args.command == "run":
        if args.once:
            print(json.dumps(engine.run_once(), indent=2))
            return 0
        raise SystemExit("continuous run mode is not implemented yet; use --once in the scaffold")
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
