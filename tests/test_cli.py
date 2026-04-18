from __future__ import annotations

import json
import tempfile
import unittest
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from denotary_db_agent.cli import COMMAND_BEHAVIORS, COMMAND_KIND_HANDLERS, COMMAND_KIND_PARSER_BUILDERS, COMMAND_SPECS, EVIDENCE_COMMANDS, ENGINE_DISPATCH_COMMANDS, JSON_ENGINE_COMMANDS, OPTION_SPECS, SOURCE_ACTION_COMMANDS, build_command_result, build_parser, command_uses_engine, emit_command_result, evaluate_command_exit_policy, execute_command, main, maybe_export_snapshot
from denotary_db_agent.diagnostics_snapshots import (
    artifact_kind,
    build_snapshot_metadata,
    default_diagnostics_snapshot_path,
    export_snapshot_bundle,
    export_snapshot_artifact,
    prune_diagnostics_snapshots,
    write_json_snapshot,
)


class CliTest(unittest.TestCase):
    def test_maybe_export_snapshot_returns_payload_when_export_not_requested(self) -> None:
        payload = {"agent_name": "denotary-db-agent"}

        result = maybe_export_snapshot(
            payload,
            state_db="C:/runtime/state.sqlite3",
            source_id="pg-core-ledger",
            output_path=None,
            save_snapshot=False,
            retention=20,
            manifest_retention=200,
        )

        self.assertIs(result, payload)
        self.assertNotIn("snapshot_path", result)

    def test_evidence_commands_registry_declares_engine_methods(self) -> None:
        self.assertEqual(EVIDENCE_COMMANDS["doctor"]["engine_method"], "doctor")
        self.assertEqual(EVIDENCE_COMMANDS["report"]["engine_method"], "report")
        self.assertEqual(EVIDENCE_COMMANDS["diagnostics"]["engine_method"], "diagnostics")

    def test_command_behaviors_cover_evidence_output_and_strict_policy(self) -> None:
        self.assertTrue(COMMAND_BEHAVIORS["doctor"]["supports_snapshot_export"])
        self.assertTrue(COMMAND_BEHAVIORS["doctor"]["supports_output_path"])
        self.assertTrue(COMMAND_BEHAVIORS["doctor"]["supports_strict"])
        self.assertEqual(COMMAND_BEHAVIORS["doctor"]["strict_on_severity"], {"critical", "error"})
        self.assertEqual(COMMAND_BEHAVIORS["report"]["snapshot_prefix"], "report")
        self.assertEqual(COMMAND_BEHAVIORS["metrics"]["output_mode"], "json")

    def test_evaluate_command_exit_policy_uses_behavior_contract(self) -> None:
        args = type("Args", (), {"strict": True})()
        payload = {"overall": {"severity": "critical"}}

        self.assertEqual(evaluate_command_exit_policy("doctor", args, payload), 1)
        self.assertEqual(evaluate_command_exit_policy("report", args, payload), 0)

    def test_emit_command_result_uses_shared_output_contract(self) -> None:
        stdout = StringIO()
        result = build_command_result("metrics", {"ok": True}, exit_code=7)

        with patch("sys.stdout", stdout):
            exit_code = emit_command_result(result)

        self.assertEqual(exit_code, 7)
        self.assertEqual(json.loads(stdout.getvalue()), {"ok": True})

    def test_execute_command_uses_shared_engine_decision(self) -> None:
        args = type("Args", (), {"command": "artifacts", "prune_missing": False, "source": None, "kind": None, "latest": None})()
        config = type(
            "FakeConfig",
            (),
            {
                "storage": type("FakeStorage", (), {"state_db": "C:/runtime/state.sqlite3", "evidence_manifest_retention": 200})(),
            },
        )()

        with patch("denotary_db_agent.cli.run_artifacts_command", return_value=build_command_result("artifacts", {"ok": True})) as run_artifacts:
            result = execute_command(args, config)

        self.assertFalse(command_uses_engine("artifacts"))
        self.assertEqual(result["command_name"], "artifacts")
        run_artifacts.assert_called_once()

    def test_option_specs_cover_shared_cli_flags(self) -> None:
        self.assertEqual(OPTION_SPECS["source"]["flags"], ("--source",))
        self.assertEqual(OPTION_SPECS["request_id"]["flags"], ("--request-id",))
        self.assertEqual(COMMAND_SPECS["artifacts"]["kind"], "artifacts")

    def test_non_evidence_command_registries_declare_engine_methods(self) -> None:
        self.assertEqual(JSON_ENGINE_COMMANDS["validate"]["engine_method"], "validate")
        self.assertEqual(JSON_ENGINE_COMMANDS["refresh"]["engine_method"], "refresh_source")
        self.assertEqual(JSON_ENGINE_COMMANDS["metrics"]["help"], "Show compact export-friendly metrics")
        self.assertEqual(SOURCE_ACTION_COMMANDS["pause"]["engine_method"], "pause_source")
        self.assertEqual(SOURCE_ACTION_COMMANDS["replay"]["engine_method"], "reset_checkpoint")
        self.assertEqual(SOURCE_ACTION_COMMANDS["resume"]["help"], "Resume a paused source")
        self.assertEqual(ENGINE_DISPATCH_COMMANDS["run"]["kind"], "run")
        self.assertEqual(ENGINE_DISPATCH_COMMANDS["checkpoint"]["kind"], "checkpoint")
        self.assertEqual(ENGINE_DISPATCH_COMMANDS["proof"]["kind"], "proof")
        self.assertEqual(COMMAND_KIND_HANDLERS["proof"].__name__, "run_proof_command")
        self.assertEqual(COMMAND_KIND_PARSER_BUILDERS["artifacts"].__name__, "add_artifacts_parser")

    def test_evidence_parser_specs_are_built_from_registry(self) -> None:
        parser = build_parser()

        doctor_args = parser.parse_args(["--config", "cfg.json", "doctor", "--strict", "--save-snapshot"])
        self.assertTrue(doctor_args.strict)
        self.assertTrue(doctor_args.save_snapshot)

        report_args = parser.parse_args(["--config", "cfg.json", "report", "--save-snapshot"])
        self.assertFalse(hasattr(report_args, "strict"))
        self.assertTrue(report_args.save_snapshot)

        diagnostics_args = parser.parse_args(["--config", "cfg.json", "diagnostics", "--snapshot-retention", "7"])
        self.assertEqual(diagnostics_args.snapshot_retention, 7)

        pause_args = parser.parse_args(["--config", "cfg.json", "pause", "--source", "pg-core-ledger"])
        self.assertEqual(pause_args.source, "pg-core-ledger")

        metrics_args = parser.parse_args(["--config", "cfg.json", "metrics", "--source", "pg-core-ledger"])
        self.assertEqual(metrics_args.source, "pg-core-ledger")

        refresh_args = parser.parse_args(["--config", "cfg.json", "refresh", "--source", "pg-core-ledger"])
        self.assertEqual(refresh_args.source, "pg-core-ledger")

    def test_build_snapshot_metadata_populates_standard_fields(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            snapshot_path = Path(temp_dir) / "runtime" / "diagnostics" / "report-all-20260419T100000Z.json"
            snapshot_path.parent.mkdir(parents=True, exist_ok=True)
            snapshot_path.write_text("{}", encoding="utf-8")

            result = build_snapshot_metadata(
                snapshot_path=snapshot_path,
                removed_paths=[Path("a.json"), Path("b.json")],
                state_db=str(Path(temp_dir) / "runtime" / "state.sqlite3"),
            )

            self.assertEqual(result["snapshot_path"], str(snapshot_path))
            self.assertEqual(result["pruned_snapshot_paths"], ["a.json", "b.json"])
            self.assertEqual(Path(result["manifest_path"]).name, "evidence-manifest.json")

    def test_default_diagnostics_snapshot_path_uses_state_db_parent(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state_db = Path(temp_dir) / "runtime" / "state.sqlite3"
            path = default_diagnostics_snapshot_path(str(state_db), "pg-core-ledger")

        self.assertEqual(path.parent.name, "diagnostics")
        self.assertIn("pg-core-ledger", path.name)
        self.assertTrue(path.name.endswith(".json"))

    def test_write_json_snapshot_creates_parent_and_writes_payload(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output = Path(temp_dir) / "snapshots" / "diagnostics.json"
            payload = {"agent_name": "denotary-db-agent", "sources": [{"source_id": "pg-core-ledger"}]}

            path = write_json_snapshot(payload, output)

            self.assertEqual(path, output)
            self.assertTrue(path.exists())
            self.assertEqual(json.loads(path.read_text(encoding="utf-8")), payload)

    def test_prune_diagnostics_snapshots_keeps_newest_matching_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            snapshot_dir = Path(temp_dir)
            paths = [
                snapshot_dir / "diagnostics-pg-core-ledger-20260417T225200Z.json",
                snapshot_dir / "diagnostics-pg-core-ledger-20260417T225201Z.json",
                snapshot_dir / "diagnostics-pg-core-ledger-20260417T225202Z.json",
            ]
            for path in paths:
                path.write_text("{}", encoding="utf-8")

            removed = prune_diagnostics_snapshots(paths[-1], 2, "pg-core-ledger")

            self.assertEqual([item.name for item in removed], [paths[0].name])
            self.assertFalse(paths[0].exists())
            self.assertTrue(paths[1].exists())
            self.assertTrue(paths[2].exists())

    def test_prune_diagnostics_snapshots_rejects_non_positive_retention(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            snapshot_path = Path(temp_dir) / "diagnostics-pg-core-ledger-20260417T225200Z.json"
            snapshot_path.write_text("{}", encoding="utf-8")

            with self.assertRaises(ValueError):
                prune_diagnostics_snapshots(snapshot_path, 0, "pg-core-ledger")

    def test_export_snapshot_artifact_writes_file_and_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            payload = {
                "agent_name": "denotary-db-agent",
                "report_contract": {"artifact": "report", "version": 1, "source_report_version": 1},
            }
            state_db = Path(temp_dir) / "runtime" / "state.sqlite3"

            snapshot_path, removed = export_snapshot_artifact(
                payload,
                state_db=str(state_db),
                source_id="pg-core-ledger",
                prefix="report",
                retention=20,
                manifest_retention=200,
            )

            self.assertTrue(snapshot_path.exists())
            self.assertEqual(removed, [])
            manifest_path = Path(temp_dir) / "runtime" / "diagnostics" / "evidence-manifest.json"
            manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest_payload["artifacts"][-1]["kind"], "report")
            self.assertEqual(manifest_payload["artifacts"][-1]["contract_version"], 1)

    def test_export_snapshot_bundle_returns_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            payload = {
                "agent_name": "denotary-db-agent",
                "doctor_contract": {"artifact": "doctor", "version": 1, "source_entry_version": 1},
            }
            state_db = Path(temp_dir) / "runtime" / "state.sqlite3"

            metadata = export_snapshot_bundle(
                payload,
                state_db=str(state_db),
                source_id="pg-core-ledger",
                retention=20,
                manifest_retention=200,
            )

            self.assertTrue(Path(metadata["snapshot_path"]).exists())
            self.assertEqual(metadata["pruned_snapshot_paths"], [])
            self.assertTrue(Path(metadata["manifest_path"]).exists())

    def test_artifact_kind_reads_contract_artifact(self) -> None:
        payload = {
            "agent_name": "denotary-db-agent",
            "diagnostics_contract": {"artifact": "diagnostics", "version": 1, "source_entry_version": 1},
        }

        self.assertEqual(artifact_kind(payload), "diagnostics")

    def test_diagnostics_save_snapshot_writes_file_and_reports_path(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.json"
            config_path.write_text("{}", encoding="utf-8")
            fake_config = type(
                "FakeConfig",
                (),
                {
                    "storage": type("FakeStorage", (), {"state_db": str(Path(temp_dir) / "runtime" / "state.sqlite3")})(),
                },
            )()
            fake_engine = type(
                "FakeEngine",
                (),
                {
                    "diagnostics": lambda self, source: {
                        "agent_name": "denotary-db-agent",
                        "diagnostics_contract": {"artifact": "diagnostics", "version": 1, "source_entry_version": 1},
                        "sources": [{"source_id": source}],
                    },
                },
            )()

            stdout = StringIO()
            with patch("denotary_db_agent.cli.load_config", return_value=fake_config), patch(
                "denotary_db_agent.cli.AgentEngine",
                return_value=fake_engine,
            ), patch("sys.stdout", stdout):
                exit_code = main(
                    [
                        "--config",
                        str(config_path),
                        "diagnostics",
                        "--source",
                        "pg-core-ledger",
                        "--save-snapshot",
                    ]
                )

            self.assertEqual(exit_code, 0)
            payload = json.loads(stdout.getvalue())
            self.assertIn("snapshot_path", payload)
            snapshot_path = Path(payload["snapshot_path"])
            self.assertTrue(snapshot_path.exists())
            self.assertEqual(json.loads(snapshot_path.read_text(encoding="utf-8"))["agent_name"], "denotary-db-agent")
            self.assertEqual(payload["pruned_snapshot_paths"], [])
            self.assertIn("manifest_path", payload)
            manifest_payload = json.loads(Path(payload["manifest_path"]).read_text(encoding="utf-8"))
            self.assertEqual(manifest_payload["artifacts"][-1]["kind"], "diagnostics")
            self.assertEqual(manifest_payload["artifacts"][-1]["contract_version"], 1)

    def test_diagnostics_save_snapshot_prunes_older_matching_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.json"
            config_path.write_text("{}", encoding="utf-8")
            diagnostics_dir = Path(temp_dir) / "runtime" / "diagnostics"
            diagnostics_dir.mkdir(parents=True, exist_ok=True)
            old_paths = [
                diagnostics_dir / "diagnostics-pg-core-ledger-20260417T225200Z.json",
                diagnostics_dir / "diagnostics-pg-core-ledger-20260417T225201Z.json",
            ]
            for path in old_paths:
                path.write_text("{}", encoding="utf-8")
            fake_config = type(
                "FakeConfig",
                (),
                {
                    "storage": type("FakeStorage", (), {"state_db": str(Path(temp_dir) / "runtime" / "state.sqlite3")})(),
                },
            )()
            fake_engine = type(
                "FakeEngine",
                (),
                {
                    "diagnostics": lambda self, source: {"agent_name": "denotary-db-agent", "sources": [{"source_id": source}]},
                },
            )()

            stdout = StringIO()
            with patch("denotary_db_agent.cli.load_config", return_value=fake_config), patch(
                "denotary_db_agent.cli.AgentEngine",
                return_value=fake_engine,
            ), patch("sys.stdout", stdout):
                exit_code = main(
                    [
                        "--config",
                        str(config_path),
                        "diagnostics",
                        "--source",
                        "pg-core-ledger",
                        "--save-snapshot",
                        "--snapshot-retention",
                        "2",
                    ]
                )

            self.assertEqual(exit_code, 0)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(len(payload["pruned_snapshot_paths"]), 1)
            remaining = sorted(diagnostics_dir.glob("diagnostics-pg-core-ledger-*.json"))
            self.assertEqual(len(remaining), 2)

    def test_metrics_command_prints_engine_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.json"
            config_path.write_text("{}", encoding="utf-8")
            fake_config = type(
                "FakeConfig",
                (),
                {
                    "storage": type("FakeStorage", (), {"state_db": str(Path(temp_dir) / "runtime" / "state.sqlite3")})(),
                },
            )()
            fake_engine = type(
                "FakeEngine",
                (),
                {
                    "metrics": lambda self, source: {"agent_name": "denotary-db-agent", "totals": {"source_count": 1}, "sources": [{"source_id": source}]},
                },
            )()

            stdout = StringIO()
            with patch("denotary_db_agent.cli.load_config", return_value=fake_config), patch(
                "denotary_db_agent.cli.AgentEngine",
                return_value=fake_engine,
            ), patch("sys.stdout", stdout):
                exit_code = main(
                    [
                        "--config",
                        str(config_path),
                        "metrics",
                        "--source",
                        "pg-core-ledger",
                    ]
                )

            self.assertEqual(exit_code, 0)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["totals"]["source_count"], 1)
            self.assertEqual(payload["sources"][0]["source_id"], "pg-core-ledger")

    def test_checkpoint_reset_command_uses_dispatch_registry(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.json"
            config_path.write_text("{}", encoding="utf-8")
            fake_config = type(
                "FakeConfig",
                (),
                {
                    "storage": type("FakeStorage", (), {"state_db": str(Path(temp_dir) / "runtime" / "state.sqlite3")})(),
                },
            )()
            fake_engine = type(
                "FakeEngine",
                (),
                {
                    "reset_checkpoint": lambda self, source: None,
                },
            )()

            stdout = StringIO()
            with patch("denotary_db_agent.cli.load_config", return_value=fake_config), patch(
                "denotary_db_agent.cli.AgentEngine",
                return_value=fake_engine,
            ), patch("sys.stdout", stdout):
                exit_code = main(
                    [
                        "--config",
                        str(config_path),
                        "checkpoint",
                        "--source",
                        "pg-core-ledger",
                        "--reset",
                    ]
                )

            self.assertEqual(exit_code, 0)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["action"], "checkpoint_reset")
            self.assertEqual(payload["source"], "pg-core-ledger")

    def test_proof_command_uses_dispatch_registry(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.json"
            config_path.write_text("{}", encoding="utf-8")
            fake_config = type(
                "FakeConfig",
                (),
                {
                    "storage": type("FakeStorage", (), {"state_db": str(Path(temp_dir) / "runtime" / "state.sqlite3")})(),
                },
            )()
            fake_store = type(
                "FakeStore",
                (),
                {
                    "get_proof": lambda self, request_id: {"request_id": request_id, "status": "stored"},
                },
            )()
            fake_engine = type(
                "FakeEngine",
                (),
                {
                    "store": fake_store,
                },
            )()

            stdout = StringIO()
            with patch("denotary_db_agent.cli.load_config", return_value=fake_config), patch(
                "denotary_db_agent.cli.AgentEngine",
                return_value=fake_engine,
            ), patch("sys.stdout", stdout):
                exit_code = main(
                    [
                        "--config",
                        str(config_path),
                        "proof",
                        "--request-id",
                        "req-123",
                    ]
                )

            self.assertEqual(exit_code, 0)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["proof"]["request_id"], "req-123")

    def test_artifacts_command_reads_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_dir = Path(temp_dir) / "runtime" / "diagnostics"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            manifest_path = runtime_dir / "evidence-manifest.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "artifacts": [
                            {"kind": "doctor", "source_id": "pg-core-ledger", "path": "a.json"},
                            {"kind": "report", "source_id": "pg-core-ledger", "path": "b.json"},
                        ]
                    }
                ),
                encoding="utf-8",
            )
            config_path = Path(temp_dir) / "config.json"
            config_path.write_text("{}", encoding="utf-8")
            fake_config = type(
                "FakeConfig",
                (),
                {
                    "storage": type("FakeStorage", (), {"state_db": str(Path(temp_dir) / "runtime" / "state.sqlite3")})(),
                },
            )()
            stdout = StringIO()
            with patch("denotary_db_agent.cli.load_config", return_value=fake_config), patch("sys.stdout", stdout):
                exit_code = main(
                    [
                        "--config",
                        str(config_path),
                        "artifacts",
                    ]
                )

            self.assertEqual(exit_code, 0)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["artifact_count"], 2)
            self.assertEqual(Path(payload["manifest_path"]).name, "evidence-manifest.json")

    def test_artifacts_command_filters_by_kind_and_source(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_dir = Path(temp_dir) / "runtime" / "diagnostics"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            manifest_path = runtime_dir / "evidence-manifest.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "artifacts": [
                            {"kind": "doctor", "source_id": "pg-core-ledger", "path": "a.json"},
                            {"kind": "report", "source_id": "pg-core-ledger", "path": "b.json"},
                            {"kind": "report", "source_id": "pg-archive", "path": "c.json"},
                        ]
                    }
                ),
                encoding="utf-8",
            )
            config_path = Path(temp_dir) / "config.json"
            config_path.write_text("{}", encoding="utf-8")
            fake_config = type(
                "FakeConfig",
                (),
                {
                    "storage": type("FakeStorage", (), {"state_db": str(Path(temp_dir) / "runtime" / "state.sqlite3")})(),
                },
            )()
            stdout = StringIO()
            with patch("denotary_db_agent.cli.load_config", return_value=fake_config), patch("sys.stdout", stdout):
                exit_code = main(
                    [
                        "--config",
                        str(config_path),
                        "artifacts",
                        "--source",
                        "pg-core-ledger",
                        "--kind",
                        "report",
                    ]
                )

            self.assertEqual(exit_code, 0)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["artifact_count"], 1)
            self.assertEqual(payload["artifacts"][0]["kind"], "report")
            self.assertEqual(payload["artifacts"][0]["source_id"], "pg-core-ledger")

    def test_artifacts_command_latest_returns_newest_entries(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_dir = Path(temp_dir) / "runtime" / "diagnostics"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            manifest_path = runtime_dir / "evidence-manifest.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "artifacts": [
                            {
                                "kind": "doctor",
                                "source_id": "pg-core-ledger",
                                "path": "a.json",
                                "created_at": "2026-04-18T10:00:00Z",
                            },
                            {
                                "kind": "report",
                                "source_id": "pg-core-ledger",
                                "path": "b.json",
                                "created_at": "2026-04-18T10:05:00Z",
                            },
                            {
                                "kind": "diagnostics",
                                "source_id": "pg-core-ledger",
                                "path": "c.json",
                                "created_at": "2026-04-18T10:10:00Z",
                            },
                        ]
                    }
                ),
                encoding="utf-8",
            )
            config_path = Path(temp_dir) / "config.json"
            config_path.write_text("{}", encoding="utf-8")
            fake_config = type(
                "FakeConfig",
                (),
                {
                    "storage": type("FakeStorage", (), {"state_db": str(Path(temp_dir) / "runtime" / "state.sqlite3")})(),
                },
            )()
            stdout = StringIO()
            with patch("denotary_db_agent.cli.load_config", return_value=fake_config), patch("sys.stdout", stdout):
                exit_code = main(
                    [
                        "--config",
                        str(config_path),
                        "artifacts",
                        "--source",
                        "pg-core-ledger",
                        "--latest",
                        "2",
                    ]
                )

            self.assertEqual(exit_code, 0)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["artifact_count"], 2)
            self.assertEqual([item["path"] for item in payload["artifacts"]], ["c.json", "b.json"])

    def test_artifacts_command_prunes_missing_entries(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime_dir = Path(temp_dir) / "runtime" / "diagnostics"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            existing_path = runtime_dir / "doctor-pg-core-ledger-20260417T225200Z.json"
            existing_path.write_text("{}", encoding="utf-8")
            missing_path = runtime_dir / "doctor-pg-core-ledger-20260417T225201Z.json"
            manifest_path = runtime_dir / "evidence-manifest.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "artifacts": [
                            {"kind": "doctor", "source_id": "pg-core-ledger", "path": str(existing_path)},
                            {"kind": "doctor", "source_id": "pg-core-ledger", "path": str(missing_path)},
                        ]
                    }
                ),
                encoding="utf-8",
            )
            config_path = Path(temp_dir) / "config.json"
            config_path.write_text("{}", encoding="utf-8")
            fake_config = type(
                "FakeConfig",
                (),
                {
                    "storage": type("FakeStorage", (), {"state_db": str(Path(temp_dir) / "runtime" / "state.sqlite3")})(),
                },
            )()
            stdout = StringIO()
            with patch("denotary_db_agent.cli.load_config", return_value=fake_config), patch("sys.stdout", stdout):
                exit_code = main(
                    [
                        "--config",
                        str(config_path),
                        "artifacts",
                        "--prune-missing",
                    ]
                )

            self.assertEqual(exit_code, 0)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["pruned_missing_count"], 1)
            self.assertEqual(payload["pruned_missing_paths"], [str(missing_path)])
            self.assertEqual(payload["artifact_count"], 1)
            refreshed_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(len(refreshed_manifest["artifacts"]), 1)
            self.assertEqual(refreshed_manifest["artifacts"][0]["path"], str(existing_path))

    def test_report_command_prints_engine_report(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.json"
            config_path.write_text("{}", encoding="utf-8")
            fake_config = type(
                "FakeConfig",
                (),
                {
                    "storage": type("FakeStorage", (), {"state_db": str(Path(temp_dir) / "runtime" / "state.sqlite3")})(),
                },
            )()
            fake_engine = type(
                "FakeEngine",
                (),
                {
                    "report": lambda self, source: {
                        "agent_name": "denotary-db-agent",
                        "source_filter": source,
                        "report_contract": {"artifact": "report", "version": 1, "source_report_version": 1},
                        "doctor": {"overall": {"severity": "healthy"}},
                        "metrics": {"totals": {"source_count": 1}},
                        "source_reports": [],
                    },
                },
            )()

            stdout = StringIO()
            with patch("denotary_db_agent.cli.load_config", return_value=fake_config), patch(
                "denotary_db_agent.cli.AgentEngine",
                return_value=fake_engine,
            ), patch("sys.stdout", stdout):
                exit_code = main(
                    [
                        "--config",
                        str(config_path),
                        "report",
                        "--source",
                        "pg-core-ledger",
                    ]
                )

            self.assertEqual(exit_code, 0)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["source_filter"], "pg-core-ledger")
            self.assertEqual(payload["doctor"]["overall"]["severity"], "healthy")

    def test_report_save_snapshot_prunes_older_matching_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.json"
            config_path.write_text("{}", encoding="utf-8")
            diagnostics_dir = Path(temp_dir) / "runtime" / "diagnostics"
            diagnostics_dir.mkdir(parents=True, exist_ok=True)
            old_paths = [
                diagnostics_dir / "report-pg-core-ledger-20260417T225200Z.json",
                diagnostics_dir / "report-pg-core-ledger-20260417T225201Z.json",
            ]
            for path in old_paths:
                path.write_text("{}", encoding="utf-8")
            fake_config = type(
                "FakeConfig",
                (),
                {
                    "storage": type("FakeStorage", (), {"state_db": str(Path(temp_dir) / "runtime" / "state.sqlite3")})(),
                },
            )()
            fake_engine = type(
                "FakeEngine",
                (),
                {
                    "report": lambda self, source: {
                        "agent_name": "denotary-db-agent",
                        "source_filter": source,
                        "report_contract": {"artifact": "report", "version": 1, "source_report_version": 1},
                        "doctor": {"overall": {"severity": "healthy"}},
                        "metrics": {"totals": {"source_count": 1}},
                        "source_reports": [],
                    },
                },
            )()

            stdout = StringIO()
            with patch("denotary_db_agent.cli.load_config", return_value=fake_config), patch(
                "denotary_db_agent.cli.AgentEngine",
                return_value=fake_engine,
            ), patch("sys.stdout", stdout):
                exit_code = main(
                    [
                        "--config",
                        str(config_path),
                        "report",
                        "--source",
                        "pg-core-ledger",
                        "--save-snapshot",
                        "--snapshot-retention",
                        "2",
                    ]
                )

            self.assertEqual(exit_code, 0)
            payload = json.loads(stdout.getvalue())
            self.assertIn("snapshot_path", payload)
            self.assertIn("manifest_path", payload)
            self.assertEqual(len(payload["pruned_snapshot_paths"]), 1)
            remaining = sorted(diagnostics_dir.glob("report-pg-core-ledger-*.json"))
            self.assertEqual(len(remaining), 2)
            manifest_payload = json.loads(Path(payload["manifest_path"]).read_text(encoding="utf-8"))
            self.assertEqual(manifest_payload["artifacts"][-1]["kind"], "report")
            self.assertEqual(manifest_payload["artifacts"][-1]["contract_version"], 1)

    def test_doctor_command_prints_engine_doctor_report(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.json"
            config_path.write_text("{}", encoding="utf-8")
            fake_config = type(
                "FakeConfig",
                (),
                {
                    "storage": type("FakeStorage", (), {"state_db": str(Path(temp_dir) / "runtime" / "state.sqlite3")})(),
                },
            )()
            fake_engine = type(
                "FakeEngine",
                (),
                {
                    "doctor": lambda self, source: {
                        "agent_name": "denotary-db-agent",
                        "doctor_contract": {"artifact": "doctor", "version": 1, "source_entry_version": 1},
                        "overall": {"severity": "healthy", "ok": True},
                        "sources": [{"source_id": source}],
                    },
                },
            )()

            stdout = StringIO()
            with patch("denotary_db_agent.cli.load_config", return_value=fake_config), patch(
                "denotary_db_agent.cli.AgentEngine",
                return_value=fake_engine,
            ), patch("sys.stdout", stdout):
                exit_code = main(
                    [
                        "--config",
                        str(config_path),
                        "doctor",
                        "--source",
                        "pg-core-ledger",
                    ]
                )

            self.assertEqual(exit_code, 0)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["overall"]["severity"], "healthy")
            self.assertEqual(payload["sources"][0]["source_id"], "pg-core-ledger")

    def test_doctor_save_snapshot_writes_file_and_reports_path(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.json"
            config_path.write_text("{}", encoding="utf-8")
            fake_config = type(
                "FakeConfig",
                (),
                {
                    "storage": type("FakeStorage", (), {"state_db": str(Path(temp_dir) / "runtime" / "state.sqlite3")})(),
                },
            )()
            fake_engine = type(
                "FakeEngine",
                (),
                {
                    "doctor": lambda self, source: {
                        "agent_name": "denotary-db-agent",
                        "doctor_contract": {"artifact": "doctor", "version": 1, "source_entry_version": 1},
                        "overall": {"severity": "healthy", "ok": True},
                        "sources": [{"source_id": source}],
                    },
                },
            )()

            stdout = StringIO()
            with patch("denotary_db_agent.cli.load_config", return_value=fake_config), patch(
                "denotary_db_agent.cli.AgentEngine",
                return_value=fake_engine,
            ), patch("sys.stdout", stdout):
                exit_code = main(
                    [
                        "--config",
                        str(config_path),
                        "doctor",
                        "--source",
                        "pg-core-ledger",
                        "--save-snapshot",
                    ]
                )

            self.assertEqual(exit_code, 0)
            payload = json.loads(stdout.getvalue())
            self.assertIn("snapshot_path", payload)
            snapshot_path = Path(payload["snapshot_path"])
            self.assertTrue(snapshot_path.exists())
            self.assertTrue(snapshot_path.name.startswith("doctor-pg-core-ledger-"))
            self.assertEqual(payload["pruned_snapshot_paths"], [])
            self.assertIn("manifest_path", payload)
            manifest_payload = json.loads(Path(payload["manifest_path"]).read_text(encoding="utf-8"))
            self.assertEqual(manifest_payload["artifacts"][-1]["kind"], "doctor")
            self.assertEqual(manifest_payload["artifacts"][-1]["contract_version"], 1)

    def test_doctor_save_snapshot_prunes_older_matching_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.json"
            config_path.write_text("{}", encoding="utf-8")
            diagnostics_dir = Path(temp_dir) / "runtime" / "diagnostics"
            diagnostics_dir.mkdir(parents=True, exist_ok=True)
            old_paths = [
                diagnostics_dir / "doctor-pg-core-ledger-20260417T225200Z.json",
                diagnostics_dir / "doctor-pg-core-ledger-20260417T225201Z.json",
            ]
            for path in old_paths:
                path.write_text("{}", encoding="utf-8")
            fake_config = type(
                "FakeConfig",
                (),
                {
                    "storage": type("FakeStorage", (), {"state_db": str(Path(temp_dir) / "runtime" / "state.sqlite3")})(),
                },
            )()
            fake_engine = type(
                "FakeEngine",
                (),
                {
                    "doctor": lambda self, source: {
                        "agent_name": "denotary-db-agent",
                        "overall": {"severity": "healthy", "ok": True},
                        "sources": [{"source_id": source}],
                    },
                },
            )()

            stdout = StringIO()
            with patch("denotary_db_agent.cli.load_config", return_value=fake_config), patch(
                "denotary_db_agent.cli.AgentEngine",
                return_value=fake_engine,
            ), patch("sys.stdout", stdout):
                exit_code = main(
                    [
                        "--config",
                        str(config_path),
                        "doctor",
                        "--source",
                        "pg-core-ledger",
                        "--save-snapshot",
                        "--snapshot-retention",
                        "2",
                    ]
                )

            self.assertEqual(exit_code, 0)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(len(payload["pruned_snapshot_paths"]), 1)
            remaining = sorted(diagnostics_dir.glob("doctor-pg-core-ledger-*.json"))
            self.assertEqual(len(remaining), 2)

    def test_manifest_retention_keeps_only_newest_entries(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.json"
            config_path.write_text("{}", encoding="utf-8")
            fake_config = type(
                "FakeConfig",
                (),
                {
                    "storage": type(
                        "FakeStorage",
                        (),
                        {
                            "state_db": str(Path(temp_dir) / "runtime" / "state.sqlite3"),
                            "evidence_manifest_retention": 2,
                        },
                    )(),
                },
            )()
            fake_engine = type(
                "FakeEngine",
                (),
                {
                    "doctor": lambda self, source: {
                        "agent_name": "denotary-db-agent",
                        "overall": {"severity": "healthy", "ok": True},
                        "sources": [{"source_id": source}],
                    },
                },
            )()

            for index in range(3):
                stdout = StringIO()
                with patch("denotary_db_agent.cli.load_config", return_value=fake_config), patch(
                    "denotary_db_agent.cli.AgentEngine",
                    return_value=fake_engine,
                ), patch("sys.stdout", stdout):
                    exit_code = main(
                        [
                            "--config",
                            str(config_path),
                            "doctor",
                            "--source",
                            "pg-core-ledger",
                            "--save-snapshot",
                            "--snapshot-retention",
                            "10",
                        ]
                    )
                self.assertEqual(exit_code, 0)

            manifest_path = Path(temp_dir) / "runtime" / "diagnostics" / "evidence-manifest.json"
            manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(len(manifest_payload["artifacts"]), 2)

    def test_doctor_strict_returns_nonzero_for_critical_report(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.json"
            config_path.write_text("{}", encoding="utf-8")
            fake_config = type(
                "FakeConfig",
                (),
                {
                    "storage": type("FakeStorage", (), {"state_db": str(Path(temp_dir) / "runtime" / "state.sqlite3")})(),
                },
            )()
            fake_engine = type(
                "FakeEngine",
                (),
                {
                    "doctor": lambda self, source: {
                        "agent_name": "denotary-db-agent",
                        "overall": {"severity": "critical", "ok": False},
                        "sources": [{"source_id": source}],
                    },
                },
            )()

            stdout = StringIO()
            with patch("denotary_db_agent.cli.load_config", return_value=fake_config), patch(
                "denotary_db_agent.cli.AgentEngine",
                return_value=fake_engine,
            ), patch("sys.stdout", stdout):
                exit_code = main(
                    [
                        "--config",
                        str(config_path),
                        "doctor",
                        "--source",
                        "pg-core-ledger",
                        "--strict",
                    ]
                )

            self.assertEqual(exit_code, 1)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["overall"]["severity"], "critical")

    def test_doctor_strict_keeps_zero_for_degraded_report(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.json"
            config_path.write_text("{}", encoding="utf-8")
            fake_config = type(
                "FakeConfig",
                (),
                {
                    "storage": type("FakeStorage", (), {"state_db": str(Path(temp_dir) / "runtime" / "state.sqlite3")})(),
                },
            )()
            fake_engine = type(
                "FakeEngine",
                (),
                {
                    "doctor": lambda self, source: {
                        "agent_name": "denotary-db-agent",
                        "overall": {"severity": "degraded", "ok": False},
                        "sources": [{"source_id": source}],
                    },
                },
            )()

            stdout = StringIO()
            with patch("denotary_db_agent.cli.load_config", return_value=fake_config), patch(
                "denotary_db_agent.cli.AgentEngine",
                return_value=fake_engine,
            ), patch("sys.stdout", stdout):
                exit_code = main(
                    [
                        "--config",
                        str(config_path),
                        "doctor",
                        "--source",
                        "pg-core-ledger",
                        "--strict",
                    ]
                )

            self.assertEqual(exit_code, 0)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["overall"]["severity"], "degraded")
