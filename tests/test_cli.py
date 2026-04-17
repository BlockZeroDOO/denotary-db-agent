from __future__ import annotations

import json
import tempfile
import unittest
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from denotary_db_agent.cli import main
from denotary_db_agent.diagnostics_snapshots import (
    default_diagnostics_snapshot_path,
    prune_diagnostics_snapshots,
    write_json_snapshot,
)


class CliTest(unittest.TestCase):
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
                    ]
                )

            self.assertEqual(exit_code, 0)
            payload = json.loads(stdout.getvalue())
            self.assertIn("snapshot_path", payload)
            snapshot_path = Path(payload["snapshot_path"])
            self.assertTrue(snapshot_path.exists())
            self.assertEqual(json.loads(snapshot_path.read_text(encoding="utf-8"))["agent_name"], "denotary-db-agent")
            self.assertEqual(payload["pruned_snapshot_paths"], [])

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
