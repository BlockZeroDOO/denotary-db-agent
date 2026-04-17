from __future__ import annotations

import json
import tempfile
import unittest
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from denotary_db_agent.cli import default_diagnostics_snapshot_path, main, write_json_snapshot


class CliTest(unittest.TestCase):
    def test_default_diagnostics_snapshot_path_uses_state_db_parent(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state_db = Path(temp_dir) / "runtime" / "state.sqlite3"
            path = default_diagnostics_snapshot_path("config.json", str(state_db), "pg-core-ledger")

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
