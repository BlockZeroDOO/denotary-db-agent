from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from denotary_db_agent.config import load_config


class ConfigTest(unittest.TestCase):
    def test_storage_retention_rejects_negative_values(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.json"
            config_path.write_text(
                json.dumps(
                    {
                        "agent_name": "test-agent",
                        "log_level": "INFO",
                        "denotary": {
                            "ingress_url": "http://127.0.0.1:8080",
                            "watcher_url": "http://127.0.0.1:8081",
                            "watcher_auth_token": "",
                            "submitter": "enterpriseac1",
                            "schema_id": 1,
                            "policy_id": 1,
                        },
                        "storage": {
                            "state_db": str(Path(temp_dir) / "state.sqlite3"),
                            "proof_retention": -1,
                        },
                        "sources": [
                            {
                                "id": "pg-core-ledger",
                                "adapter": "postgresql",
                                "enabled": True,
                                "source_instance": "erp-eu-1",
                                "database_name": "ledger",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "proof_retention must be non-negative"):
                load_config(config_path)

    def test_diagnostics_snapshot_retention_rejects_zero(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.json"
            config_path.write_text(
                json.dumps(
                    {
                        "agent_name": "test-agent",
                        "log_level": "INFO",
                        "denotary": {
                            "ingress_url": "http://127.0.0.1:8080",
                            "watcher_url": "http://127.0.0.1:8081",
                            "watcher_auth_token": "",
                            "submitter": "enterpriseac1",
                            "schema_id": 1,
                            "policy_id": 1,
                        },
                        "storage": {
                            "state_db": str(Path(temp_dir) / "state.sqlite3"),
                            "diagnostics_snapshot_retention": 0,
                        },
                        "sources": [
                            {
                                "id": "pg-core-ledger",
                                "adapter": "postgresql",
                                "enabled": True,
                                "source_instance": "erp-eu-1",
                                "database_name": "ledger",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "diagnostics_snapshot_retention must be at least 1"):
                load_config(config_path)
