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

    def test_evidence_manifest_retention_rejects_zero(self) -> None:
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
                            "evidence_manifest_retention": 0,
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

            with self.assertRaisesRegex(ValueError, "evidence_manifest_retention must be at least 1"):
                load_config(config_path)

    def test_broadcast_backend_rejects_unknown_value(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.json"
            config_path.write_text(
                json.dumps(
                    {
                        "denotary": {
                            "ingress_url": "http://127.0.0.1:8080",
                            "watcher_url": "http://127.0.0.1:8081",
                            "submitter": "enterpriseac1",
                            "schema_id": 1,
                            "policy_id": 1,
                            "broadcast_backend": "magic_wallet",
                        },
                        "storage": {
                            "state_db": str(Path(temp_dir) / "state.sqlite3"),
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

            with self.assertRaisesRegex(ValueError, "broadcast_backend must be one of"):
                load_config(config_path)

    def test_wallet_command_rejects_non_string_entries(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.json"
            config_path.write_text(
                json.dumps(
                    {
                        "denotary": {
                            "ingress_url": "http://127.0.0.1:8080",
                            "watcher_url": "http://127.0.0.1:8081",
                            "submitter": "enterpriseac1",
                            "schema_id": 1,
                            "policy_id": 1,
                            "broadcast_backend": "cleos_wallet",
                            "wallet_command": ["wsl", 123],
                        },
                        "storage": {
                            "state_db": str(Path(temp_dir) / "state.sqlite3"),
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

            with self.assertRaisesRegex(ValueError, "wallet_command must be an array of non-empty strings"):
                load_config(config_path)

    def test_load_config_resolves_env_file_relative_to_config(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "config.json"
            env_path = Path(temp_dir) / "agent.env"
            env_path.write_text("DENOTARY_SUBMITTER_PRIVATE_KEY=test-wif\n", encoding="utf-8")
            config_path.write_text(
                json.dumps(
                    {
                        "denotary": {
                            "ingress_url": "http://127.0.0.1:8080",
                            "watcher_url": "http://127.0.0.1:8081",
                            "submitter": "enterpriseac1",
                            "schema_id": 1,
                            "policy_id": 1,
                            "broadcast_backend": "private_key_env",
                            "env_file": "agent.env",
                        },
                        "storage": {
                            "state_db": str(Path(temp_dir) / "state.sqlite3"),
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

            config = load_config(config_path)

        self.assertEqual(config.denotary.env_file, str(env_path.resolve()))
