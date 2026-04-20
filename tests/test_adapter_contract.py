from __future__ import annotations

import unittest

from denotary_db_agent.adapters.cassandra import CassandraAdapter
from denotary_db_agent.adapters.db2 import Db2Adapter
from denotary_db_agent.adapters.mariadb import MariaDbAdapter
from denotary_db_agent.adapters.mongodb import MongoDbAdapter
from denotary_db_agent.adapters.mysql import MySqlAdapter
from denotary_db_agent.adapters.oracle import OracleAdapter
from denotary_db_agent.adapters.postgres import PostgresAdapter
from denotary_db_agent.adapters.redis import RedisAdapter
from denotary_db_agent.adapters.registry import ADAPTERS, build_adapter
from denotary_db_agent.adapters.snowflake import SnowflakeAdapter
from denotary_db_agent.adapters.sqlserver import SqlServerAdapter
from denotary_db_agent.config import SourceConfig


class AdapterRegistryContractTest(unittest.TestCase):
    def setUp(self) -> None:
        self.cases = [
            ("postgresql", PostgresAdapter, {"host": "127.0.0.1", "port": 5432, "username": "denotary", "database": "ledger"}),
            ("mysql", MySqlAdapter, {"host": "127.0.0.1", "port": 3306, "username": "denotary", "database": "ledger"}),
            ("mariadb", MariaDbAdapter, {"host": "127.0.0.1", "port": 3306, "username": "denotary", "database": "ledger"}),
            ("sqlserver", SqlServerAdapter, {"host": "127.0.0.1", "port": 1433, "username": "sa", "database": "ledger"}),
            ("oracle", OracleAdapter, {"host": "127.0.0.1", "port": 1521, "username": "denotary", "service_name": "XEPDB1"}),
            ("mongodb", MongoDbAdapter, {"uri": "mongodb://127.0.0.1:27017"}),
            ("snowflake", SnowflakeAdapter, {"account": "acme-org.eu-central-1", "username": "denotary", "database": "ANALYTICS", "schema": "PUBLIC", "warehouse": "NOTARY_WH"}),
            ("redis", RedisAdapter, {"host": "127.0.0.1", "port": 6379}),
            ("db2", Db2Adapter, {"host": "127.0.0.1", "port": 50000, "username": "db2inst1", "password": "secret", "database": "LEDGER"}),
            ("cassandra", CassandraAdapter, {"host": "127.0.0.1", "port": 9042, "username": "cassandra", "password": "secret"}),
        ]

    def _source_config(self, adapter: str, connection: dict[str, object]) -> SourceConfig:
        default_capture_mode = "scan" if adapter == "redis" else "watermark"
        return SourceConfig(
            id=f"{adapter}-source",
            adapter=adapter,
            enabled=True,
            source_instance="test-instance",
            database_name="ledger",
            include={"ledger": ["invoices"]},
            connection=connection,
            options={"capture_mode": default_capture_mode},
        )

    def _source_config_with_mode(self, adapter: str, connection: dict[str, object], capture_mode: str) -> SourceConfig:
        return SourceConfig(
            id=f"{adapter}-source",
            adapter=adapter,
            enabled=True,
            source_instance="test-instance",
            database_name="ledger",
            include={"ledger": ["invoices"]},
            connection=connection,
            options={
                "capture_mode": capture_mode,
                "dry_run_events": [{"table_or_collection": "invoices", "after": {"id": 1}}],
            },
        )

    def test_registry_contains_all_wave_one_adapters(self) -> None:
        for adapter_name, adapter_class, connection in self.cases:
            with self.subTest(adapter=adapter_name):
                self.assertIn(adapter_name, ADAPTERS)
                adapter = build_adapter(self._source_config(adapter_name, connection))
                self.assertIsInstance(adapter, adapter_class)

    def test_adapters_declare_capture_defaults_and_cdc_modes(self) -> None:
        expected = {
            "postgresql": {
                "default_capture_mode": "watermark",
                "cdc_modes": ("trigger", "logical"),
                "default_checkpoint_strategy": "table_watermark",
                "default_activity_model": "polling",
            },
            "mysql": {
                "default_capture_mode": "watermark",
                "cdc_modes": ("binlog",),
                "default_checkpoint_strategy": "table_watermark",
                "default_activity_model": "polling",
            },
            "mariadb": {
                "default_capture_mode": "watermark",
                "cdc_modes": ("binlog",),
                "default_checkpoint_strategy": "table_watermark",
                "default_activity_model": "polling",
            },
            "sqlserver": {
                "default_capture_mode": "watermark",
                "cdc_modes": ("change_tracking",),
                "default_checkpoint_strategy": "table_watermark",
                "default_activity_model": "polling",
            },
            "oracle": {
                "default_capture_mode": "watermark",
                "cdc_modes": ("logminer",),
                "default_checkpoint_strategy": "table_watermark",
                "default_activity_model": "polling",
            },
            "mongodb": {
                "default_capture_mode": "watermark",
                "cdc_modes": ("change_streams",),
                "default_checkpoint_strategy": "document_watermark",
                "default_activity_model": "polling",
            },
            "snowflake": {
                "default_capture_mode": "watermark",
                "cdc_modes": (),
                "default_checkpoint_strategy": "table_watermark",
                "default_activity_model": "polling",
            },
            "redis": {
                "default_capture_mode": "scan",
                "cdc_modes": (),
                "default_checkpoint_strategy": "key_lexicographic",
                "default_activity_model": "polling",
            },
            "db2": {
                "default_capture_mode": "watermark",
                "cdc_modes": (),
                "default_checkpoint_strategy": "table_watermark",
                "default_activity_model": "polling",
            },
            "cassandra": {
                "default_capture_mode": "watermark",
                "cdc_modes": (),
                "default_checkpoint_strategy": "table_watermark",
                "default_activity_model": "polling",
            },
        }
        for adapter_name, _adapter_class, connection in self.cases:
            with self.subTest(adapter=adapter_name):
                adapter = build_adapter(self._source_config(adapter_name, connection))
                capabilities = adapter.discover_capabilities()
                self.assertEqual(capabilities.default_capture_mode, expected[adapter_name]["default_capture_mode"])
                self.assertEqual(capabilities.cdc_modes, expected[adapter_name]["cdc_modes"])
                self.assertEqual(capabilities.checkpoint_strategy, expected[adapter_name]["default_checkpoint_strategy"])
                self.assertEqual(capabilities.activity_model, expected[adapter_name]["default_activity_model"])
                self.assertEqual(adapter.capture_mode(), capabilities.default_capture_mode)
                self.assertEqual(adapter.is_cdc_mode(), adapter.capture_mode() in capabilities.cdc_modes)

    def test_cdc_modes_declare_checkpoint_and_activity_models(self) -> None:
        expected = {
            "postgresql": {
                "trigger": ("trigger_sequence", "notification_polling"),
                "logical": ("lsn_cursor", "stream"),
            },
            "mysql": {"binlog": ("binlog_cursor", "stream")},
            "mariadb": {"binlog": ("binlog_cursor", "stream")},
            "sqlserver": {"change_tracking": ("change_tracking_version", "polling")},
            "oracle": {"logminer": ("logminer_scn", "polling")},
            "mongodb": {"change_streams": ("resume_token", "stream")},
            "snowflake": {},
            "redis": {},
            "db2": {},
            "cassandra": {},
        }
        for adapter_name, _adapter_class, connection in self.cases:
            for capture_mode, expected_values in expected[adapter_name].items():
                with self.subTest(adapter=adapter_name, capture_mode=capture_mode):
                    adapter = build_adapter(self._source_config_with_mode(adapter_name, connection, capture_mode))
                    capabilities = adapter.discover_capabilities()
                    self.assertEqual(capabilities.checkpoint_strategy, expected_values[0])
                    self.assertEqual(capabilities.activity_model, expected_values[1])
                    summary = adapter.build_cdc_summary()
                    self.assertEqual(summary["checkpoint_strategy"], expected_values[0])
                    self.assertEqual(summary["activity_model"], expected_values[1])
                    self.assertTrue(summary["is_cdc_mode"])
                    self.assertEqual(summary["configured_capture_mode"], capture_mode)


if __name__ == "__main__":
    unittest.main()
