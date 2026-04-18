from __future__ import annotations

import unittest

from denotary_db_agent.adapters.mariadb import MariaDbAdapter
from denotary_db_agent.adapters.mongodb import MongoDbAdapter
from denotary_db_agent.adapters.mysql import MySqlAdapter
from denotary_db_agent.adapters.oracle import OracleAdapter
from denotary_db_agent.adapters.postgres import PostgresAdapter
from denotary_db_agent.adapters.registry import ADAPTERS, build_adapter
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
        ]

    def _source_config(self, adapter: str, connection: dict[str, object]) -> SourceConfig:
        return SourceConfig(
            id=f"{adapter}-source",
            adapter=adapter,
            enabled=True,
            source_instance="test-instance",
            database_name="ledger",
            include={"ledger": ["invoices"]},
            connection=connection,
            options={"capture_mode": "watermark"},
        )

    def test_registry_contains_all_wave_one_adapters(self) -> None:
        for adapter_name, adapter_class, connection in self.cases:
            with self.subTest(adapter=adapter_name):
                self.assertIn(adapter_name, ADAPTERS)
                adapter = build_adapter(self._source_config(adapter_name, connection))
                self.assertIsInstance(adapter, adapter_class)

    def test_adapters_declare_capture_defaults_and_cdc_modes(self) -> None:
        expected = {
            "postgresql": {"default_capture_mode": "watermark", "cdc_modes": ("trigger", "logical")},
            "mysql": {"default_capture_mode": "watermark", "cdc_modes": ("binlog",)},
            "mariadb": {"default_capture_mode": "watermark", "cdc_modes": ("binlog",)},
            "sqlserver": {"default_capture_mode": "watermark", "cdc_modes": ()},
            "oracle": {"default_capture_mode": "watermark", "cdc_modes": ()},
            "mongodb": {"default_capture_mode": "watermark", "cdc_modes": ("change_streams",)},
        }
        for adapter_name, _adapter_class, connection in self.cases:
            with self.subTest(adapter=adapter_name):
                adapter = build_adapter(self._source_config(adapter_name, connection))
                capabilities = adapter.discover_capabilities()
                self.assertEqual(capabilities.default_capture_mode, expected[adapter_name]["default_capture_mode"])
                self.assertEqual(capabilities.cdc_modes, expected[adapter_name]["cdc_modes"])
                self.assertEqual(adapter.capture_mode(), capabilities.default_capture_mode)
                self.assertEqual(adapter.is_cdc_mode(), adapter.capture_mode() in capabilities.cdc_modes)


if __name__ == "__main__":
    unittest.main()
