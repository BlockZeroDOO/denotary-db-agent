from __future__ import annotations

import unittest

from denotary_db_agent.adapters.scylladb import ScyllaDbAdapter
from denotary_db_agent.config import SourceConfig


def build_source_config() -> SourceConfig:
    return SourceConfig(
        id="scylladb-source",
        adapter="scylladb",
        enabled=True,
        source_instance="cluster-eu-1",
        database_name="ledger",
        include={"ledger": ["invoices"]},
        connection={"host": "127.0.0.1", "port": 9042},
        options={"capture_mode": "watermark"},
    )


class ScyllaDbAdapterTest(unittest.TestCase):
    def test_capabilities_expose_scylladb_identity(self) -> None:
        adapter = ScyllaDbAdapter(build_source_config())
        capabilities = adapter.discover_capabilities()

        self.assertEqual(capabilities.source_type, "scylladb")
        self.assertEqual(capabilities.default_capture_mode, "watermark")
        self.assertEqual(capabilities.cdc_modes, ())
        self.assertIn("Cassandra-compatible baseline", capabilities.notes)


if __name__ == "__main__":
    unittest.main()
