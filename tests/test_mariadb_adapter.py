from __future__ import annotations

import unittest
from unittest.mock import patch

from denotary_db_agent.adapters.mariadb import MariaDbAdapter
from denotary_db_agent.config import SourceConfig
from denotary_db_agent.models import SourceCheckpoint


class FakeCursor:
    def __init__(self, results: list[list[dict[str, object]]]):
        self._results = results
        self.executed: list[tuple[str, tuple[object, ...] | None]] = []
        self._current: list[dict[str, object]] = []

    def execute(self, sql: str, params: tuple[object, ...] | list[object] | None = None) -> None:
        normalized = tuple(params) if isinstance(params, list) else params
        self.executed.append((sql, normalized))
        self._current = self._results.pop(0)

    def fetchone(self):
        return self._current[0] if self._current else None

    def fetchall(self):
        return list(self._current)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeConnection:
    def __init__(self, results: list[list[dict[str, object]]]):
        self.cursor_instance = FakeCursor(results)
        self.closed = False

    def cursor(self):
        return self.cursor_instance

    def close(self) -> None:
        self.closed = True


class MariaDbAdapterTest(unittest.TestCase):
    def setUp(self) -> None:
        self.config = SourceConfig(
            id="mariadb-core-ledger",
            adapter="mariadb",
            enabled=True,
            source_instance="erp-eu-1",
            database_name="ledger",
            backfill_mode="full",
            include={"ledger": ["invoices"]},
            connection={
                "host": "127.0.0.1",
                "port": 3306,
                "username": "denotary",
                "database": "ledger",
            },
            options={
                "capture_mode": "watermark",
                "watermark_column": "updated_at",
                "commit_timestamp_column": "updated_at",
                "row_limit": 100,
            },
        )

    def test_discover_capabilities_describes_watermark_baseline(self) -> None:
        adapter = MariaDbAdapter(self.config)
        capabilities = adapter.discover_capabilities()

        self.assertEqual(capabilities.source_type, "mariadb")
        self.assertEqual(capabilities.minimum_version, "10.6")
        self.assertTrue(capabilities.supports_cdc)
        self.assertTrue(capabilities.supports_snapshot)
        self.assertEqual(capabilities.operations, ("snapshot",))
        self.assertEqual(capabilities.capture_modes, ("watermark", "binlog"))
        self.assertEqual(capabilities.cdc_modes, ("binlog",))
        self.assertEqual(
            capabilities.bootstrap_requirements,
            ("tracked tables visible", "watermark columns configured"),
        )
        self.assertIn("watermark-based snapshot polling", capabilities.notes)

    def test_discover_capabilities_describes_binlog_mode(self) -> None:
        self.config.options["capture_mode"] = "binlog"
        adapter = MariaDbAdapter(self.config)
        capabilities = adapter.discover_capabilities()

        self.assertTrue(capabilities.supports_cdc)
        self.assertEqual(capabilities.operations, ("insert", "update", "delete"))
        self.assertEqual(capabilities.capture_modes, ("watermark", "binlog"))
        self.assertEqual(capabilities.cdc_modes, ("binlog",))
        self.assertEqual(
            capabilities.bootstrap_requirements,
            ("tracked tables visible", "row-based binlog enabled", "replication privileges configured"),
        )
        self.assertIn("binlog CDC baseline", capabilities.notes)

    def test_validate_connection_rejects_missing_fields(self) -> None:
        config = SourceConfig(
            id="mariadb-core-ledger",
            adapter="mariadb",
            enabled=True,
            source_instance="erp-eu-1",
            database_name="ledger",
            connection={},
        )
        adapter = MariaDbAdapter(config)

        with self.assertRaisesRegex(
            ValueError,
            "mariadb connection is missing required fields: host, port, username, database",
        ):
            adapter.validate_connection()

    def test_bootstrap_reports_tracked_tables(self) -> None:
        adapter = MariaDbAdapter(self.config)
        connection = FakeConnection(
            [
                [{"version": "10.11.8-MariaDB"}],
                [
                    {
                        "column_name": "id",
                        "data_type": "bigint",
                        "ordinal_position": 1,
                        "is_primary_key": 1,
                    },
                    {
                        "column_name": "status",
                        "data_type": "varchar",
                        "ordinal_position": 2,
                        "is_primary_key": 0,
                    },
                    {
                        "column_name": "updated_at",
                        "data_type": "datetime",
                        "ordinal_position": 3,
                        "is_primary_key": 0,
                    },
                ],
                [
                    {
                        "column_name": "id",
                        "data_type": "bigint",
                        "ordinal_position": 1,
                        "is_primary_key": 1,
                    },
                    {
                        "column_name": "status",
                        "data_type": "varchar",
                        "ordinal_position": 2,
                        "is_primary_key": 0,
                    },
                    {
                        "column_name": "updated_at",
                        "data_type": "datetime",
                        "ordinal_position": 3,
                        "is_primary_key": 0,
                    },
                ],
            ]
        )

        with patch("denotary_db_agent.adapters.mysql.pymysql", object()), patch.object(adapter, "_connect") as connect:
            connect.return_value.__enter__.return_value = connection
            connect.return_value.__exit__.return_value = False
            bootstrap = adapter.bootstrap()

        self.assertEqual(bootstrap["capture_mode"], "watermark")
        self.assertEqual(len(bootstrap["tracked_tables"]), 1)
        self.assertEqual(bootstrap["tracked_tables"][0]["table_name"], "invoices")

    def test_start_stream_raises_when_capture_mode_is_not_binlog(self) -> None:
        adapter = MariaDbAdapter(self.config)
        with self.assertRaisesRegex(NotImplementedError, "mariadb CDC streaming is only available when capture_mode is set to binlog"):
            list(adapter.start_stream(SourceCheckpoint(source_id=self.config.id, token="token-1", updated_at="2026-04-18T10:00:00Z")))
