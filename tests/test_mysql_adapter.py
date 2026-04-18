from __future__ import annotations

import unittest
from unittest.mock import patch

from denotary_db_agent.adapters.mysql import MySqlAdapter
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


class MySqlAdapterTest(unittest.TestCase):
    def setUp(self) -> None:
        self.config = SourceConfig(
            id="mysql-core-ledger",
            adapter="mysql",
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
        adapter = MySqlAdapter(self.config)
        capabilities = adapter.discover_capabilities()

        self.assertEqual(capabilities.source_type, "mysql")
        self.assertEqual(capabilities.minimum_version, "8.0")
        self.assertFalse(capabilities.supports_cdc)
        self.assertTrue(capabilities.supports_snapshot)
        self.assertEqual(capabilities.operations, ("snapshot",))
        self.assertEqual(capabilities.capture_modes, ("watermark",))
        self.assertEqual(
            capabilities.bootstrap_requirements,
            ("tracked tables visible", "watermark columns configured"),
        )
        self.assertIn("watermark-based snapshot polling", capabilities.notes)

    def test_validate_connection_rejects_missing_fields(self) -> None:
        config = SourceConfig(
            id="mysql-core-ledger",
            adapter="mysql",
            enabled=True,
            source_instance="erp-eu-1",
            database_name="ledger",
            connection={},
        )
        adapter = MySqlAdapter(config)

        with self.assertRaisesRegex(ValueError, "mysql connection is missing required fields: host, port, username, database"):
            adapter.validate_connection()

    def test_bootstrap_and_inspect_report_tracked_tables(self) -> None:
        adapter = MySqlAdapter(self.config)
        connection = FakeConnection(
            [
                [{"version": "8.0.39"}],
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

        connection = FakeConnection(
            [
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
                ]
            ]
        )
        with patch.object(adapter, "_connect") as connect:
            connect.return_value.__enter__.return_value = connection
            connect.return_value.__exit__.return_value = False
            inspect = adapter.inspect()

        self.assertEqual(inspect["capture_modes"], ["watermark"])
        self.assertEqual(inspect["bootstrap_requirements"], ["tracked tables visible", "watermark columns configured"])
        self.assertEqual(inspect["tracked_tables"][0]["primary_key_columns"], ["id"])

    def test_read_snapshot_emits_rows_and_checkpoint_state(self) -> None:
        adapter = MySqlAdapter(self.config)
        spec_rows = [
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
        ]
        data_rows = [
            {
                "id": 1,
                "status": "issued",
                "updated_at": "2026-04-18T10:00:00Z",
            },
            {
                "id": 2,
                "status": "paid",
                "updated_at": "2026-04-18T10:05:00Z",
            },
        ]
        connection = FakeConnection([spec_rows, data_rows])

        with patch.object(adapter, "_connect") as connect:
            connect.return_value.__enter__.return_value = connection
            connect.return_value.__exit__.return_value = False
            events = list(adapter.read_snapshot())

        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].operation, "snapshot")
        self.assertEqual(events[0].primary_key, {"id": 1})
        self.assertEqual(events[0].after["status"], "issued")
        self.assertIn('"ledger.invoices"', events[-1].checkpoint_token)

    def test_read_snapshot_respects_checkpoint_resume(self) -> None:
        adapter = MySqlAdapter(self.config)
        checkpoint = SourceCheckpoint(
            source_id=self.config.id,
            token='{"ledger.invoices":{"watermark":"2026-04-18T10:00:00Z","pk":[1]}}',
            updated_at="2026-04-18T10:01:00Z",
        )
        spec_rows = [
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
        ]
        data_rows = [
            {
                "id": 2,
                "status": "paid",
                "updated_at": "2026-04-18T10:05:00Z",
            }
        ]
        connection = FakeConnection([spec_rows, data_rows])

        with patch.object(adapter, "_connect") as connect:
            connect.return_value.__enter__.return_value = connection
            connect.return_value.__exit__.return_value = False
            events = list(adapter.read_snapshot(checkpoint))

        self.assertEqual(len(events), 1)
        executed_sql, executed_params = connection.cursor_instance.executed[-1]
        self.assertIn("`updated_at` > %s", executed_sql)
        self.assertIsNotNone(executed_params)
        self.assertEqual(executed_params[0], "2026-04-18T10:00:00Z")

    def test_start_stream_raises_until_binlog_cdc_is_added(self) -> None:
        adapter = MySqlAdapter(self.config)
        with self.assertRaisesRegex(NotImplementedError, "mysql CDC streaming is not implemented yet"):
            list(adapter.start_stream(None))
