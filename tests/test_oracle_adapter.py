from __future__ import annotations

import unittest
from unittest.mock import patch

from denotary_db_agent.adapters.oracle import OracleAdapter
from denotary_db_agent.config import SourceConfig
from denotary_db_agent.models import SourceCheckpoint


class FakeCursor:
    def __init__(self, results: list[list[dict[str, object] | tuple[object, ...]]], description: list[tuple[str]] | None = None):
        self._results = results
        self.executed: list[tuple[str, dict[str, object] | None]] = []
        self._current: list[dict[str, object] | tuple[object, ...]] = []
        self.description = description or []

    def execute(self, sql: str, params: dict[str, object] | None = None, **kwargs: object) -> None:
        normalized = dict(params or {})
        if kwargs:
            normalized.update(kwargs)
        self.executed.append((sql, normalized or None))
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
    def __init__(self, results: list[list[dict[str, object] | tuple[object, ...]]], description: list[tuple[str]] | None = None):
        self.cursor_instance = FakeCursor(results, description=description)
        self.closed = False

    def cursor(self):
        return self.cursor_instance

    def close(self) -> None:
        self.closed = True


class OracleAdapterTest(unittest.TestCase):
    def setUp(self) -> None:
        self.config = SourceConfig(
            id="oracle-core-ledger",
            adapter="oracle",
            enabled=True,
            source_instance="erp-eu-1",
            database_name="ledger",
            backfill_mode="full",
            include={"LEDGER": ["INVOICES"]},
            connection={
                "host": "127.0.0.1",
                "port": 1521,
                "username": "denotary",
                "service_name": "XEPDB1",
            },
            options={
                "capture_mode": "watermark",
                "watermark_column": "updated_at",
                "commit_timestamp_column": "updated_at",
                "row_limit": 100,
            },
        )

    def test_discover_capabilities_describes_watermark_baseline(self) -> None:
        adapter = OracleAdapter(self.config)
        capabilities = adapter.discover_capabilities()

        self.assertEqual(capabilities.source_type, "oracle")
        self.assertEqual(capabilities.minimum_version, "19c")
        self.assertFalse(capabilities.supports_cdc)
        self.assertTrue(capabilities.supports_snapshot)
        self.assertEqual(capabilities.operations, ("snapshot",))
        self.assertEqual(capabilities.capture_modes, ("watermark",))
        self.assertEqual(
            capabilities.bootstrap_requirements,
            ("tracked tables visible", "watermark columns configured"),
        )
        self.assertIn("LogMiner", capabilities.notes)

    def test_validate_connection_rejects_missing_fields(self) -> None:
        config = SourceConfig(
            id="oracle-core-ledger",
            adapter="oracle",
            enabled=True,
            source_instance="erp-eu-1",
            database_name="ledger",
            connection={},
        )
        adapter = OracleAdapter(config)

        with self.assertRaisesRegex(
            ValueError,
            "oracle connection is missing required fields: host, port, username, service_name",
        ):
            adapter.validate_connection()

    def test_bootstrap_and_inspect_report_tracked_tables(self) -> None:
        adapter = OracleAdapter(self.config)
        connection = FakeConnection(
            [
                [{"OK": 1}],
                [
                    {
                        "COLUMN_NAME": "ID",
                        "DATA_TYPE": "NUMBER",
                        "ORDINAL_POSITION": 1,
                        "IS_PRIMARY_KEY": 1,
                    },
                    {
                        "COLUMN_NAME": "STATUS",
                        "DATA_TYPE": "VARCHAR2",
                        "ORDINAL_POSITION": 2,
                        "IS_PRIMARY_KEY": 0,
                    },
                    {
                        "COLUMN_NAME": "UPDATED_AT",
                        "DATA_TYPE": "TIMESTAMP",
                        "ORDINAL_POSITION": 3,
                        "IS_PRIMARY_KEY": 0,
                    },
                ],
                [
                    {
                        "COLUMN_NAME": "ID",
                        "DATA_TYPE": "NUMBER",
                        "ORDINAL_POSITION": 1,
                        "IS_PRIMARY_KEY": 1,
                    },
                    {
                        "COLUMN_NAME": "STATUS",
                        "DATA_TYPE": "VARCHAR2",
                        "ORDINAL_POSITION": 2,
                        "IS_PRIMARY_KEY": 0,
                    },
                    {
                        "COLUMN_NAME": "UPDATED_AT",
                        "DATA_TYPE": "TIMESTAMP",
                        "ORDINAL_POSITION": 3,
                        "IS_PRIMARY_KEY": 0,
                    },
                ],
            ]
        )

        with patch("denotary_db_agent.adapters.oracle.oracledb", object()), patch.object(adapter, "_connect") as connect:
            connect.return_value.__enter__.return_value = connection
            connect.return_value.__exit__.return_value = False
            bootstrap = adapter.bootstrap()

        self.assertEqual(bootstrap["capture_mode"], "watermark")
        self.assertEqual(len(bootstrap["tracked_tables"]), 1)
        self.assertEqual(bootstrap["tracked_tables"][0]["table_name"], "INVOICES")

        connection = FakeConnection(
            [
                [
                    {
                        "COLUMN_NAME": "ID",
                        "DATA_TYPE": "NUMBER",
                        "ORDINAL_POSITION": 1,
                        "IS_PRIMARY_KEY": 1,
                    },
                    {
                        "COLUMN_NAME": "STATUS",
                        "DATA_TYPE": "VARCHAR2",
                        "ORDINAL_POSITION": 2,
                        "IS_PRIMARY_KEY": 0,
                    },
                    {
                        "COLUMN_NAME": "UPDATED_AT",
                        "DATA_TYPE": "TIMESTAMP",
                        "ORDINAL_POSITION": 3,
                        "IS_PRIMARY_KEY": 0,
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
        self.assertEqual(inspect["tracked_tables"][0]["primary_key_columns"], ["ID"])

    def test_read_snapshot_emits_rows_and_checkpoint_state(self) -> None:
        adapter = OracleAdapter(self.config)
        spec_rows = [
            {
                "COLUMN_NAME": "ID",
                "DATA_TYPE": "NUMBER",
                "ORDINAL_POSITION": 1,
                "IS_PRIMARY_KEY": 1,
            },
            {
                "COLUMN_NAME": "STATUS",
                "DATA_TYPE": "VARCHAR2",
                "ORDINAL_POSITION": 2,
                "IS_PRIMARY_KEY": 0,
            },
            {
                "COLUMN_NAME": "UPDATED_AT",
                "DATA_TYPE": "TIMESTAMP",
                "ORDINAL_POSITION": 3,
                "IS_PRIMARY_KEY": 0,
            },
        ]
        data_rows = [
            {
                "ID": 1,
                "STATUS": "issued",
                "UPDATED_AT": "2026-04-18T10:00:00Z",
            },
            {
                "ID": 2,
                "STATUS": "paid",
                "UPDATED_AT": "2026-04-18T10:05:00Z",
            },
        ]
        connection = FakeConnection([spec_rows, data_rows])

        with patch.object(adapter, "_connect") as connect:
            connect.return_value.__enter__.return_value = connection
            connect.return_value.__exit__.return_value = False
            events = list(adapter.read_snapshot())

        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].operation, "snapshot")
        self.assertEqual(events[0].primary_key, {"ID": 1})
        self.assertEqual(events[0].after["STATUS"], "issued")
        self.assertIn('"LEDGER.INVOICES"', events[-1].checkpoint_token)

    def test_read_snapshot_respects_checkpoint_resume(self) -> None:
        adapter = OracleAdapter(self.config)
        checkpoint = SourceCheckpoint(
            source_id=self.config.id,
            token='{"LEDGER.INVOICES":{"watermark":"2026-04-18T10:00:00Z","pk":[1]}}',
            updated_at="2026-04-18T10:01:00Z",
        )
        spec_rows = [
            {
                "COLUMN_NAME": "ID",
                "DATA_TYPE": "NUMBER",
                "ORDINAL_POSITION": 1,
                "IS_PRIMARY_KEY": 1,
            },
            {
                "COLUMN_NAME": "STATUS",
                "DATA_TYPE": "VARCHAR2",
                "ORDINAL_POSITION": 2,
                "IS_PRIMARY_KEY": 0,
            },
            {
                "COLUMN_NAME": "UPDATED_AT",
                "DATA_TYPE": "TIMESTAMP",
                "ORDINAL_POSITION": 3,
                "IS_PRIMARY_KEY": 0,
            },
        ]
        data_rows = [
            {
                "ID": 2,
                "STATUS": "paid",
                "UPDATED_AT": "2026-04-18T10:05:00Z",
            }
        ]
        connection = FakeConnection([spec_rows, data_rows])

        with patch.object(adapter, "_connect") as connect:
            connect.return_value.__enter__.return_value = connection
            connect.return_value.__exit__.return_value = False
            events = list(adapter.read_snapshot(checkpoint))

        self.assertEqual(len(events), 1)
        executed_sql, executed_params = connection.cursor_instance.executed[-1]
        self.assertIn('"UPDATED_AT" > :watermark_gt', executed_sql)
        self.assertIn("fetch first 100 rows only", executed_sql.lower())
        self.assertIsNotNone(executed_params)
        self.assertEqual(str(executed_params["watermark_gt"]), "2026-04-18 10:00:00")

    def test_start_stream_raises_until_cdc_is_added(self) -> None:
        adapter = OracleAdapter(self.config)
        with self.assertRaisesRegex(NotImplementedError, "oracle CDC streaming is not implemented yet"):
            list(adapter.start_stream(None))
