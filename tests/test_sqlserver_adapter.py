from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from denotary_db_agent.adapters.sqlserver import SqlServerAdapter
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


class SqlServerAdapterTest(unittest.TestCase):
    def setUp(self) -> None:
        self.config = SourceConfig(
            id="sqlserver-core-ledger",
            adapter="sqlserver",
            enabled=True,
            source_instance="erp-eu-1",
            database_name="ledger",
            backfill_mode="full",
            include={"dbo": ["invoices"]},
            connection={
                "host": "127.0.0.1",
                "port": 1433,
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
        adapter = SqlServerAdapter(self.config)
        capabilities = adapter.discover_capabilities()

        self.assertEqual(capabilities.source_type, "sqlserver")
        self.assertEqual(capabilities.minimum_version, "2019")
        self.assertTrue(capabilities.supports_cdc)
        self.assertTrue(capabilities.supports_snapshot)
        self.assertEqual(capabilities.operations, ("snapshot",))
        self.assertEqual(capabilities.capture_modes, ("watermark", "change_tracking"))
        self.assertEqual(capabilities.cdc_modes, ("change_tracking",))
        self.assertEqual(
            capabilities.bootstrap_requirements,
            ("tracked tables visible", "watermark columns configured"),
        )
        self.assertIn("Change Tracking CDC baseline", capabilities.notes)

    def test_validate_connection_rejects_missing_fields(self) -> None:
        config = SourceConfig(
            id="sqlserver-core-ledger",
            adapter="sqlserver",
            enabled=True,
            source_instance="erp-eu-1",
            database_name="ledger",
            connection={},
        )
        adapter = SqlServerAdapter(config)

        with self.assertRaisesRegex(
            ValueError,
            "sqlserver connection is missing required fields: host, port, username, database",
        ):
            adapter.validate_connection()

    def test_bootstrap_and_inspect_report_tracked_tables(self) -> None:
        adapter = SqlServerAdapter(self.config)
        connection = FakeConnection(
            [
                [{"version": "Microsoft SQL Server 2019"}],
                [
                    {
                        "column_name": "id",
                        "data_type": "bigint",
                        "ordinal_position": 1,
                        "is_primary_key": 1,
                    },
                    {
                        "column_name": "status",
                        "data_type": "nvarchar",
                        "ordinal_position": 2,
                        "is_primary_key": 0,
                    },
                    {
                        "column_name": "updated_at",
                        "data_type": "datetime2",
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
                        "data_type": "nvarchar",
                        "ordinal_position": 2,
                        "is_primary_key": 0,
                    },
                    {
                        "column_name": "updated_at",
                        "data_type": "datetime2",
                        "ordinal_position": 3,
                        "is_primary_key": 0,
                    },
                ],
            ]
        )

        with patch("denotary_db_agent.adapters.sqlserver.pytds", object()), patch.object(adapter, "_connect") as connect:
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
                        "data_type": "nvarchar",
                        "ordinal_position": 2,
                        "is_primary_key": 0,
                    },
                    {
                        "column_name": "updated_at",
                        "data_type": "datetime2",
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

        self.assertEqual(inspect["capture_modes"], ["watermark", "change_tracking"])
        self.assertEqual(inspect["bootstrap_requirements"], ["tracked tables visible", "watermark columns configured"])
        self.assertEqual(inspect["tracked_tables"][0]["primary_key_columns"], ["id"])

    def test_read_snapshot_emits_rows_and_checkpoint_state(self) -> None:
        adapter = SqlServerAdapter(self.config)
        spec_rows = [
            {
                "column_name": "id",
                "data_type": "bigint",
                "ordinal_position": 1,
                "is_primary_key": 1,
            },
            {
                "column_name": "status",
                "data_type": "nvarchar",
                "ordinal_position": 2,
                "is_primary_key": 0,
            },
            {
                "column_name": "updated_at",
                "data_type": "datetime2",
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
        self.assertIn('"dbo.invoices"', events[-1].checkpoint_token)

    def test_read_snapshot_respects_checkpoint_resume(self) -> None:
        adapter = SqlServerAdapter(self.config)
        checkpoint = SourceCheckpoint(
            source_id=self.config.id,
            token='{"dbo.invoices":{"watermark":"2026-04-18T10:00:00Z","pk":[1]}}',
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
                "data_type": "nvarchar",
                "ordinal_position": 2,
                "is_primary_key": 0,
            },
            {
                "column_name": "updated_at",
                "data_type": "datetime2",
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
        self.assertIn("[updated_at] > %s", executed_sql)
        self.assertIn("top 100", executed_sql.lower())
        self.assertIsNotNone(executed_params)
        self.assertEqual(executed_params[0], "2026-04-18T10:00:00Z")

    def test_start_stream_raises_until_cdc_is_added(self) -> None:
        adapter = SqlServerAdapter(self.config)
        with self.assertRaisesRegex(
            NotImplementedError,
            "sqlserver CDC streaming is only available when capture_mode is set to change_tracking",
        ):
            list(adapter.start_stream(None))

    def test_change_tracking_bootstrap_and_inspect_report_cdc_status(self) -> None:
        config = SourceConfig(
            **{
                **self.config.__dict__,
                "options": {
                    **self.config.options,
                    "capture_mode": "change_tracking",
                },
            }
        )
        adapter = SqlServerAdapter(config)
        spec = adapter._spec_summary(
            adapter_spec := type(
                "Spec",
                (),
                {
                    "schema_name": "dbo",
                    "table_name": "invoices",
                    "watermark_column": "updated_at",
                    "commit_timestamp_column": "updated_at",
                    "primary_key_columns": ["id"],
                    "selected_columns": ["id", "status", "updated_at"],
                },
            )()
        )
        cdc_summary = {
            "capture_mode": "change_tracking",
            "database_enabled": True,
            "current_version": 42,
            "runtime": {
                "transport": "polling",
                "cursor": {"version": 42},
            },
            "tracked_tables": [
                {
                    "table_key": "dbo.invoices",
                    "enabled": True,
                    "begin_version": 1,
                    "min_valid_version": 1,
                    "cleanup_version": 0,
                    "track_columns_updated": True,
                }
            ],
        }

        with patch.object(adapter, "validate_connection"), patch.object(adapter, "_connect") as connect, patch.object(
            adapter, "_load_table_specs", return_value=[adapter_spec]
        ), patch.object(adapter, "_cdc_summary", return_value=cdc_summary):
            connect.return_value.__enter__.return_value = FakeConnection([[{"version": "Microsoft SQL Server 2022"}]])
            connect.return_value.__exit__.return_value = False
            bootstrap = adapter.bootstrap()
            inspect = adapter.inspect()

        self.assertEqual(bootstrap["capture_mode"], "change_tracking")
        self.assertEqual(bootstrap["cdc"]["current_version"], 42)
        self.assertEqual(bootstrap["cdc"]["runtime"]["transport"], "polling")
        self.assertEqual(bootstrap["cdc"]["runtime"]["cursor"]["version"], 42)
        self.assertEqual(bootstrap["tracked_tables"][0]["table_name"], spec["table_name"])
        self.assertEqual(inspect["capture_modes"], ["watermark", "change_tracking"])
        self.assertEqual(inspect["cdc_modes"], ["change_tracking"])
        self.assertTrue(inspect["supports_cdc"])

    def test_change_tracking_stream_emits_changes_and_checkpoint_cursor(self) -> None:
        config = SourceConfig(
            **{
                **self.config.__dict__,
                "backfill_mode": "none",
                "options": {
                    **self.config.options,
                    "capture_mode": "change_tracking",
                    "row_limit": 2,
                },
            }
        )
        adapter = SqlServerAdapter(config)
        spec = type(
            "Spec",
            (),
            {
                "schema_name": "dbo",
                "table_name": "invoices",
                "key": "dbo.invoices",
                "primary_key_columns": ["id"],
                "selected_columns": ["id", "status", "updated_at"],
                "commit_timestamp_column": "updated_at",
            },
        )()
        changes = [
            {
                "spec": spec,
                "table_key": "dbo.invoices",
                "change_version": 101,
                "operation": "insert",
                "primary_key": {"id": 1},
                "pk_marker": [1],
                "before": None,
                "after": {"id": 1, "status": "draft", "updated_at": "2026-04-18T10:00:01Z"},
                "commit_timestamp": "2026-04-18T10:00:01Z",
                "metadata": {"capture_mode": "change_tracking"},
            },
            {
                "spec": spec,
                "table_key": "dbo.invoices",
                "change_version": 101,
                "operation": "update",
                "primary_key": {"id": 2},
                "pk_marker": [2],
                "before": None,
                "after": {"id": 2, "status": "issued", "updated_at": "2026-04-18T10:00:02Z"},
                "commit_timestamp": "2026-04-18T10:00:02Z",
                "metadata": {"capture_mode": "change_tracking"},
            },
            {
                "spec": spec,
                "table_key": "dbo.invoices",
                "change_version": 102,
                "operation": "delete",
                "primary_key": {"id": 3},
                "pk_marker": [3],
                "before": None,
                "after": None,
                "commit_timestamp": "2026-04-18T10:00:03Z",
                "metadata": {"capture_mode": "change_tracking"},
            },
        ]

        with patch.object(adapter, "_connect") as connect, patch.object(adapter, "_load_table_specs", return_value=[spec]), patch.object(
            adapter, "_current_change_tracking_version", return_value=120
        ), patch.object(adapter, "_validate_change_tracking_prerequisites"), patch.object(
            adapter, "_fetch_change_tracking_rows", return_value=changes
        ):
            connect.return_value.__enter__.return_value = object()
            connect.return_value.__exit__.return_value = False
            initial = list(adapter.start_stream(None))
            first_pass = list(adapter.start_stream(None))

        self.assertEqual(initial, [])
        self.assertEqual([event.operation for event in first_pass], ["insert", "update"])
        first_checkpoint = json.loads(first_pass[-1].checkpoint_token)
        self.assertEqual(first_checkpoint["mode"], "change_tracking")
        self.assertEqual(first_checkpoint["version"], 101)
        self.assertNotIn("table_key", first_checkpoint)
        self.assertNotIn("pk", first_checkpoint)

        with patch.object(adapter, "_connect") as connect, patch.object(adapter, "_load_table_specs", return_value=[spec]), patch.object(
            adapter, "_current_change_tracking_version", return_value=120
        ), patch.object(adapter, "_validate_change_tracking_prerequisites"), patch.object(
            adapter, "_fetch_change_tracking_rows", return_value=changes[2:]
        ):
            connect.return_value.__enter__.return_value = object()
            connect.return_value.__exit__.return_value = False
            resumed = list(
                adapter.start_stream(
                    SourceCheckpoint(
                        source_id=config.id,
                        token=first_pass[-1].checkpoint_token,
                        updated_at="2026-04-18T10:00:02Z",
                    )
                )
            )

        self.assertEqual(len(resumed), 1)
        self.assertEqual(resumed[0].operation, "delete")
        resumed_checkpoint = json.loads(resumed[0].checkpoint_token)
        self.assertEqual(resumed_checkpoint, {"mode": "change_tracking", "version": 102})
