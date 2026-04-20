from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from denotary_db_agent.adapters.snowflake import SnowflakeAdapter
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

    def cursor(self, *args, **kwargs):
        return self.cursor_instance

    def close(self) -> None:
        self.closed = True


def build_source_config(connection: dict[str, object] | None = None, options: dict[str, object] | None = None) -> SourceConfig:
    return SourceConfig(
        id="snowflake-source",
        adapter="snowflake",
        enabled=True,
        source_instance="analytics-us-1",
        database_name="ANALYTICS",
        backfill_mode="full",
        include={"PUBLIC": ["ORDERS"]},
        connection=connection or {
            "account": "acme-org.eu-central-1",
            "username": "denotary",
            "database": "ANALYTICS",
            "schema": "PUBLIC",
            "warehouse": "NOTARY_WH",
        },
        options=options or {"capture_mode": "watermark"},
    )


class SnowflakeAdapterTest(unittest.TestCase):
    def test_capabilities_declare_wave_two_snapshot_baseline(self) -> None:
        adapter = SnowflakeAdapter(build_source_config())
        capabilities = adapter.discover_capabilities()

        self.assertEqual(capabilities.source_type, "snowflake")
        self.assertFalse(capabilities.supports_cdc)
        self.assertTrue(capabilities.supports_snapshot)
        self.assertEqual(capabilities.operations, ("snapshot",))
        self.assertEqual(capabilities.capture_modes, ("watermark",))
        self.assertEqual(capabilities.cdc_modes, ())
        self.assertEqual(capabilities.default_capture_mode, "watermark")
        self.assertEqual(capabilities.checkpoint_strategy, "table_watermark")
        self.assertEqual(capabilities.activity_model, "polling")
        self.assertIn("Wave 2", capabilities.notes)

    def test_validate_connection_requires_core_snowflake_fields(self) -> None:
        adapter = SnowflakeAdapter(
            build_source_config(
                connection={
                    "account": "acme-org.eu-central-1",
                    "username": "denotary",
                }
            )
        )

        with self.assertRaisesRegex(
            ValueError,
            "snowflake connection is missing required fields: database, schema, warehouse",
        ):
            adapter.validate_connection()

    def test_validate_connection_requires_connector_for_live_use(self) -> None:
        adapter = SnowflakeAdapter(build_source_config())

        with patch("denotary_db_agent.adapters.snowflake.snowflake_connector", None):
            with self.assertRaisesRegex(RuntimeError, "snowflake-connector-python is required"):
                adapter.validate_connection()

    def test_bootstrap_and_inspect_report_live_tracked_objects(self) -> None:
        adapter = SnowflakeAdapter(build_source_config(options={"capture_mode": "watermark", "primary_key_columns": ["ID"]}))
        connection = FakeConnection(
            [
                [{"version": "9.0.0"}],
                [{"COLUMN_NAME": "ID"}, {"COLUMN_NAME": "STATUS"}, {"COLUMN_NAME": "UPDATED_AT"}],
                [{"COLUMN_NAME": "ID"}],
                [{"COLUMN_NAME": "ID"}, {"COLUMN_NAME": "STATUS"}, {"COLUMN_NAME": "UPDATED_AT"}],
                [{"COLUMN_NAME": "ID"}],
            ]
        )

        with patch("denotary_db_agent.adapters.snowflake.snowflake_connector", object()), patch.object(adapter, "_connect") as connect:
            connect.return_value.__enter__.return_value = connection
            connect.return_value.__exit__.return_value = False
            bootstrap = adapter.bootstrap()

        self.assertEqual(bootstrap["capture_mode"], "watermark")
        self.assertEqual(bootstrap["tracking_model"], "configured_objects")
        self.assertEqual(bootstrap["tracked_tables"][0]["table_name"], "ORDERS")
        self.assertEqual(bootstrap["tracked_tables"][0]["selected_columns"], ["ID", "STATUS", "UPDATED_AT"])
        self.assertEqual(bootstrap["tracked_tables"][0]["primary_key_columns"], ["ID"])
        self.assertEqual(bootstrap["cdc"]["runtime"]["transport"], "polling")

        connection = FakeConnection(
            [
                [{"version": "9.0.0"}],
                [{"COLUMN_NAME": "ID"}, {"COLUMN_NAME": "STATUS"}, {"COLUMN_NAME": "UPDATED_AT"}],
                [{"COLUMN_NAME": "ID"}],
                [{"COLUMN_NAME": "ID"}, {"COLUMN_NAME": "STATUS"}, {"COLUMN_NAME": "UPDATED_AT"}],
                [{"COLUMN_NAME": "ID"}],
            ]
        )
        with patch("denotary_db_agent.adapters.snowflake.snowflake_connector", object()), patch.object(adapter, "_connect") as connect:
            connect.return_value.__enter__.return_value = connection
            connect.return_value.__exit__.return_value = False
            inspect_payload = adapter.inspect()

        self.assertEqual(inspect_payload["source_type"], "snowflake")
        self.assertEqual(inspect_payload["default_capture_mode"], "watermark")
        self.assertEqual(inspect_payload["checkpoint_strategy"], "table_watermark")
        self.assertEqual(inspect_payload["activity_model"], "polling")
        self.assertEqual(inspect_payload["capture_modes"], ["watermark"])
        self.assertEqual(inspect_payload["cdc_modes"], [])
        self.assertFalse(inspect_payload["is_cdc_mode"])
        self.assertEqual(inspect_payload["tracking_model"], "configured_objects")

    def test_bootstrap_reports_tracked_tables_and_views_from_configured_metadata(self) -> None:
        adapter = SnowflakeAdapter(
            build_source_config(
                options={
                    "capture_mode": "watermark",
                    "tracked_object_type": "view",
                    "watermark_column": "COMMITTED_AT",
                    "commit_timestamp_column": "COMMITTED_AT",
                    "primary_key_columns": ["ORDER_ID", "LINE_ID"],
                }
            )
        )
        connection = FakeConnection(
            [
                [{"version": "9.0.0"}],
                [{"COLUMN_NAME": "ORDER_ID"}, {"COLUMN_NAME": "LINE_ID"}, {"COLUMN_NAME": "COMMITTED_AT"}],
                [{"COLUMN_NAME": "ORDER_ID"}, {"COLUMN_NAME": "LINE_ID"}, {"COLUMN_NAME": "COMMITTED_AT"}],
            ]
        )

        with patch("denotary_db_agent.adapters.snowflake.snowflake_connector", object()), patch.object(adapter, "_connect") as connect:
            connect.return_value.__enter__.return_value = connection
            connect.return_value.__exit__.return_value = False
            bootstrap = adapter.bootstrap()

        self.assertEqual(bootstrap["capture_mode"], "watermark")
        self.assertEqual(bootstrap["tracking_model"], "configured_objects")
        self.assertEqual(len(bootstrap["tracked_tables"]), 1)
        self.assertEqual(bootstrap["tracked_tables"][0]["object_type"], "view")
        self.assertEqual(bootstrap["tracked_tables"][0]["watermark_column"], "COMMITTED_AT")
        self.assertEqual(bootstrap["tracked_tables"][0]["primary_key_columns"], ["ORDER_ID", "LINE_ID"])
        self.assertFalse(bootstrap["cdc"]["supports_cdc"])

    def test_runtime_signature_is_deterministic_for_configured_objects(self) -> None:
        adapter = SnowflakeAdapter(build_source_config(options={"capture_mode": "watermark", "dry_run_events": []}))

        signature = json.loads(adapter.runtime_signature())

        self.assertEqual(signature["adapter"], "snowflake")
        self.assertEqual(signature["capture_mode"], "watermark")
        self.assertEqual(signature["tracked_tables"][0]["key"], "ANALYTICS.PUBLIC.ORDERS")
        self.assertEqual(signature["tracked_tables"][0]["object_type"], "table")

    def test_read_snapshot_replays_dry_run_events(self) -> None:
        adapter = SnowflakeAdapter(
            build_source_config(
                options={
                    "capture_mode": "watermark",
                    "dry_run_events": [
                        {
                            "schema_or_namespace": "PUBLIC",
                            "table_or_collection": "ORDERS",
                            "operation": "snapshot",
                            "primary_key": {"ID": 1},
                            "change_version": "PUBLIC.ORDERS:2026-04-20T08:00:00Z:1",
                            "commit_timestamp": "2026-04-20T08:00:00Z",
                            "after": {"ID": 1, "STATUS": "issued"},
                            "checkpoint_token": '{"PUBLIC.ORDERS":{"watermark":"2026-04-20T08:00:00Z","pk":[1]}}',
                        }
                    ],
                }
            )
        )

        events = list(
            adapter.read_snapshot(
                SourceCheckpoint(
                    source_id="snowflake-source",
                    token="{}",
                    updated_at="2026-04-20T08:00:01Z",
                )
            )
        )

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].source_type, "snowflake")
        self.assertEqual(events[0].primary_key, {"ID": 1})
        self.assertEqual(events[0].after["STATUS"], "issued")
        self.assertIn("PUBLIC.ORDERS", events[0].checkpoint_token)

    def test_read_snapshot_emits_rows_and_checkpoint_state(self) -> None:
        adapter = SnowflakeAdapter(
            build_source_config(
                options={
                    "capture_mode": "watermark",
                    "watermark_column": "UPDATED_AT",
                    "commit_timestamp_column": "UPDATED_AT",
                    "primary_key_columns": ["ID"],
                    "row_limit": 100,
                }
            )
        )
        connection = FakeConnection(
            [
                [{"COLUMN_NAME": "ID"}, {"COLUMN_NAME": "STATUS"}, {"COLUMN_NAME": "UPDATED_AT"}],
                [{"COLUMN_NAME": "ID"}],
                [
                    {"ID": 1, "STATUS": "issued", "UPDATED_AT": "2026-04-20T08:00:00Z"},
                    {"ID": 2, "STATUS": "paid", "UPDATED_AT": "2026-04-20T08:05:00Z"},
                ],
            ]
        )

        with patch("denotary_db_agent.adapters.snowflake.snowflake_connector", object()), patch.object(adapter, "_connect") as connect:
            connect.return_value.__enter__.return_value = connection
            connect.return_value.__exit__.return_value = False
            events = list(adapter.read_snapshot())

        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].operation, "snapshot")
        self.assertEqual(events[0].primary_key, {"ID": 1})
        self.assertEqual(events[0].after["STATUS"], "issued")
        self.assertIn('"ANALYTICS.PUBLIC.ORDERS"', events[-1].checkpoint_token)

    def test_read_snapshot_respects_checkpoint_resume(self) -> None:
        adapter = SnowflakeAdapter(
            build_source_config(
                options={
                    "capture_mode": "watermark",
                    "watermark_column": "UPDATED_AT",
                    "commit_timestamp_column": "UPDATED_AT",
                    "primary_key_columns": ["ID"],
                    "row_limit": 100,
                }
            )
        )
        checkpoint = SourceCheckpoint(
            source_id="snowflake-source",
            token='{"ANALYTICS.PUBLIC.ORDERS":{"watermark":"2026-04-20T08:00:00Z","pk":[1]}}',
            updated_at="2026-04-20T08:01:00Z",
        )
        connection = FakeConnection(
            [
                [{"COLUMN_NAME": "ID"}, {"COLUMN_NAME": "STATUS"}, {"COLUMN_NAME": "UPDATED_AT"}],
                [{"COLUMN_NAME": "ID"}],
                [
                    {"ID": 2, "STATUS": "paid", "UPDATED_AT": "2026-04-20T08:05:00Z"},
                ],
            ]
        )

        with patch("denotary_db_agent.adapters.snowflake.snowflake_connector", object()), patch.object(adapter, "_connect") as connect:
            connect.return_value.__enter__.return_value = connection
            connect.return_value.__exit__.return_value = False
            events = list(adapter.read_snapshot(checkpoint))

        self.assertEqual(len(events), 1)
        executed_sql, executed_params = connection.cursor_instance.executed[-1]
        self.assertIn('"UPDATED_AT" > %s', executed_sql)
        self.assertIsNotNone(executed_params)
        self.assertEqual(executed_params[0], "2026-04-20T08:00:00Z")

    def test_object_name_prefix_can_override_default_object_type(self) -> None:
        adapter = SnowflakeAdapter(
            build_source_config(
                options={
                    "capture_mode": "watermark",
                    "tracked_object_type": "table",
                }
            )
        )
        adapter.config.include = {"PUBLIC": ["view:ORDERS_AUDIT"]}
        connection = FakeConnection(
            [
                [{"version": "9.0.0"}],
                [{"COLUMN_NAME": "ID"}, {"COLUMN_NAME": "UPDATED_AT"}],
                [{"COLUMN_NAME": "ID"}, {"COLUMN_NAME": "UPDATED_AT"}],
            ]
        )

        with patch("denotary_db_agent.adapters.snowflake.snowflake_connector", object()), patch.object(adapter, "_connect") as connect:
            connect.return_value.__enter__.return_value = connection
            connect.return_value.__exit__.return_value = False
            inspect_payload = adapter.inspect()

        self.assertEqual(inspect_payload["tracked_tables"][0]["object_type"], "view")
        self.assertEqual(inspect_payload["tracked_tables"][0]["table_name"], "ORDERS_AUDIT")

    def test_live_introspection_resolves_case_insensitive_columns(self) -> None:
        adapter = SnowflakeAdapter(
            build_source_config(
                options={
                    "capture_mode": "watermark",
                    "watermark_column": "updated_at",
                    "commit_timestamp_column": "updated_at",
                    "primary_key_columns": ["id"],
                }
            )
        )
        connection = FakeConnection(
            [
                [{"version": "9.0.0"}],
                [{"column_name": "ID"}, {"column_name": "STATUS"}, {"column_name": "UPDATED_AT"}],
                [{"column_name": "ID"}],
            ]
        )

        with patch("denotary_db_agent.adapters.snowflake.snowflake_connector", object()), patch.object(adapter, "_connect") as connect:
            connect.return_value.__enter__.return_value = connection
            connect.return_value.__exit__.return_value = False
            adapter.validate_connection()

        self.assertEqual(len(connection.cursor_instance.executed), 3)


if __name__ == "__main__":
    unittest.main()
