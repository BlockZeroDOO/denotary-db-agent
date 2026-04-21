from __future__ import annotations

import json
import unittest
from datetime import datetime
from unittest.mock import patch

from denotary_db_agent.adapters.db2 import Db2Adapter
from denotary_db_agent.config import SourceConfig
from denotary_db_agent.models import SourceCheckpoint


class FakeCursor:
    def __init__(self, results: list[list[dict[str, object]]]):
        self._results = results
        self.executed: list[tuple[str, tuple[object, ...] | None]] = []
        self._current: list[dict[str, object]] = []

    def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
        self.executed.append((sql, params))
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


def build_source_config(connection: dict[str, object] | None = None, options: dict[str, object] | None = None) -> SourceConfig:
    return SourceConfig(
        id="db2-source",
        adapter="db2",
        enabled=True,
        source_instance="erp-eu-1",
        database_name="LEDGER",
        backfill_mode="full",
        include={"DB2INST1": ["INVOICES"]},
        connection=connection or {
            "host": "127.0.0.1",
            "port": 50000,
            "username": "db2inst1",
            "password": "secret",
            "database": "LEDGER",
        },
        options=options or {"capture_mode": "watermark"},
    )


class Db2AdapterTest(unittest.TestCase):
    def test_capabilities_declare_wave_two_snapshot_baseline(self) -> None:
        adapter = Db2Adapter(build_source_config())
        capabilities = adapter.discover_capabilities()

        self.assertEqual(capabilities.source_type, "db2")
        self.assertFalse(capabilities.supports_cdc)
        self.assertTrue(capabilities.supports_snapshot)
        self.assertEqual(capabilities.operations, ("snapshot",))
        self.assertEqual(capabilities.capture_modes, ("watermark",))
        self.assertEqual(capabilities.default_capture_mode, "watermark")
        self.assertEqual(capabilities.checkpoint_strategy, "table_watermark")
        self.assertEqual(capabilities.activity_model, "polling")
        self.assertIn("Wave 2", capabilities.notes)

    def test_validate_connection_requires_core_db2_fields(self) -> None:
        adapter = Db2Adapter(build_source_config(connection={"host": "127.0.0.1"}))
        with self.assertRaisesRegex(ValueError, "port, username, password, database"):
            adapter.validate_connection()

    def test_validate_connection_requires_driver_for_live_use(self) -> None:
        adapter = Db2Adapter(build_source_config())
        with patch("denotary_db_agent.adapters.db2.ibm_db_dbi", None):
            with self.assertRaisesRegex(RuntimeError, "ibm_db is required"):
                adapter.validate_connection()

    def test_bootstrap_and_inspect_report_live_tracked_objects(self) -> None:
        adapter = Db2Adapter(build_source_config(options={"capture_mode": "watermark", "primary_key_columns": ["ID"]}))
        connection = FakeConnection(
            [
                [{"OK": 1}],
                [{"COLNAME": "ID"}, {"COLNAME": "STATUS"}, {"COLNAME": "UPDATED_AT"}],
                [{"COLNAME": "ID"}],
                [{"COLNAME": "ID"}, {"COLNAME": "STATUS"}, {"COLNAME": "UPDATED_AT"}],
                [{"COLNAME": "ID"}],
            ]
        )

        with patch("denotary_db_agent.adapters.db2.ibm_db_dbi", object()), patch.object(adapter, "_connect") as connect:
            connect.return_value.__enter__.return_value = connection
            connect.return_value.__exit__.return_value = False
            bootstrap = adapter.bootstrap()

        self.assertEqual(bootstrap["capture_mode"], "watermark")
        self.assertEqual(bootstrap["tracked_tables"][0]["table_name"], "INVOICES")
        self.assertEqual(bootstrap["tracked_tables"][0]["selected_columns"], ["ID", "STATUS", "UPDATED_AT"])
        self.assertEqual(bootstrap["tracked_tables"][0]["primary_key_columns"], ["ID"])
        self.assertEqual(bootstrap["cdc"]["runtime"]["transport"], "polling")

        connection = FakeConnection(
            [
                [{"OK": 1}],
                [{"COLNAME": "ID"}, {"COLNAME": "STATUS"}, {"COLNAME": "UPDATED_AT"}],
                [{"COLNAME": "ID"}],
                [{"COLNAME": "ID"}, {"COLNAME": "STATUS"}, {"COLNAME": "UPDATED_AT"}],
                [{"COLNAME": "ID"}],
            ]
        )
        with patch("denotary_db_agent.adapters.db2.ibm_db_dbi", object()), patch.object(adapter, "_connect") as connect:
            connect.return_value.__enter__.return_value = connection
            connect.return_value.__exit__.return_value = False
            inspect_payload = adapter.inspect()

        self.assertEqual(inspect_payload["source_type"], "db2")
        self.assertEqual(inspect_payload["default_capture_mode"], "watermark")
        self.assertEqual(inspect_payload["checkpoint_strategy"], "table_watermark")
        self.assertEqual(inspect_payload["activity_model"], "polling")

    def test_runtime_signature_is_deterministic_for_configured_tables(self) -> None:
        adapter = Db2Adapter(build_source_config(options={"capture_mode": "watermark", "dry_run_events": []}))
        signature = json.loads(adapter.runtime_signature())

        self.assertEqual(signature["adapter"], "db2")
        self.assertEqual(signature["capture_mode"], "watermark")
        self.assertEqual(signature["tracked_tables"][0]["key"], "DB2INST1.INVOICES")

    def test_read_snapshot_replays_dry_run_events(self) -> None:
        adapter = Db2Adapter(
            build_source_config(
                options={
                    "capture_mode": "watermark",
                    "dry_run_events": [
                        {
                            "schema_or_namespace": "DB2INST1",
                            "table_or_collection": "INVOICES",
                            "operation": "snapshot",
                            "primary_key": {"ID": 1},
                            "change_version": "DB2INST1.INVOICES:2026-04-20T08:00:00Z:1",
                            "commit_timestamp": "2026-04-20T08:00:00Z",
                            "after": {"ID": 1, "STATUS": "issued"},
                            "checkpoint_token": '{"DB2INST1.INVOICES":{"watermark":"2026-04-20T08:00:00Z","pk":[1]}}',
                        }
                    ],
                }
            )
        )

        events = list(adapter.read_snapshot(SourceCheckpoint(source_id="db2-source", token="{}", updated_at="2026-04-20T08:00:01Z")))
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].source_type, "db2")
        self.assertEqual(events[0].primary_key, {"ID": 1})
        self.assertEqual(events[0].after["STATUS"], "issued")

    def test_read_snapshot_emits_rows_and_checkpoint_state(self) -> None:
        adapter = Db2Adapter(
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
                [{"COLNAME": "ID"}, {"COLNAME": "STATUS"}, {"COLNAME": "UPDATED_AT"}],
                [{"COLNAME": "ID"}],
                [
                    {"ID": 1, "STATUS": "issued", "UPDATED_AT": "2026-04-20T08:00:00Z"},
                    {"ID": 2, "STATUS": "paid", "UPDATED_AT": "2026-04-20T08:05:00Z"},
                ],
            ]
        )

        with patch("denotary_db_agent.adapters.db2.ibm_db_dbi", object()), patch.object(adapter, "_connect") as connect:
            connect.return_value.__enter__.return_value = connection
            connect.return_value.__exit__.return_value = False
            events = list(adapter.read_snapshot())

        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].operation, "snapshot")
        self.assertEqual(events[0].primary_key, {"ID": 1})
        self.assertIn('"DB2INST1.INVOICES"', events[-1].checkpoint_token)

    def test_read_snapshot_respects_checkpoint_resume(self) -> None:
        adapter = Db2Adapter(
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
            source_id="db2-source",
            token='{"DB2INST1.INVOICES":{"watermark":"2026-04-20T08:00:00Z","pk":[1]}}',
            updated_at="2026-04-20T08:01:00Z",
        )
        connection = FakeConnection(
            [
                [{"COLNAME": "ID"}, {"COLNAME": "STATUS"}, {"COLNAME": "UPDATED_AT"}],
                [{"COLNAME": "ID"}],
                [{"ID": 2, "STATUS": "paid", "UPDATED_AT": "2026-04-20T08:05:00Z"}],
            ]
        )

        with patch("denotary_db_agent.adapters.db2.ibm_db_dbi", object()), patch.object(adapter, "_connect") as connect:
            connect.return_value.__enter__.return_value = connection
            connect.return_value.__exit__.return_value = False
            events = list(adapter.read_snapshot(checkpoint))

        self.assertEqual(len(events), 1)
        executed_sql, executed_params = connection.cursor_instance.executed[-1]
        self.assertIn('"UPDATED_AT" > ?', executed_sql)
        self.assertIsInstance(executed_params[0], datetime)
        self.assertEqual(executed_params[0].isoformat(), "2026-04-20T08:00:00")


if __name__ == "__main__":
    unittest.main()
