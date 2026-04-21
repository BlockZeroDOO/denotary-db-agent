from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from denotary_db_agent.adapters.sqlite import SqliteAdapter
from denotary_db_agent.config import SourceConfig
from denotary_db_agent.models import SourceCheckpoint


def build_source_config(database_path: str, options: dict[str, object] | None = None) -> SourceConfig:
    return SourceConfig(
        id="sqlite-source",
        adapter="sqlite",
        enabled=True,
        source_instance="edge-device-1",
        database_name="ledger",
        backfill_mode="full",
        include={"main": ["invoices"]},
        connection={"path": database_path},
        options=options or {"capture_mode": "watermark"},
    )


class SqliteAdapterTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.database_path = str(Path(self.temp_dir.name) / "ledger.sqlite3")
        self._create_database()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _create_database(self) -> None:
        connection = sqlite3.connect(self.database_path)
        try:
            connection.execute(
                """
                create table invoices (
                    id integer primary key,
                    status text not null,
                    updated_at text not null
                )
                """
            )
            connection.executemany(
                "insert into invoices (id, status, updated_at) values (?, ?, ?)",
                [
                    (1, "issued", "2026-04-20T08:00:00Z"),
                    (2, "paid", "2026-04-20T08:05:00Z"),
                ],
            )
            connection.commit()
        finally:
            connection.close()

    def test_capabilities_declare_wave_two_snapshot_baseline(self) -> None:
        adapter = SqliteAdapter(build_source_config(self.database_path))
        capabilities = adapter.discover_capabilities()

        self.assertEqual(capabilities.source_type, "sqlite")
        self.assertFalse(capabilities.supports_cdc)
        self.assertTrue(capabilities.supports_snapshot)
        self.assertEqual(capabilities.operations, ("snapshot",))
        self.assertEqual(capabilities.capture_modes, ("watermark",))
        self.assertEqual(capabilities.cdc_modes, ())
        self.assertEqual(capabilities.default_capture_mode, "watermark")
        self.assertEqual(capabilities.checkpoint_strategy, "table_watermark")
        self.assertEqual(capabilities.activity_model, "polling")
        self.assertIn("Wave 2", capabilities.notes)

    def test_validate_connection_requires_database_path(self) -> None:
        adapter = SqliteAdapter(build_source_config("", options={"capture_mode": "watermark"}))
        adapter.config.connection = {}

        with self.assertRaisesRegex(ValueError, "sqlite connection is missing required field: path"):
            adapter.validate_connection()

    def test_validate_connection_requires_existing_database(self) -> None:
        missing_path = str(Path(self.temp_dir.name) / "missing.sqlite3")
        adapter = SqliteAdapter(build_source_config(missing_path))

        with self.assertRaisesRegex(ValueError, "sqlite database path does not exist"):
            adapter.validate_connection()

    def test_bootstrap_and_inspect_report_live_tracked_tables(self) -> None:
        adapter = SqliteAdapter(
            build_source_config(
                self.database_path,
                options={
                    "capture_mode": "watermark",
                    "watermark_column": "updated_at",
                    "commit_timestamp_column": "updated_at",
                    "primary_key_columns": ["id"],
                },
            )
        )

        bootstrap = adapter.bootstrap()
        inspect_payload = adapter.inspect()

        self.assertEqual(bootstrap["capture_mode"], "watermark")
        self.assertEqual(bootstrap["tracking_model"], "file_tables")
        self.assertEqual(bootstrap["tracked_tables"][0]["table_name"], "invoices")
        self.assertEqual(bootstrap["tracked_tables"][0]["primary_key_columns"], ["id"])
        self.assertEqual(bootstrap["tracked_tables"][0]["selected_columns"], ["id", "status", "updated_at"])
        self.assertEqual(bootstrap["cdc"]["runtime"]["transport"], "polling")

        self.assertEqual(inspect_payload["source_type"], "sqlite")
        self.assertEqual(inspect_payload["default_capture_mode"], "watermark")
        self.assertEqual(inspect_payload["checkpoint_strategy"], "table_watermark")
        self.assertEqual(inspect_payload["activity_model"], "polling")
        self.assertEqual(inspect_payload["tracking_model"], "file_tables")

    def test_runtime_signature_is_deterministic_for_tracked_tables(self) -> None:
        adapter = SqliteAdapter(
            build_source_config(
                self.database_path,
                options={"capture_mode": "watermark", "dry_run_events": []},
            )
        )

        signature = json.loads(adapter.runtime_signature())

        self.assertEqual(signature["adapter"], "sqlite")
        self.assertEqual(signature["capture_mode"], "watermark")
        self.assertEqual(signature["tracked_tables"][0]["key"], "main.invoices")

    def test_read_snapshot_replays_dry_run_events(self) -> None:
        adapter = SqliteAdapter(
            build_source_config(
                self.database_path,
                options={
                    "capture_mode": "watermark",
                    "dry_run_events": [
                        {
                            "schema_or_namespace": "main",
                            "table_or_collection": "invoices",
                            "operation": "snapshot",
                            "primary_key": {"id": 1},
                            "change_version": "main.invoices:2026-04-20T08:00:00Z:1",
                            "commit_timestamp": "2026-04-20T08:00:00Z",
                            "after": {"id": 1, "status": "issued"},
                            "checkpoint_token": '{"main.invoices":{"watermark":"2026-04-20T08:00:00Z","pk":[1]}}',
                        }
                    ],
                },
            )
        )

        events = list(
            adapter.read_snapshot(
                SourceCheckpoint(
                    source_id="sqlite-source",
                    token="{}",
                    updated_at="2026-04-20T08:00:01Z",
                )
            )
        )

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].source_type, "sqlite")
        self.assertEqual(events[0].primary_key, {"id": 1})
        self.assertEqual(events[0].after["status"], "issued")
        self.assertIn("main.invoices", events[0].checkpoint_token)

    def test_read_snapshot_emits_rows_and_checkpoint_state(self) -> None:
        adapter = SqliteAdapter(
            build_source_config(
                self.database_path,
                options={
                    "capture_mode": "watermark",
                    "watermark_column": "updated_at",
                    "commit_timestamp_column": "updated_at",
                    "primary_key_columns": ["id"],
                    "row_limit": 100,
                },
            )
        )

        events = list(adapter.read_snapshot())

        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].operation, "snapshot")
        self.assertEqual(events[0].primary_key, {"id": 1})
        self.assertEqual(events[0].after["status"], "issued")
        self.assertIn('"main.invoices"', events[-1].checkpoint_token)

    def test_read_snapshot_respects_checkpoint_resume(self) -> None:
        adapter = SqliteAdapter(
            build_source_config(
                self.database_path,
                options={
                    "capture_mode": "watermark",
                    "watermark_column": "updated_at",
                    "commit_timestamp_column": "updated_at",
                    "primary_key_columns": ["id"],
                    "row_limit": 100,
                },
            )
        )
        checkpoint = SourceCheckpoint(
            source_id="sqlite-source",
            token='{"main.invoices":{"watermark":"2026-04-20T08:00:00Z","pk":[1]}}',
            updated_at="2026-04-20T08:01:00Z",
        )

        events = list(adapter.read_snapshot(checkpoint))

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].primary_key, {"id": 2})
        self.assertEqual(events[0].after["status"], "paid")

    def test_live_introspection_resolves_case_insensitive_columns(self) -> None:
        adapter = SqliteAdapter(
            build_source_config(
                self.database_path,
                options={
                    "capture_mode": "watermark",
                    "watermark_column": "UPDATED_AT",
                    "commit_timestamp_column": "updated_at",
                    "primary_key_columns": ["ID"],
                },
            )
        )

        adapter.validate_connection()


if __name__ == "__main__":
    unittest.main()
