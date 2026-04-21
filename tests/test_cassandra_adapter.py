from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from denotary_db_agent.adapters.cassandra import CassandraAdapter
from denotary_db_agent.config import SourceConfig
from denotary_db_agent.models import SourceCheckpoint


class FakeSession:
    def __init__(self, results: list[list[dict[str, object]]]):
        self._results = results
        self.executed: list[tuple[str, tuple[object, ...] | None]] = []

    def execute(self, sql: str, params: tuple[object, ...] | None = None):
        self.executed.append((sql, params))
        return list(self._results.pop(0))


def build_source_config(connection: dict[str, object] | None = None, options: dict[str, object] | None = None) -> SourceConfig:
    return SourceConfig(
        id="cassandra-source",
        adapter="cassandra",
        enabled=True,
        source_instance="events-eu-1",
        database_name="ledger",
        backfill_mode="full",
        include={"ledger": ["invoices"]},
        connection=connection or {
            "host": "127.0.0.1",
            "port": 9042,
            "username": "cassandra",
            "password": "secret",
        },
        options=options or {"capture_mode": "watermark"},
    )


class CassandraAdapterTest(unittest.TestCase):
    def test_capabilities_declare_wave_two_snapshot_baseline(self) -> None:
        adapter = CassandraAdapter(build_source_config())
        capabilities = adapter.discover_capabilities()

        self.assertEqual(capabilities.source_type, "cassandra")
        self.assertFalse(capabilities.supports_cdc)
        self.assertTrue(capabilities.supports_snapshot)
        self.assertEqual(capabilities.operations, ("snapshot",))
        self.assertEqual(capabilities.capture_modes, ("watermark",))
        self.assertEqual(capabilities.default_capture_mode, "watermark")
        self.assertEqual(capabilities.checkpoint_strategy, "table_watermark")
        self.assertEqual(capabilities.activity_model, "polling")
        self.assertIn("Wave 2", capabilities.notes)

    def test_validate_connection_requires_host_or_hosts(self) -> None:
        adapter = CassandraAdapter(build_source_config(connection={"port": 9042}))
        with self.assertRaisesRegex(ValueError, "host or hosts"):
            adapter.validate_connection()

    def test_validate_connection_requires_driver_for_live_use(self) -> None:
        adapter = CassandraAdapter(build_source_config())
        with patch("denotary_db_agent.adapters.cassandra.Cluster", None):
            with self.assertRaisesRegex(RuntimeError, "cassandra-driver is required"):
                adapter.validate_connection()

    def test_bootstrap_and_inspect_report_live_tracked_objects(self) -> None:
        adapter = CassandraAdapter(build_source_config(options={"capture_mode": "watermark", "primary_key_columns": ["ID"]}))
        session = FakeSession(
            [
                [{"release_version": "5.0.1"}],
                [
                    {"column_name": "id", "position": 0},
                    {"column_name": "status", "position": 1},
                    {"column_name": "updated_at", "position": 2},
                ],
                [
                    {"column_name": "id", "kind": "partition_key", "position": 0},
                    {"column_name": "status", "kind": "regular", "position": 1},
                    {"column_name": "updated_at", "kind": "regular", "position": 2},
                ],
                [
                    {"column_name": "id", "position": 0},
                    {"column_name": "status", "position": 1},
                    {"column_name": "updated_at", "position": 2},
                ],
                [
                    {"column_name": "id", "kind": "partition_key", "position": 0},
                    {"column_name": "status", "kind": "regular", "position": 1},
                    {"column_name": "updated_at", "kind": "regular", "position": 2},
                ],
            ]
        )

        with patch("denotary_db_agent.adapters.cassandra.Cluster", object()), patch.object(adapter, "_connect") as connect:
            connect.return_value.__enter__.return_value = session
            connect.return_value.__exit__.return_value = False
            bootstrap = adapter.bootstrap()

        self.assertEqual(bootstrap["capture_mode"], "watermark")
        self.assertEqual(bootstrap["tracked_tables"][0]["table_name"], "invoices")
        self.assertEqual(bootstrap["tracked_tables"][0]["selected_columns"], ["id", "status", "updated_at"])
        self.assertEqual(bootstrap["tracked_tables"][0]["primary_key_columns"], ["id"])
        self.assertEqual(bootstrap["cdc"]["runtime"]["transport"], "polling")

        session = FakeSession(
            [
                [{"release_version": "5.0.1"}],
                [
                    {"column_name": "id", "position": 0},
                    {"column_name": "status", "position": 1},
                    {"column_name": "updated_at", "position": 2},
                ],
                [
                    {"column_name": "id", "kind": "partition_key", "position": 0},
                    {"column_name": "status", "kind": "regular", "position": 1},
                    {"column_name": "updated_at", "kind": "regular", "position": 2},
                ],
                [
                    {"column_name": "id", "position": 0},
                    {"column_name": "status", "position": 1},
                    {"column_name": "updated_at", "position": 2},
                ],
                [
                    {"column_name": "id", "kind": "partition_key", "position": 0},
                    {"column_name": "status", "kind": "regular", "position": 1},
                    {"column_name": "updated_at", "kind": "regular", "position": 2},
                ],
            ]
        )
        with patch("denotary_db_agent.adapters.cassandra.Cluster", object()), patch.object(adapter, "_connect") as connect:
            connect.return_value.__enter__.return_value = session
            connect.return_value.__exit__.return_value = False
            inspect_payload = adapter.inspect()

        self.assertEqual(inspect_payload["source_type"], "cassandra")
        self.assertEqual(inspect_payload["default_capture_mode"], "watermark")
        self.assertEqual(inspect_payload["checkpoint_strategy"], "table_watermark")
        self.assertEqual(inspect_payload["activity_model"], "polling")

    def test_runtime_signature_is_deterministic_for_configured_tables(self) -> None:
        adapter = CassandraAdapter(build_source_config(options={"capture_mode": "watermark", "dry_run_events": []}))
        with patch("denotary_db_agent.adapters.cassandra.Cluster", None):
            signature = json.loads(adapter.runtime_signature())

        self.assertEqual(signature["adapter"], "cassandra")
        self.assertEqual(signature["capture_mode"], "watermark")
        self.assertEqual(signature["tracked_tables"][0]["key"], "ledger.invoices")

    def test_read_snapshot_replays_dry_run_events(self) -> None:
        adapter = CassandraAdapter(
            build_source_config(
                options={
                    "capture_mode": "watermark",
                    "dry_run_events": [
                        {
                            "schema_or_namespace": "ledger",
                            "table_or_collection": "invoices",
                            "operation": "snapshot",
                            "primary_key": {"id": 1},
                            "change_version": "ledger.invoices:2026-04-20T08:00:00Z:1",
                            "commit_timestamp": "2026-04-20T08:00:00Z",
                            "after": {"id": 1, "status": "issued"},
                            "checkpoint_token": '{"ledger.invoices":{"watermark":"2026-04-20T08:00:00Z","pk":[1]}}',
                        }
                    ],
                }
            )
        )

        events = list(adapter.read_snapshot(SourceCheckpoint(source_id="cassandra-source", token="{}", updated_at="2026-04-20T08:00:01Z")))
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].source_type, "cassandra")
        self.assertEqual(events[0].primary_key, {"id": 1})
        self.assertEqual(events[0].after["status"], "issued")

    def test_read_snapshot_emits_rows_and_checkpoint_state(self) -> None:
        adapter = CassandraAdapter(
            build_source_config(
                options={
                    "capture_mode": "watermark",
                    "watermark_column": "updated_at",
                    "commit_timestamp_column": "updated_at",
                    "primary_key_columns": ["id"],
                    "row_limit": 100,
                }
            )
        )
        session = FakeSession(
            [
                [
                    {"column_name": "id", "position": 0},
                    {"column_name": "status", "position": 1},
                    {"column_name": "updated_at", "position": 2},
                ],
                [
                    {"column_name": "id", "kind": "partition_key", "position": 0},
                    {"column_name": "status", "kind": "regular", "position": 1},
                    {"column_name": "updated_at", "kind": "regular", "position": 2},
                ],
                [
                    {"id": 1, "status": "issued", "updated_at": "2026-04-20T08:00:00Z"},
                    {"id": 2, "status": "paid", "updated_at": "2026-04-20T08:05:00Z"},
                ],
            ]
        )

        with patch("denotary_db_agent.adapters.cassandra.Cluster", object()), patch.object(adapter, "_connect") as connect:
            connect.return_value.__enter__.return_value = session
            connect.return_value.__exit__.return_value = False
            events = list(adapter.read_snapshot())

        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].operation, "snapshot")
        self.assertEqual(events[0].primary_key, {"id": 1})
        self.assertIn('"ledger.invoices"', events[-1].checkpoint_token)

    def test_read_snapshot_respects_checkpoint_resume(self) -> None:
        adapter = CassandraAdapter(
            build_source_config(
                options={
                    "capture_mode": "watermark",
                    "watermark_column": "updated_at",
                    "commit_timestamp_column": "updated_at",
                    "primary_key_columns": ["id"],
                    "row_limit": 100,
                }
            )
        )
        checkpoint = SourceCheckpoint(
            source_id="cassandra-source",
            token='{"ledger.invoices":{"watermark":"2026-04-20T08:00:00Z","pk":[1]}}',
            updated_at="2026-04-20T08:01:00Z",
        )
        session = FakeSession(
            [
                [
                    {"column_name": "id", "position": 0},
                    {"column_name": "status", "position": 1},
                    {"column_name": "updated_at", "position": 2},
                ],
                [
                    {"column_name": "id", "kind": "partition_key", "position": 0},
                    {"column_name": "status", "kind": "regular", "position": 1},
                    {"column_name": "updated_at", "kind": "regular", "position": 2},
                ],
                [
                    {"id": 1, "status": "issued", "updated_at": "2026-04-20T08:00:00Z"},
                    {"id": 2, "status": "paid", "updated_at": "2026-04-20T08:05:00Z"},
                ],
            ]
        )

        with patch("denotary_db_agent.adapters.cassandra.Cluster", object()), patch.object(adapter, "_connect") as connect:
            connect.return_value.__enter__.return_value = session
            connect.return_value.__exit__.return_value = False
            events = list(adapter.read_snapshot(checkpoint))

        self.assertEqual(len(events), 1)
        executed_sql, executed_params = session.executed[-1]
        self.assertIn('"updated_at" >= %s', executed_sql)
        self.assertEqual(executed_params[0], "2026-04-20T08:00:00Z")

    def test_read_snapshot_checkpoint_resume_does_not_drop_one_row_at_row_limit_boundary(self) -> None:
        adapter = CassandraAdapter(
            build_source_config(
                options={
                    "capture_mode": "watermark",
                    "watermark_column": "updated_at",
                    "commit_timestamp_column": "updated_at",
                    "primary_key_columns": ["id"],
                    "row_limit": 100,
                }
            )
        )
        checkpoint = SourceCheckpoint(
            source_id="cassandra-source",
            token='{"ledger.invoices":{"watermark":"2026-04-20T08:00:00Z","pk":[100]}}',
            updated_at="2026-04-20T08:01:00Z",
        )
        rows = [{"id": 100, "status": "issued", "updated_at": "2026-04-20T08:00:00Z"}]
        rows.extend(
            {"id": 100 + index, "status": "paid", "updated_at": "2026-04-20T08:05:00Z"}
            for index in range(1, 101)
        )
        session = FakeSession(
            [
                [
                    {"column_name": "id", "position": 0},
                    {"column_name": "status", "position": 1},
                    {"column_name": "updated_at", "position": 2},
                ],
                [
                    {"column_name": "id", "kind": "partition_key", "position": 0},
                    {"column_name": "status", "kind": "regular", "position": 1},
                    {"column_name": "updated_at", "kind": "regular", "position": 2},
                ],
                rows,
            ]
        )

        with patch("denotary_db_agent.adapters.cassandra.Cluster", object()), patch.object(adapter, "_connect") as connect:
            connect.return_value.__enter__.return_value = session
            connect.return_value.__exit__.return_value = False
            events = list(adapter.read_snapshot(checkpoint))

        self.assertEqual(len(events), 100)
        self.assertEqual(events[0].primary_key, {"id": 101})
        self.assertEqual(events[-1].primary_key, {"id": 200})
        executed_sql, _executed_params = session.executed[-1]
        self.assertNotIn("limit %s", executed_sql.lower())


if __name__ == "__main__":
    unittest.main()
