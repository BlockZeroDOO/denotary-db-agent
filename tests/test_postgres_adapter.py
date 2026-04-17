from __future__ import annotations

import json
import unittest
from contextlib import contextmanager
from unittest.mock import patch

from denotary_db_agent.adapters.postgres import PostgresAdapter, PostgresTableSpec
from denotary_db_agent.config import SourceConfig
from denotary_db_agent.models import SourceCheckpoint


class PostgresAdapterTest(unittest.TestCase):
    def make_config(self) -> SourceConfig:
        return SourceConfig(
            id="pg-core-ledger",
            adapter="postgresql",
            enabled=True,
            source_instance="erp-eu-1",
            database_name="ledger",
            include={"public": ["invoices", "payments"]},
            exclude={},
            checkpoint_policy="after_ack",
            backfill_mode="full",
            batch_enabled=False,
            batch_size=100,
            flush_interval_ms=1000,
            connection={
                "host": "127.0.0.1",
                "port": 5432,
                "username": "denotary",
                "database": "ledger",
            },
            options={
                "watermark_column": "updated_at",
                "commit_timestamp_column": "updated_at",
                "row_limit": 100,
            },
        )

    def test_validate_connection_uses_live_connectivity(self) -> None:
        adapter = PostgresAdapter(self.make_config())

        class DummyCursor:
            def execute(self, *_args, **_kwargs):
                return self

            def fetchone(self):
                return {"version": "PostgreSQL 16"}

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        class DummyConnection:
            def cursor(self):
                return DummyCursor()

        @contextmanager
        def dummy_connect():
            yield DummyConnection()

        with patch.object(adapter, "_connect", dummy_connect), patch.object(
            adapter, "_load_table_specs", return_value=[]
        ) as load_specs:
            adapter.validate_connection()
        load_specs.assert_called_once()

    def test_discover_capabilities_describes_pgoutput_bootstrap_mode(self) -> None:
        config = self.make_config()
        config.options["capture_mode"] = "logical"
        config.options["output_plugin"] = "pgoutput"
        adapter = PostgresAdapter(config)

        capabilities = adapter.discover_capabilities()

        self.assertTrue(capabilities.supports_cdc)
        self.assertIn("pgoutput", capabilities.notes)

    def test_read_snapshot_emits_sorted_events_and_checkpoint_state(self) -> None:
        adapter = PostgresAdapter(self.make_config())
        specs = [
            PostgresTableSpec(
                schema_name="public",
                table_name="invoices",
                watermark_column="updated_at",
                commit_timestamp_column="updated_at",
                primary_key_columns=["id"],
                selected_columns=["id", "updated_at", "status"],
            ),
            PostgresTableSpec(
                schema_name="public",
                table_name="payments",
                watermark_column="updated_at",
                commit_timestamp_column="updated_at",
                primary_key_columns=["id"],
                selected_columns=["id", "updated_at", "amount"],
            ),
        ]

        rows = {
            "public.invoices": [
                {"id": 2, "updated_at": "2026-04-17T10:00:02Z", "status": "issued"},
            ],
            "public.payments": [
                {"id": 1, "updated_at": "2026-04-17T10:00:01Z", "amount": "100.00"},
            ],
        }

        @contextmanager
        def dummy_connect():
            yield object()

        def fake_fetch_rows(_connection, spec, _table_state):
            return rows[spec.key]

        with patch.object(adapter, "_connect", dummy_connect), patch.object(
            adapter, "_load_table_specs", return_value=specs
        ), patch.object(adapter, "_fetch_rows", side_effect=fake_fetch_rows):
            events = list(adapter.read_snapshot())

        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].table_or_collection, "payments")
        self.assertEqual(events[1].table_or_collection, "invoices")
        self.assertEqual(events[0].operation, "snapshot")
        checkpoint_state = json.loads(events[-1].checkpoint_token)
        self.assertEqual(checkpoint_state["public.payments"]["watermark"], "2026-04-17T10:00:01Z")
        self.assertEqual(checkpoint_state["public.invoices"]["watermark"], "2026-04-17T10:00:02Z")

    def test_read_snapshot_respects_checkpoint_resume(self) -> None:
        adapter = PostgresAdapter(self.make_config())
        spec = PostgresTableSpec(
            schema_name="public",
            table_name="invoices",
            watermark_column="updated_at",
            commit_timestamp_column="updated_at",
            primary_key_columns=["id"],
            selected_columns=["id", "updated_at", "status"],
        )
        checkpoint = SourceCheckpoint(
            source_id="pg-core-ledger",
            token=json.dumps({"public.invoices": {"watermark": "2026-04-17T10:00:00Z", "pk": ["10"]}}),
            updated_at="2026-04-17T10:00:00Z",
        )

        @contextmanager
        def dummy_connect():
            yield object()

        def fake_fetch_rows(_connection, _spec, table_state):
            self.assertEqual(table_state, {"watermark": "2026-04-17T10:00:00Z", "pk": ["10"]})
            return [{"id": 11, "updated_at": "2026-04-17T10:00:01Z", "status": "issued"}]

        with patch.object(adapter, "_connect", dummy_connect), patch.object(
            adapter, "_load_table_specs", return_value=[spec]
        ), patch.object(adapter, "_fetch_rows", side_effect=fake_fetch_rows):
            events = list(adapter.read_snapshot(checkpoint))

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].primary_key["id"], 11)
        parsed_checkpoint = json.loads(events[0].checkpoint_token)
        self.assertEqual(parsed_checkpoint["public.invoices"]["pk"], ["11"])

    def test_bootstrap_reports_tracked_tables_for_trigger_mode(self) -> None:
        config = self.make_config()
        config.options["capture_mode"] = "trigger"
        adapter = PostgresAdapter(config)
        specs = [
            PostgresTableSpec(
                schema_name="public",
                table_name="invoices",
                watermark_column="updated_at",
                commit_timestamp_column="updated_at",
                primary_key_columns=["id"],
                selected_columns=["id", "updated_at", "status"],
            )
        ]

        @contextmanager
        def dummy_connect():
            yield object()

        with patch.object(adapter, "validate_connection") as validate_connection, patch.object(
            adapter, "_connect", dummy_connect
        ), patch.object(adapter, "_load_table_specs", return_value=specs), patch.object(
            adapter,
            "_inspect_trigger_cdc_state",
            return_value={"installed_trigger_count": 1, "pending_event_rows": 0},
        ):
            summary = adapter.bootstrap()

        validate_connection.assert_called_once()
        self.assertEqual(summary["capture_mode"], "trigger")
        self.assertEqual(len(summary["tracked_tables"]), 1)
        self.assertEqual(summary["tracked_tables"][0]["table"], "invoices")
        self.assertEqual(summary["cdc"]["installed_trigger_count"], 1)

    def test_inspect_reports_runtime_state_for_trigger_mode(self) -> None:
        config = self.make_config()
        config.options["capture_mode"] = "trigger"
        adapter = PostgresAdapter(config)
        specs = [
            PostgresTableSpec(
                schema_name="public",
                table_name="payments",
                watermark_column="updated_at",
                commit_timestamp_column="updated_at",
                primary_key_columns=["id"],
                selected_columns=["id", "updated_at", "amount"],
            )
        ]

        @contextmanager
        def dummy_connect():
            yield object()

        with patch.object(adapter, "_connect", dummy_connect), patch.object(
            adapter, "_load_table_specs", return_value=specs
        ), patch.object(
            adapter,
            "_inspect_trigger_cdc_state",
            return_value={"installed_trigger_count": 1, "pending_event_rows": 3},
        ):
            details = adapter.inspect()

        self.assertEqual(details["capture_mode"], "trigger")
        self.assertTrue(details["supports_cdc"])
        self.assertEqual(details["tracked_tables"][0]["table"], "payments")
        self.assertEqual(details["cdc"]["pending_event_rows"], 3)

    def test_parse_test_decoding_update_change(self) -> None:
        config = self.make_config()
        config.options["capture_mode"] = "logical"
        adapter = PostgresAdapter(config)
        spec = PostgresTableSpec(
            schema_name="public",
            table_name="invoices",
            watermark_column="updated_at",
            commit_timestamp_column="updated_at",
            primary_key_columns=["id"],
            selected_columns=["id", "updated_at", "status"],
        )
        spec_map = {spec.key: spec}
        decoded = adapter._parse_test_decoding_change(
            "table public.invoices: UPDATE: old-key: id[bigint]:3001 old-tuple: id[bigint]:3001 status[text]:'draft' updated_at[timestamp with time zone]:'2026-04-17 11:00:01+00' new-tuple: id[bigint]:3001 status[text]:'issued' updated_at[timestamp with time zone]:'2026-04-17 11:00:02+00'",
            spec_map,
        )
        assert decoded is not None
        parsed_spec, operation, before_row, after_row = decoded
        self.assertEqual(parsed_spec.table_name, "invoices")
        self.assertEqual(operation, "update")
        self.assertEqual(before_row["status"], "draft")
        self.assertEqual(after_row["status"], "issued")

    def test_parse_logical_checkpoint_roundtrip(self) -> None:
        adapter = PostgresAdapter(self.make_config())
        checkpoint = SourceCheckpoint(
            source_id="pg-core-ledger",
            token=json.dumps(
                {
                    "mode": "logical_cdc",
                    "xid": "736",
                    "lsn": "0/16B6A28",
                    "commit_lsn": "0/16B6A40",
                    "event_index": 2,
                    "advance_lsn": False,
                }
            ),
            updated_at="2026-04-17T10:00:00Z",
        )
        self.assertEqual(adapter._parse_logical_checkpoint(checkpoint), "0/16B6A40")
        self.assertEqual(adapter._parse_logical_checkpoint_state(checkpoint), ("736", 2, "0/16B6A40", False))
        self.assertGreater(adapter._lsn_compare("0/16B6A29", "0/16B6A28"), 0)

    def test_logical_stream_emits_event_index_and_defers_slot_advance_inside_same_lsn(self) -> None:
        config = self.make_config()
        config.options["capture_mode"] = "logical"
        config.options["row_limit"] = 1
        adapter = PostgresAdapter(config)
        spec = PostgresTableSpec(
            schema_name="public",
            table_name="invoices",
            watermark_column="updated_at",
            commit_timestamp_column="updated_at",
            primary_key_columns=["id"],
            selected_columns=["id", "updated_at", "status"],
        )

        @contextmanager
        def dummy_connect():
            yield object()

        with patch.object(adapter, "_connect", dummy_connect), patch.object(
            adapter, "_load_table_specs", return_value=[spec]
        ), patch.object(adapter, "_ensure_logical_cdc_setup"), patch.object(
            adapter,
            "_fetch_logical_changes",
            return_value=[
                {"lsn": "0/16B6A28", "xid": "736", "data": "BEGIN"},
                {
                    "lsn": "0/16B6A28",
                    "xid": "736",
                    "data": "table public.invoices: INSERT: id[bigint]:1 status[text]:'draft' updated_at[timestamp with time zone]:'2026-04-17 12:00:01+00'",
                },
                {
                    "lsn": "0/16B6A28",
                    "xid": "736",
                    "data": "table public.invoices: INSERT: id[bigint]:2 status[text]:'issued' updated_at[timestamp with time zone]:'2026-04-17 12:00:01+00'",
                },
                {"lsn": "0/16B6A40", "xid": "736", "data": "COMMIT"},
            ],
        ):
            events = list(adapter.start_stream(None))

        self.assertEqual(len(events), 1)
        token = json.loads(events[0].checkpoint_token)
        self.assertEqual(token["xid"], "736")
        self.assertEqual(token["event_index"], 1)
        self.assertFalse(token["advance_lsn"])

    def test_runtime_signature_changes_with_table_shape(self) -> None:
        config = self.make_config()
        adapter = PostgresAdapter(config)
        spec_v1 = PostgresTableSpec(
            schema_name="public",
            table_name="invoices",
            watermark_column="updated_at",
            commit_timestamp_column="updated_at",
            primary_key_columns=["id"],
            selected_columns=["id", "updated_at", "status"],
        )
        spec_v2 = PostgresTableSpec(
            schema_name="public",
            table_name="invoices",
            watermark_column="updated_at",
            commit_timestamp_column="updated_at",
            primary_key_columns=["id"],
            selected_columns=["id", "updated_at", "status", "amount"],
        )

        @contextmanager
        def dummy_connect():
            yield object()

        with patch.object(adapter, "_connect", dummy_connect), patch.object(
            adapter, "_load_table_specs", return_value=[spec_v1]
        ):
            signature_v1 = adapter.runtime_signature()

        with patch.object(adapter, "_connect", dummy_connect), patch.object(
            adapter, "_load_table_specs", return_value=[spec_v2]
        ):
            signature_v2 = adapter.runtime_signature()

        self.assertNotEqual(signature_v1, signature_v2)

    def test_inspect_reports_publication_state_for_pgoutput(self) -> None:
        config = self.make_config()
        config.options["capture_mode"] = "logical"
        config.options["output_plugin"] = "pgoutput"
        adapter = PostgresAdapter(config)
        specs = [
            PostgresTableSpec(
                schema_name="public",
                table_name="invoices",
                watermark_column="updated_at",
                commit_timestamp_column="updated_at",
                primary_key_columns=["id"],
                selected_columns=["id", "updated_at", "status"],
            )
        ]

        @contextmanager
        def dummy_connect():
            yield object()

        with patch.object(adapter, "_connect", dummy_connect), patch.object(
            adapter, "_load_table_specs", return_value=specs
        ), patch.object(
            adapter,
            "_inspect_logical_cdc_state",
            return_value={
                "mode": "logical",
                "slot_name": "denotary_pg_core_ledger_slot",
                "plugin": "pgoutput",
                "publication_name": "denotary_pg_core_ledger_pub",
                "publication_exists": True,
                "tracked_tables": ["public.invoices"],
                "publication_tables": ["public.invoices"],
                "publication_in_sync": True,
                "pending_changes": False,
                "replica_identity_expected": "f",
                "replica_identity_by_table": {"public.invoices": "f"},
                "replica_identity_in_sync": True,
                "tracked_table_count": 1,
                "wal_level": "logical",
                "current_wal_lsn": "0/16B6A40",
                "retained_wal_bytes": 0,
                "flush_lag_bytes": 0,
                "slot_exists": True,
                "slot_active": False,
                "restart_lsn": "",
                "confirmed_flush_lsn": "",
                "wal_status": "reserved",
            },
        ):
            details = adapter.inspect()

        self.assertEqual(details["capture_mode"], "logical")
        self.assertEqual(details["cdc"]["plugin"], "pgoutput")
        self.assertEqual(details["cdc"]["publication_name"], "denotary_pg_core_ledger_pub")
        self.assertEqual(details["cdc"]["publication_tables"], ["public.invoices"])
        self.assertTrue(details["cdc"]["publication_in_sync"])
        self.assertFalse(details["cdc"]["pending_changes"])
        self.assertTrue(details["cdc"]["replica_identity_in_sync"])
        self.assertEqual(details["cdc"]["retained_wal_bytes"], 0)

    def test_wait_for_changes_detects_logical_pending_changes(self) -> None:
        config = self.make_config()
        config.options["capture_mode"] = "logical"
        config.options["logical_wait_poll_sec"] = 0.05
        adapter = PostgresAdapter(config)

        @contextmanager
        def dummy_connect():
            yield object()

        with patch.object(adapter, "_connect", dummy_connect), patch.object(
            adapter, "_logical_has_pending_changes", return_value=True
        ) as has_pending:
            changed = adapter.wait_for_changes(0.1)

        self.assertTrue(changed)
        has_pending.assert_called_once()
