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

