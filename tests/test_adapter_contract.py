from __future__ import annotations

import unittest

from denotary_db_agent.adapters.mongodb import MongoDbAdapter
from denotary_db_agent.adapters.registry import ADAPTERS, build_adapter
from denotary_db_agent.config import SourceConfig
from denotary_db_agent.models import ChangeEvent, SourceCheckpoint


class AdapterContractTest(unittest.TestCase):
    def setUp(self) -> None:
        self.cases = [
            {
                "adapter": "mongodb",
                "class": MongoDbAdapter,
                "required_error": "mongodb connection is missing required field: uri",
                "connection": {
                    "uri": "mongodb://127.0.0.1:27017",
                },
                "minimum_version": "6.0",
                "notes": "change streams",
                "capture_modes": ("snapshot", "change_streams"),
                "bootstrap_requirements": ("replica set or sharded cluster", "tracked collections visible"),
            },
        ]

    def _source_config(self, adapter: str, connection: dict[str, object]) -> SourceConfig:
        return SourceConfig(
            id=f"{adapter}-source",
            adapter=adapter,
            enabled=True,
            source_instance="test-instance",
            database_name="ledger",
            include={"public": ["invoices"]},
            exclude={},
            connection=connection,
            options={"capture_mode": "cdc"},
        )

    def _sample_event(self, adapter: str, checkpoint_token: str = "token-1") -> ChangeEvent:
        return ChangeEvent(
            source_id=f"{adapter}-source",
            source_type=adapter,
            source_instance="test-instance",
            database_name="ledger",
            schema_or_namespace="public",
            table_or_collection="invoices",
            operation="update",
            primary_key={"id": 1},
            change_version="change-1",
            commit_timestamp="2026-04-18T10:00:00Z",
            after={"id": 1, "status": "issued"},
            before={"id": 1, "status": "draft"},
            metadata={"txid": 100},
            checkpoint_token=checkpoint_token,
        )

    def test_registry_contains_scaffold_adapters(self) -> None:
        for case in self.cases:
            with self.subTest(adapter=case["adapter"]):
                self.assertIn(case["adapter"], ADAPTERS)
                adapter = build_adapter(self._source_config(case["adapter"], case["connection"]))
                self.assertIsInstance(adapter, case["class"])

    def test_scaffold_adapters_expose_expected_capabilities(self) -> None:
        for case in self.cases:
            with self.subTest(adapter=case["adapter"]):
                adapter = case["class"](self._source_config(case["adapter"], case["connection"]))
                capabilities = adapter.discover_capabilities()
                self.assertEqual(capabilities.source_type, case["adapter"])
                self.assertEqual(capabilities.minimum_version, case["minimum_version"])
                self.assertTrue(capabilities.supports_cdc)
                self.assertTrue(capabilities.supports_snapshot)
                self.assertEqual(capabilities.operations, ("insert", "update", "delete"))
                self.assertEqual(capabilities.capture_modes, case["capture_modes"])
                self.assertEqual(capabilities.bootstrap_requirements, case["bootstrap_requirements"])
                self.assertIn(case["notes"], capabilities.notes)

    def test_scaffold_adapters_validate_required_connection_fields(self) -> None:
        for case in self.cases:
            with self.subTest(adapter=case["adapter"]):
                adapter = case["class"](self._source_config(case["adapter"], {}))
                with self.assertRaisesRegex(ValueError, case["required_error"]):
                    adapter.validate_connection()

                adapter = case["class"](self._source_config(case["adapter"], case["connection"]))
                adapter.validate_connection()

    def test_scaffold_adapters_support_bootstrap_and_inspect(self) -> None:
        for case in self.cases:
            with self.subTest(adapter=case["adapter"]):
                adapter = case["class"](self._source_config(case["adapter"], case["connection"]))
                bootstrap = adapter.bootstrap()
                inspect = adapter.inspect()
                self.assertEqual(bootstrap["source_id"], f"{case['adapter']}-source")
                self.assertEqual(bootstrap["adapter"], case["adapter"])
                self.assertEqual(bootstrap["bootstrap"], "validated")
                self.assertEqual(inspect["source_type"], case["adapter"])
                self.assertEqual(inspect["operations"], ["insert", "update", "delete"])
                self.assertEqual(inspect["capture_modes"], list(case["capture_modes"]))
                self.assertEqual(inspect["bootstrap_requirements"], list(case["bootstrap_requirements"]))

    def test_scaffold_adapters_return_empty_snapshot_and_noop_checkpoint_resume(self) -> None:
        checkpoint = SourceCheckpoint(
            source_id="source-1",
            token="checkpoint-1",
            updated_at="2026-04-18T10:00:00Z",
        )
        for case in self.cases:
            with self.subTest(adapter=case["adapter"]):
                adapter = case["class"](self._source_config(case["adapter"], case["connection"]))
                self.assertEqual(list(adapter.read_snapshot(checkpoint)), [])
                self.assertIsNone(adapter.resume_from_checkpoint(checkpoint))
                self.assertIsNone(adapter.stop_stream())

    def test_scaffold_adapters_raise_not_implemented_for_cdc_stream(self) -> None:
        checkpoint = SourceCheckpoint(
            source_id="source-1",
            token="checkpoint-1",
            updated_at="2026-04-18T10:00:00Z",
        )
        for case in self.cases:
            with self.subTest(adapter=case["adapter"]):
                adapter = case["class"](self._source_config(case["adapter"], case["connection"]))
                with self.assertRaisesRegex(NotImplementedError, "not implemented in the scaffold yet"):
                    list(adapter.start_stream(checkpoint))

    def test_scaffold_adapters_serialize_checkpoint_prefers_checkpoint_token(self) -> None:
        for case in self.cases:
            with self.subTest(adapter=case["adapter"]):
                adapter = case["class"](self._source_config(case["adapter"], case["connection"]))
                self.assertEqual(adapter.serialize_checkpoint(self._sample_event(case["adapter"])), "token-1")
                self.assertEqual(
                    adapter.serialize_checkpoint(self._sample_event(case["adapter"], checkpoint_token="")),
                    "change-1",
                )
