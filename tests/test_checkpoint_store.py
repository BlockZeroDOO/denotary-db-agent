from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from denotary_db_agent.checkpoint_store import CheckpointStore
from denotary_db_agent.models import DeliveryAttempt, ProofArtifact


class CheckpointStoreTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.store = CheckpointStore(str(Path(self.temp_dir.name) / "state.sqlite3"))

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_checkpoint_roundtrip(self) -> None:
        self.store.set_checkpoint("source-1", "token-1", "2026-04-17T10:00:00Z")
        checkpoint = self.store.get_checkpoint("source-1")
        self.assertIsNotNone(checkpoint)
        assert checkpoint is not None
        self.assertEqual(checkpoint.token, "token-1")

    def test_delivery_roundtrip(self) -> None:
        self.store.upsert_delivery(
            DeliveryAttempt(
                request_id="req-1",
                trace_id="trace-1",
                source_id="source-1",
                external_ref="ext-1",
                tx_id=None,
                status="prepared_registered",
                prepared_action={"account": "verifbill"},
                last_error=None,
                updated_at="2026-04-17T10:00:00Z",
            )
        )
        rows = self.store.list_deliveries("source-1")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["status"], "prepared_registered")

    def test_source_pause_roundtrip(self) -> None:
        self.assertFalse(self.store.is_source_paused("source-1"))
        self.store.set_source_paused("source-1", True, "2026-04-17T10:00:00Z")
        self.assertTrue(self.store.is_source_paused("source-1"))
        controls = self.store.list_source_controls()
        self.assertEqual(len(controls), 1)
        self.assertEqual(int(controls[0]["paused"]), 1)
        self.store.set_source_paused("source-1", False, "2026-04-17T10:10:00Z")
        self.assertFalse(self.store.is_source_paused("source-1"))

    def test_runtime_signature_roundtrip(self) -> None:
        self.assertIsNone(self.store.get_runtime_signature("source-1"))
        self.store.set_runtime_signature("source-1", "sig-1", "2026-04-17T10:00:00Z")
        self.assertEqual(self.store.get_runtime_signature("source-1"), "sig-1")
        rows = self.store.list_runtime_signatures()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["signature"], "sig-1")

    def test_prune_deliveries_keeps_newest_rows(self) -> None:
        for index in range(3):
            self.store.upsert_delivery(
                DeliveryAttempt(
                    request_id=f"req-{index}",
                    trace_id=f"trace-{index}",
                    source_id="source-1",
                    external_ref=f"ext-{index}",
                    tx_id=None,
                    status="prepared_registered",
                    prepared_action=None,
                    last_error=None,
                    updated_at=f"2026-04-17T10:00:0{index}Z",
                )
            )

        removed = self.store.prune_deliveries("source-1", 2)
        rows = self.store.list_deliveries("source-1")

        self.assertEqual(removed, 1)
        self.assertEqual([row["request_id"] for row in rows], ["req-2", "req-1"])

    def test_prune_proofs_keeps_newest_rows_and_returns_removed_paths(self) -> None:
        for index in range(3):
            self.store.upsert_proof(
                ProofArtifact(
                    request_id=f"req-{index}",
                    source_id="source-1",
                    receipt={"index": index},
                    audit_chain={"index": index},
                    export_path=f"proof-{index}.json",
                    updated_at=f"2026-04-17T10:00:0{index}Z",
                )
            )

        removed = self.store.prune_proofs("source-1", 2)
        rows = self.store.list_proofs("source-1")

        self.assertEqual(len(removed), 1)
        self.assertEqual(removed[0]["request_id"], "req-0")
        self.assertEqual([row["request_id"] for row in rows], ["req-2", "req-1"])

    def test_prune_dlq_keeps_newest_rows(self) -> None:
        for index in range(3):
            self.store.push_dlq(
                "source-1",
                f"error-{index}",
                {"index": index},
                f"2026-04-17T10:00:0{index}Z",
            )

        removed = self.store.prune_dlq("source-1", 2)
        rows = self.store.list_dlq("source-1")

        self.assertEqual(removed, 1)
        self.assertEqual([row["reason"] for row in rows], ["error-2", "error-1"])
