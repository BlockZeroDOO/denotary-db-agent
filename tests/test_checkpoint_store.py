from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from denotary_db_agent.checkpoint_store import CheckpointStore
from denotary_db_agent.models import DeliveryAttempt


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

