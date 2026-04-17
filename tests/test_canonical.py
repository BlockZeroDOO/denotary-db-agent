from __future__ import annotations

import unittest

from denotary_db_agent.canonical import build_external_ref, canonicalize_event
from denotary_db_agent.models import ChangeEvent


def sample_event() -> ChangeEvent:
    return ChangeEvent(
        source_id="pg-core-ledger",
        source_type="postgresql",
        source_instance="erp-eu-1",
        database_name="ledger",
        schema_or_namespace="public",
        table_or_collection="invoices",
        operation="update",
        primary_key={"id": 1001},
        change_version="lsn:10/20",
        commit_timestamp="2026-04-17T10:11:12Z",
        before={"status": "draft", "amount": "100.00"},
        after={"status": "issued", "amount": "100.00"},
        metadata={"txid": 44},
        checkpoint_token="lsn:10/20",
    )


class CanonicalizationTest(unittest.TestCase):
    def test_external_ref_is_stable(self) -> None:
        event_a = sample_event()
        event_b = sample_event()
        self.assertEqual(build_external_ref(event_a), build_external_ref(event_b))

    def test_canonicalization_is_deterministic(self) -> None:
        env_a = canonicalize_event(sample_event())
        env_b = canonicalize_event(sample_event())
        self.assertEqual(env_a.after_hash, env_b.after_hash)
        self.assertEqual(env_a.before_hash, env_b.before_hash)
        self.assertEqual(env_a.metadata_hash, env_b.metadata_hash)
        self.assertEqual(env_a.external_ref, env_b.external_ref)

    def test_single_prepare_payload_uses_payload_field(self) -> None:
        envelope = canonicalize_event(sample_event())

        payload = envelope.to_prepare_payload("verification", 123, 456)

        self.assertIn("payload", payload)
        self.assertNotIn("document", payload)
        self.assertEqual(payload["payload"]["table_or_collection"], "invoices")
