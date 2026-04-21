from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from denotary_db_agent.adapters.elasticsearch import ElasticsearchAdapter
from denotary_db_agent.config import SourceConfig
from denotary_db_agent.models import SourceCheckpoint


class FakeIndicesClient:
    def __init__(self, mappings: dict[str, dict[str, object]]):
        self._mappings = mappings

    def get_mapping(self, index: str):
        return self._mappings[index]


class FakeClient:
    def __init__(self, mappings: dict[str, dict[str, object]], search_results: list[dict[str, object]] | None = None):
        self.indices = FakeIndicesClient(mappings)
        self._search_results = search_results or []
        self.search_calls: list[tuple[str, dict[str, object]]] = []
        self.closed = False

    def info(self):
        return {"version": {"number": "8.15.0"}}

    def search(self, *, index: str, **kwargs):
        self.search_calls.append((index, kwargs))
        return self._search_results.pop(0)

    def close(self):
        self.closed = True


def build_source_config(connection: dict[str, object] | None = None, options: dict[str, object] | None = None) -> SourceConfig:
    resolved_connection = (
        {
            "url": "http://127.0.0.1:9200",
        }
        if connection is None
        else connection
    )
    return SourceConfig(
        id="elasticsearch-source",
        adapter="elasticsearch",
        enabled=True,
        source_instance="search-eu-1",
        database_name="search",
        backfill_mode="full",
        include={"default": ["orders"]},
        connection=resolved_connection,
        options=options or {"capture_mode": "watermark"},
    )


class ElasticsearchAdapterTest(unittest.TestCase):
    def test_capabilities_declare_wave_two_snapshot_baseline(self) -> None:
        adapter = ElasticsearchAdapter(build_source_config())
        capabilities = adapter.discover_capabilities()

        self.assertEqual(capabilities.source_type, "elasticsearch")
        self.assertFalse(capabilities.supports_cdc)
        self.assertTrue(capabilities.supports_snapshot)
        self.assertEqual(capabilities.operations, ("snapshot",))
        self.assertEqual(capabilities.capture_modes, ("watermark",))
        self.assertEqual(capabilities.default_capture_mode, "watermark")
        self.assertEqual(capabilities.checkpoint_strategy, "document_watermark")
        self.assertEqual(capabilities.activity_model, "polling")
        self.assertIn("Wave 2", capabilities.notes)

    def test_validate_connection_requires_url_or_hosts(self) -> None:
        adapter = ElasticsearchAdapter(build_source_config(connection={}))
        with self.assertRaisesRegex(ValueError, "url, host, or hosts"):
            adapter.validate_connection()

    def test_validate_connection_requires_driver_for_live_use(self) -> None:
        adapter = ElasticsearchAdapter(build_source_config())
        with patch("denotary_db_agent.adapters.elasticsearch.Elasticsearch", None):
            with self.assertRaisesRegex(RuntimeError, "elasticsearch is required"):
                adapter.validate_connection()

    def test_bootstrap_and_inspect_report_live_tracked_objects(self) -> None:
        adapter = ElasticsearchAdapter(build_source_config(options={"capture_mode": "watermark", "primary_key_field": "_id"}))
        client = FakeClient(
            {
                "orders": {
                    "orders": {
                        "mappings": {
                            "properties": {
                                "status": {"type": "keyword"},
                                "updated_at": {"type": "date"},
                            }
                        }
                    }
                }
            }
        )
        with patch("denotary_db_agent.adapters.elasticsearch.Elasticsearch", object()), patch.object(adapter, "_client", return_value=client):
            bootstrap = adapter.bootstrap()

        self.assertEqual(bootstrap["capture_mode"], "watermark")
        self.assertEqual(bootstrap["tracked_tables"][0]["table_name"], "orders")
        self.assertIn("updated_at", bootstrap["tracked_tables"][0]["selected_columns"])
        self.assertEqual(bootstrap["tracked_tables"][0]["primary_key_columns"], ["_id"])
        self.assertEqual(bootstrap["cdc"]["runtime"]["transport"], "polling")

        client = FakeClient(
            {
                "orders": {
                    "orders": {
                        "mappings": {
                            "properties": {
                                "status": {"type": "keyword"},
                                "updated_at": {"type": "date"},
                            }
                        }
                    }
                }
            }
        )
        with patch("denotary_db_agent.adapters.elasticsearch.Elasticsearch", object()), patch.object(adapter, "_client", return_value=client):
            inspect_payload = adapter.inspect()

        self.assertEqual(inspect_payload["source_type"], "elasticsearch")
        self.assertEqual(inspect_payload["default_capture_mode"], "watermark")
        self.assertEqual(inspect_payload["checkpoint_strategy"], "document_watermark")
        self.assertEqual(inspect_payload["activity_model"], "polling")

    def test_runtime_signature_is_deterministic_for_configured_indices(self) -> None:
        adapter = ElasticsearchAdapter(build_source_config(options={"capture_mode": "watermark", "dry_run_events": []}))
        signature = json.loads(adapter.runtime_signature())

        self.assertEqual(signature["adapter"], "elasticsearch")
        self.assertEqual(signature["capture_mode"], "watermark")
        self.assertEqual(signature["tracked_tables"][0]["key"], "default.orders")

    def test_read_snapshot_replays_dry_run_events(self) -> None:
        adapter = ElasticsearchAdapter(
            build_source_config(
                options={
                    "capture_mode": "watermark",
                    "dry_run_events": [
                        {
                            "schema_or_namespace": "default",
                            "table_or_collection": "orders",
                            "operation": "snapshot",
                            "primary_key": {"_id": "1"},
                            "change_version": "default.orders:2026-04-20T08:00:00Z:1",
                            "commit_timestamp": "2026-04-20T08:00:00Z",
                            "after": {"_id": "1", "status": "issued"},
                            "checkpoint_token": '{"default.orders":{"watermark":"2026-04-20T08:00:00Z","pk":"1"}}',
                        }
                    ],
                }
            )
        )

        events = list(adapter.read_snapshot(SourceCheckpoint(source_id="elasticsearch-source", token="{}", updated_at="2026-04-20T08:00:01Z")))
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].source_type, "elasticsearch")
        self.assertEqual(events[0].primary_key, {"_id": "1"})
        self.assertEqual(events[0].after["status"], "issued")

    def test_read_snapshot_emits_documents_and_checkpoint_state(self) -> None:
        adapter = ElasticsearchAdapter(
            build_source_config(
                options={
                    "capture_mode": "watermark",
                    "watermark_field": "updated_at",
                    "commit_timestamp_field": "updated_at",
                    "primary_key_field": "_id",
                    "row_limit": 100,
                }
            )
        )
        client = FakeClient(
            {
                "orders": {
                    "orders": {
                        "mappings": {
                            "properties": {
                                "status": {"type": "keyword"},
                                "updated_at": {"type": "date"},
                            }
                        }
                    }
                }
            },
            search_results=[
                {
                    "hits": {
                        "hits": [
                            {"_id": "1", "_source": {"status": "issued", "updated_at": "2026-04-20T08:00:00Z"}},
                            {"_id": "2", "_source": {"status": "paid", "updated_at": "2026-04-20T08:05:00Z"}},
                        ]
                    }
                }
            ],
        )
        with patch.object(adapter, "_client", return_value=client):
            events = list(adapter.read_snapshot())

        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].operation, "snapshot")
        self.assertEqual(events[0].primary_key, {"_id": "1"})
        self.assertIn('"default.orders"', events[-1].checkpoint_token)

    def test_read_snapshot_respects_checkpoint_resume(self) -> None:
        adapter = ElasticsearchAdapter(
            build_source_config(
                options={
                    "capture_mode": "watermark",
                    "watermark_field": "updated_at",
                    "commit_timestamp_field": "updated_at",
                    "primary_key_field": "_id",
                    "row_limit": 100,
                }
            )
        )
        checkpoint = SourceCheckpoint(
            source_id="elasticsearch-source",
            token='{"default.orders":{"watermark":"2026-04-20T08:00:00Z","pk":"1"}}',
            updated_at="2026-04-20T08:01:00Z",
        )
        client = FakeClient(
            {
                "orders": {
                    "orders": {
                        "mappings": {
                            "properties": {
                                "status": {"type": "keyword"},
                                "updated_at": {"type": "date"},
                            }
                        }
                    }
                }
            },
            search_results=[
                {
                    "hits": {
                        "hits": [
                            {"_id": "1", "_source": {"status": "issued", "updated_at": "2026-04-20T08:00:00Z"}},
                            {"_id": "2", "_source": {"status": "paid", "updated_at": "2026-04-20T08:05:00Z"}},
                        ]
                    }
                }
            ],
        )
        with patch.object(adapter, "_client", return_value=client):
            events = list(adapter.read_snapshot(checkpoint))

        self.assertEqual(len(events), 1)
        index_name, kwargs = client.search_calls[-1]
        self.assertEqual(index_name, "orders")
        self.assertEqual(kwargs["query"]["bool"]["filter"][1]["range"]["updated_at"]["gte"], "2026-04-20T08:00:00Z")


if __name__ == "__main__":
    unittest.main()
