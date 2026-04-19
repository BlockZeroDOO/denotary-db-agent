from __future__ import annotations

import unittest
from unittest.mock import patch
from datetime import datetime, timezone

from denotary_db_agent.adapters.mongodb import MongoDbAdapter
from denotary_db_agent.config import SourceConfig
from denotary_db_agent.models import SourceCheckpoint


class FakeCursor:
    def __init__(self, documents: list[dict[str, object]]):
        self._documents = documents

    def sort(self, fields):
        self._documents = sorted(
            self._documents,
            key=lambda document: tuple(document[field] for field, _direction in fields),
        )
        return self

    def limit(self, size: int):
        self._documents = self._documents[:size]
        return self

    def __iter__(self):
        return iter(self._documents)


class FakeCollection:
    def __init__(self, documents: list[dict[str, object]]):
        self.documents = documents
        self.find_calls: list[dict[str, object]] = []
        self.watch_calls: list[tuple[list[dict[str, object]], dict[str, object]]] = []
        self.change_events: list[dict[str, object]] = []
        self.streams: list[object] = []

    def find(self, query: dict[str, object]):
        self.find_calls.append(query)
        filtered = [document for document in self.documents if self._matches(document, query)]
        return FakeCursor(filtered)

    def watch(self, pipeline: list[dict[str, object]], **kwargs: object):
        self.watch_calls.append((pipeline, kwargs))
        if self.streams:
            return self.streams.pop(0)
        return FakeChangeStream(list(self.change_events))

    def _matches(self, document: dict[str, object], query: dict[str, object]) -> bool:
        if "$and" in query:
            return all(self._matches(document, item) for item in query["$and"])
        if "$or" in query:
            return any(self._matches(document, item) for item in query["$or"])
        for field, value in query.items():
            if isinstance(value, dict):
                candidate = document.get(field)
                if "$exists" in value:
                    exists = field in document
                    if bool(value["$exists"]) != exists:
                        return False
                if "$ne" in value and candidate == value["$ne"]:
                    return False
                if "$gt" in value and not (candidate > value["$gt"]):
                    return False
            elif document.get(field) != value:
                return False
        return True


class FakeDatabase:
    def __init__(self, collections: dict[str, FakeCollection]):
        self.collections = collections

    def list_collection_names(self):
        return list(self.collections.keys())

    def __getitem__(self, name: str) -> FakeCollection:
        return self.collections[name]


class FakeAdmin:
    def __init__(self):
        self.commands: list[str] = []

    def command(self, name: str):
        self.commands.append(name)
        if name == "hello":
            return {"ok": 1, "setName": "rs0"}
        return {"ok": 1}


class FakeMongoClient:
    def __init__(self, databases: dict[str, FakeDatabase]):
        self.databases = databases
        self.admin = FakeAdmin()
        self.closed = False

    def __getitem__(self, name: str) -> FakeDatabase:
        return self.databases[name]

    def close(self) -> None:
        self.closed = True


class FakeChangeStream:
    def __init__(self, events: list[dict[str, object]]):
        self.events = events
        self.closed = False

    def try_next(self):
        if not self.events:
            return None
        return self.events.pop(0)

    def close(self):
        self.closed = True


class BrokenChangeStream(FakeChangeStream):
    def __init__(self, error: Exception):
        super().__init__([])
        self.error = error

    def try_next(self):
        raise self.error


class MongoDbAdapterTest(unittest.TestCase):
    def setUp(self) -> None:
        self.config = SourceConfig(
            id="mongodb-core-ledger",
            adapter="mongodb",
            enabled=True,
            source_instance="erp-eu-1",
            database_name="ledger",
            backfill_mode="full",
            include={"ledger": ["invoices"]},
            connection={
                "uri": "mongodb://127.0.0.1:27017",
            },
            options={
                "capture_mode": "watermark",
                "watermark_column": "updated_at",
                "commit_timestamp_column": "updated_at",
                "row_limit": 100,
            },
        )

    def test_discover_capabilities_describes_watermark_baseline(self) -> None:
        adapter = MongoDbAdapter(self.config)
        capabilities = adapter.discover_capabilities()

        self.assertEqual(capabilities.source_type, "mongodb")
        self.assertEqual(capabilities.minimum_version, "6.0")
        self.assertTrue(capabilities.supports_cdc)
        self.assertTrue(capabilities.supports_snapshot)
        self.assertEqual(capabilities.operations, ("insert", "update", "delete", "snapshot"))
        self.assertEqual(capabilities.capture_modes, ("watermark", "change_streams"))
        self.assertEqual(
            capabilities.bootstrap_requirements,
            ("tracked collections visible", "watermark fields configured"),
        )
        self.assertIn("native CDC path", capabilities.notes)

    def test_validate_connection_rejects_missing_uri(self) -> None:
        config = SourceConfig(
            id="mongodb-core-ledger",
            adapter="mongodb",
            enabled=True,
            source_instance="erp-eu-1",
            database_name="ledger",
            connection={},
        )
        adapter = MongoDbAdapter(config)

        with self.assertRaisesRegex(ValueError, "mongodb connection is missing required field: uri"):
            adapter.validate_connection()

    def test_bootstrap_and_inspect_report_tracked_collections(self) -> None:
        adapter = MongoDbAdapter(self.config)
        client = FakeMongoClient(
            {
                "ledger": FakeDatabase(
                    {
                        "invoices": FakeCollection([]),
                    }
                )
            }
        )

        with patch("denotary_db_agent.adapters.mongodb.pymongo", object()), patch.object(adapter, "_connect") as connect:
            connect.return_value.__enter__.return_value = client
            connect.return_value.__exit__.return_value = False
            bootstrap = adapter.bootstrap()

        self.assertEqual(bootstrap["capture_mode"], "watermark")
        self.assertEqual(len(bootstrap["tracked_collections"]), 1)
        self.assertEqual(bootstrap["tracked_collections"][0]["collection_name"], "invoices")
        self.assertEqual(client.admin.commands, ["ping"])

        with patch.object(adapter, "_connect") as connect:
            connect.return_value.__enter__.return_value = client
            connect.return_value.__exit__.return_value = False
            inspect = adapter.inspect()

        self.assertEqual(inspect["capture_modes"], ["watermark", "change_streams"])
        self.assertEqual(inspect["tracked_collections"][0]["primary_key_field"], "_id")
        self.assertIsNone(inspect["cdc"])

    def test_read_snapshot_emits_documents_and_checkpoint_state(self) -> None:
        adapter = MongoDbAdapter(self.config)
        collection = FakeCollection(
            [
                {
                    "_id": "doc-1",
                    "status": "issued",
                    "updated_at": "2026-04-18T10:00:00Z",
                },
                {
                    "_id": "doc-2",
                    "status": "paid",
                    "updated_at": "2026-04-18T10:05:00Z",
                },
            ]
        )
        client = FakeMongoClient({"ledger": FakeDatabase({"invoices": collection})})

        with patch.object(adapter, "_connect") as connect:
            connect.return_value.__enter__.return_value = client
            connect.return_value.__exit__.return_value = False
            events = list(adapter.read_snapshot())

        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].operation, "snapshot")
        self.assertEqual(events[0].primary_key, {"_id": "doc-1"})
        self.assertEqual(events[0].after["status"], "issued")
        self.assertIn('"ledger.invoices"', events[-1].checkpoint_token)

    def test_read_snapshot_respects_checkpoint_resume(self) -> None:
        adapter = MongoDbAdapter(self.config)
        checkpoint = SourceCheckpoint(
            source_id=self.config.id,
            token='{"ledger.invoices":{"watermark":"2026-04-18T10:00:00Z","watermark_type":"str","pk":"doc-1","pk_type":"str"}}',
            updated_at="2026-04-18T10:01:00Z",
        )
        collection = FakeCollection(
            [
                {
                    "_id": "doc-1",
                    "status": "issued",
                    "updated_at": "2026-04-18T10:00:00Z",
                },
                {
                    "_id": "doc-2",
                    "status": "paid",
                    "updated_at": "2026-04-18T10:00:00Z",
                },
                {
                    "_id": "doc-3",
                    "status": "settled",
                    "updated_at": "2026-04-18T10:05:00Z",
                },
            ]
        )
        client = FakeMongoClient({"ledger": FakeDatabase({"invoices": collection})})

        with patch.object(adapter, "_connect") as connect:
            connect.return_value.__enter__.return_value = client
            connect.return_value.__exit__.return_value = False
            events = list(adapter.read_snapshot(checkpoint))

        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].primary_key, {"_id": "doc-2"})
        self.assertIn("$or", collection.find_calls[-1]["$and"][1])

    def test_validate_connection_requires_replica_set_for_change_streams(self) -> None:
        config = SourceConfig(
            id="mongodb-core-ledger",
            adapter="mongodb",
            enabled=True,
            source_instance="erp-eu-1",
            database_name="ledger",
            include={"ledger": ["invoices"]},
            connection={"uri": "mongodb://127.0.0.1:27017"},
            options={"capture_mode": "change_streams"},
        )
        adapter = MongoDbAdapter(config)
        client = FakeMongoClient({"ledger": FakeDatabase({"invoices": FakeCollection([])})})
        client.admin.command = lambda name: {"ok": 1} if name == "hello" else {"ok": 1}

        with patch("denotary_db_agent.adapters.mongodb.pymongo", object()), patch.object(adapter, "_connect") as connect:
            connect.return_value.__enter__.return_value = client
            connect.return_value.__exit__.return_value = False
            with self.assertRaisesRegex(ValueError, "require a replica set or sharded deployment"):
                adapter.validate_connection()

    def test_start_stream_emits_change_stream_events(self) -> None:
        config = SourceConfig(
            id="mongodb-core-ledger",
            adapter="mongodb",
            enabled=True,
            source_instance="erp-eu-1",
            database_name="ledger",
            include={"ledger": ["invoices"]},
            connection={"uri": "mongodb://127.0.0.1:27017"},
            options={"capture_mode": "change_streams"},
        )
        adapter = MongoDbAdapter(config)
        collection = FakeCollection([])
        collection.change_events = [
            {
                "_id": {"_data": "token-1"},
                "operationType": "insert",
                "clusterTime": datetime(2026, 4, 18, 10, 0, 0, tzinfo=timezone.utc),
                "documentKey": {"_id": "doc-1"},
                "fullDocument": {"_id": "doc-1", "status": "issued"},
                "ns": {"db": "ledger", "coll": "invoices"},
            },
            {
                "_id": {"_data": "token-2"},
                "operationType": "delete",
                "clusterTime": datetime(2026, 4, 18, 10, 0, 1, tzinfo=timezone.utc),
                "documentKey": {"_id": "doc-1"},
                "ns": {"db": "ledger", "coll": "invoices"},
            },
        ]
        client = FakeMongoClient({"ledger": FakeDatabase({"invoices": collection})})

        with patch("denotary_db_agent.adapters.mongodb.pymongo", object()), patch.object(adapter, "_ensure_stream_client", return_value=client):
            events = list(adapter.start_stream(None))

        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].operation, "insert")
        self.assertEqual(events[0].primary_key, {"_id": "doc-1"})
        self.assertEqual(events[0].after["status"], "issued")
        self.assertEqual(events[1].operation, "delete")
        self.assertIsNone(events[1].after)
        self.assertEqual(collection.watch_calls[0][1]["full_document"], "updateLookup")

    def test_start_stream_reopens_stream_after_recoverable_error(self) -> None:
        config = SourceConfig(
            id="mongodb-core-ledger",
            adapter="mongodb",
            enabled=True,
            source_instance="erp-eu-1",
            database_name="ledger",
            include={"ledger": ["invoices"]},
            connection={"uri": "mongodb://127.0.0.1:27017"},
            options={"capture_mode": "change_streams"},
        )
        adapter = MongoDbAdapter(config)
        collection = FakeCollection([])
        collection.streams = [
            BrokenChangeStream(RuntimeError("stream lost after restart")),
            FakeChangeStream(
                [
                    {
                        "_id": {"_data": "token-2"},
                        "operationType": "insert",
                        "clusterTime": datetime(2026, 4, 18, 10, 0, 2, tzinfo=timezone.utc),
                        "documentKey": {"_id": "doc-2"},
                        "fullDocument": {"_id": "doc-2", "status": "paid"},
                        "ns": {"db": "ledger", "coll": "invoices"},
                    }
                ]
            ),
        ]
        client = FakeMongoClient({"ledger": FakeDatabase({"invoices": collection})})

        with patch("denotary_db_agent.adapters.mongodb.pymongo", object()), patch.object(
            adapter, "_ensure_stream_client", return_value=client
        ), patch.object(adapter, "_stream_error_types", return_value=(RuntimeError,)):
            events = list(adapter.start_stream(None))

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].primary_key, {"_id": "doc-2"})
        self.assertEqual(len(collection.watch_calls), 2)


if __name__ == "__main__":
    unittest.main()
