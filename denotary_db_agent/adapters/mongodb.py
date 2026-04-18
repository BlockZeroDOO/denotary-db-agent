from __future__ import annotations

import json
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal
from typing import Any, Iterator

from denotary_db_agent.adapters.base import AdapterCapabilities, BaseAdapter
from denotary_db_agent.models import ChangeEvent, SourceCheckpoint

try:
    from bson import Binary, Decimal128, ObjectId
    import pymongo
except ImportError:  # pragma: no cover - handled in runtime validation
    Binary = None
    Decimal128 = None
    ObjectId = None
    pymongo = None


@dataclass
class MongoCollectionSpec:
    database_name: str
    collection_name: str
    watermark_column: str
    commit_timestamp_column: str
    primary_key_field: str

    @property
    def key(self) -> str:
        return f"{self.database_name}.{self.collection_name}"


class MongoDbAdapter(BaseAdapter):
    source_type = "mongodb"
    minimum_version = "6.0"
    adapter_notes = (
        "Live baseline implementation uses MongoDB watermark-based snapshot polling over tracked collections. "
        "MongoDB change streams remain the next MongoDB-specific step."
    )

    def discover_capabilities(self) -> AdapterCapabilities:
        return AdapterCapabilities(
            source_type=self.source_type,
            minimum_version=self.minimum_version,
            supports_cdc=False,
            supports_snapshot=True,
            operations=("snapshot",),
            capture_modes=("watermark",),
            bootstrap_requirements=("tracked collections visible", "watermark fields configured"),
            notes=self.adapter_notes,
        )

    def validate_connection(self) -> None:
        uri = self.config.connection.get("uri")
        if not uri:
            raise ValueError(f"{self.source_type} connection is missing required field: uri")
        if pymongo is None:
            raise RuntimeError("pymongo is required for live mongodb adapter use")
        if self.config.options.get("dry_run_events"):
            return
        with self._connect() as client:
            client.admin.command("ping")
            self._load_collection_specs(client)

    def bootstrap(self) -> dict:
        if self.config.options.get("dry_run_events"):
            summary = super().bootstrap()
            summary.update(
                {
                    "capture_mode": self._capture_mode(),
                    "tracked_collections": [],
                    "cdc": None,
                }
            )
            return summary
        self.validate_connection()
        with self._connect() as client:
            specs = self._load_collection_specs(client)
        return {
            "source_id": self.config.id,
            "adapter": self.config.adapter,
            "capture_mode": self._capture_mode(),
            "tracked_collections": [self._spec_summary(spec) for spec in specs],
            "cdc": None,
        }

    def inspect(self) -> dict:
        if self.config.options.get("dry_run_events"):
            details = super().inspect()
            details.update(
                {
                    "capture_mode": self._capture_mode(),
                    "tracked_collections": [],
                    "cdc": None,
                }
            )
            return details
        capabilities = self.discover_capabilities()
        with self._connect() as client:
            specs = self._load_collection_specs(client)
        return {
            "source_id": self.config.id,
            "adapter": self.config.adapter,
            "source_type": capabilities.source_type,
            "capture_mode": self._capture_mode(),
            "supports_cdc": capabilities.supports_cdc,
            "supports_snapshot": capabilities.supports_snapshot,
            "operations": list(capabilities.operations),
            "capture_modes": list(capabilities.capture_modes),
            "bootstrap_requirements": list(capabilities.bootstrap_requirements),
            "tracked_collections": [self._spec_summary(spec) for spec in specs],
            "cdc": None,
            "notes": capabilities.notes,
        }

    def runtime_signature(self) -> str:
        if self.config.options.get("dry_run_events"):
            return super().runtime_signature()
        with self._connect() as client:
            specs = self._load_collection_specs(client)
        payload = {
            "adapter": self.config.adapter,
            "source_id": self.config.id,
            "capture_mode": self._capture_mode(),
            "tracked_collections": [self._spec_signature_entry(spec) for spec in specs],
            "include": self.config.include,
        }
        return json.dumps(payload, sort_keys=True)

    def refresh_runtime(self) -> dict:
        return self.bootstrap()

    def start_stream(self, checkpoint: SourceCheckpoint | None):
        raise NotImplementedError("mongodb change streams are not implemented yet; use watermark snapshot mode")

    def stop_stream(self) -> None:
        return None

    def read_snapshot(self, checkpoint: SourceCheckpoint | None = None):
        dry_events = self.config.options.get("dry_run_events") or []
        for item in dry_events:
            yield ChangeEvent(
                source_id=self.config.id,
                source_type=self.source_type,
                source_instance=self.config.source_instance,
                database_name=str(item.get("database_name", self.config.database_name)),
                schema_or_namespace=str(item.get("schema_or_namespace", item.get("database_name", self.config.database_name))),
                table_or_collection=str(item.get("table_or_collection", "records")),
                operation=str(item.get("operation", "snapshot")),  # type: ignore[arg-type]
                primary_key=dict(item.get("primary_key") or {}),
                change_version=str(item.get("change_version", item.get("checkpoint_token", "0"))),
                commit_timestamp=str(item.get("commit_timestamp", "1970-01-01T00:00:00Z")),
                before=item.get("before"),
                after=item.get("after") or {},
                metadata=dict(item.get("metadata") or {}),
                checkpoint_token=str(item.get("checkpoint_token", item.get("change_version", "0"))),
            )
        if dry_events:
            return

        checkpoint_state = self._parse_checkpoint(checkpoint)
        if checkpoint is None and self.config.backfill_mode == "none":
            return

        with self._connect() as client:
            specs = self._load_collection_specs(client)
            candidates: list[tuple[MongoCollectionSpec, dict[str, Any]]] = []
            for spec in specs:
                collection_state = checkpoint_state.get(spec.key)
                collection = client[spec.database_name][spec.collection_name]
                candidates.extend((spec, document) for document in self._fetch_documents(collection, spec, collection_state))

        candidates.sort(key=lambda item: self._sort_key(item[0], item[1]))
        current_state = dict(checkpoint_state)
        for spec, document in candidates:
            commit_timestamp = self._normalize_timestamp(document.get(spec.commit_timestamp_column))
            primary_key_value = document.get(spec.primary_key_field)
            current_state[spec.key] = {
                "watermark": self._normalize_checkpoint_value(document.get(spec.watermark_column)),
                "watermark_type": self._value_type(document.get(spec.watermark_column)),
                "pk": self._normalize_checkpoint_value(primary_key_value),
                "pk_type": self._value_type(primary_key_value),
            }
            normalized_document = self._normalize_document(document)
            primary_key = {spec.primary_key_field: normalized_document[spec.primary_key_field]}
            yield ChangeEvent(
                source_id=self.config.id,
                source_type=self.source_type,
                source_instance=self.config.source_instance,
                database_name=spec.database_name,
                schema_or_namespace=spec.database_name,
                table_or_collection=spec.collection_name,
                operation="snapshot",
                primary_key=primary_key,
                change_version=f"{spec.key}:{commit_timestamp}:{primary_key[spec.primary_key_field]}",
                commit_timestamp=commit_timestamp,
                before=None,
                after=normalized_document,
                metadata={
                    "capture_mode": "watermark-poll",
                    "watermark_column": spec.watermark_column,
                    "commit_timestamp_column": spec.commit_timestamp_column,
                    "primary_key_field": spec.primary_key_field,
                },
                checkpoint_token=json.dumps(current_state, sort_keys=True),
            )

    def serialize_checkpoint(self, event: ChangeEvent) -> str:
        return event.checkpoint_token or event.change_version

    def resume_from_checkpoint(self, checkpoint: SourceCheckpoint | None) -> None:
        return None

    @contextmanager
    def _connect(self) -> Iterator[Any]:
        if pymongo is None:
            raise RuntimeError("pymongo is required for live mongodb adapter use")
        client = pymongo.MongoClient(
            str(self.config.connection["uri"]),
            serverSelectionTimeoutMS=int(self.config.connection.get("server_selection_timeout_ms", 10000)),
            connectTimeoutMS=int(self.config.connection.get("connect_timeout_ms", 10000)),
        )
        try:
            yield client
        finally:
            client.close()

    def _capture_mode(self) -> str:
        return str(self.config.options.get("capture_mode", "watermark")).lower()

    def _load_collection_specs(self, client: Any) -> list[MongoCollectionSpec]:
        include = self.config.include or {self.config.database_name: []}
        watermark_column = str(self.config.options.get("watermark_column", "updated_at"))
        commit_timestamp_column = str(self.config.options.get("commit_timestamp_column", watermark_column))
        primary_key_field = str(self.config.options.get("primary_key_field", "_id"))
        specs: list[MongoCollectionSpec] = []
        for database_name, collections in include.items():
            target_database = str(database_name or self.config.database_name)
            if not collections:
                raise ValueError(f"{self.source_type} include must list explicit collections for the current baseline")
            available = set(client[target_database].list_collection_names())
            for collection_name in collections:
                if collection_name not in available:
                    raise ValueError(f"{self.source_type} collection {target_database}.{collection_name} was not found")
                specs.append(
                    MongoCollectionSpec(
                        database_name=target_database,
                        collection_name=str(collection_name),
                        watermark_column=watermark_column,
                        commit_timestamp_column=commit_timestamp_column,
                        primary_key_field=primary_key_field,
                    )
                )
        return specs

    def _fetch_documents(
        self,
        collection: Any,
        spec: MongoCollectionSpec,
        collection_state: dict[str, Any] | None,
    ) -> list[dict[str, Any]]:
        query: dict[str, Any] = {
            spec.watermark_column: {
                "$exists": True,
                "$ne": None,
            }
        }
        if collection_state:
            watermark_value = self._restore_checkpoint_value(
                collection_state.get("watermark"),
                str(collection_state.get("watermark_type", "string")),
            )
            primary_key_value = self._restore_checkpoint_value(
                collection_state.get("pk"),
                str(collection_state.get("pk_type", "string")),
            )
            query = {
                "$and": [
                    query,
                    {
                        "$or": [
                            {spec.watermark_column: {"$gt": watermark_value}},
                            {
                                spec.watermark_column: watermark_value,
                                spec.primary_key_field: {"$gt": primary_key_value},
                            },
                        ]
                    },
                ]
            }
        cursor = collection.find(query).sort(
            [
                (spec.watermark_column, 1),
                (spec.primary_key_field, 1),
            ]
        )
        row_limit = int(self.config.options.get("row_limit", self.config.batch_size))
        if row_limit > 0:
            cursor = cursor.limit(row_limit)
        return list(cursor)

    def _parse_checkpoint(self, checkpoint: SourceCheckpoint | None) -> dict[str, dict[str, Any]]:
        if checkpoint is None or not checkpoint.token:
            return {}
        payload = json.loads(checkpoint.token)
        if not isinstance(payload, dict):
            raise ValueError(f"{self.source_type} checkpoint token must be a JSON object")
        return payload

    def _sort_key(self, spec: MongoCollectionSpec, document: dict[str, Any]) -> tuple[Any, ...]:
        return (
            self._normalize_timestamp(document.get(spec.watermark_column)),
            str(self._normalize_checkpoint_value(document.get(spec.primary_key_field))),
        )

    def _normalize_document(self, payload: Any) -> Any:
        if isinstance(payload, dict):
            return {str(key): self._normalize_document(value) for key, value in payload.items()}
        if isinstance(payload, list):
            return [self._normalize_document(item) for item in payload]
        if isinstance(payload, tuple):
            return [self._normalize_document(item) for item in payload]
        return self._normalize_scalar(payload)

    def _normalize_scalar(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, datetime):
            normalized = value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
            return normalized.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        if isinstance(value, date) and not isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, time):
            return value.replace(microsecond=0).isoformat()
        if isinstance(value, Decimal):
            return format(value, "f")
        if Decimal128 is not None and isinstance(value, Decimal128):
            return format(value.to_decimal(), "f")
        if ObjectId is not None and isinstance(value, ObjectId):
            return str(value)
        if Binary is not None and isinstance(value, Binary):
            return bytes(value).hex()
        if isinstance(value, bytes):
            return value.hex()
        return value

    def _normalize_timestamp(self, value: Any) -> str:
        normalized = self._normalize_scalar(value)
        if not isinstance(normalized, str):
            raise ValueError(f"{self.source_type} timestamp value must normalize to string, got {type(normalized)!r}")
        return normalized

    def _normalize_checkpoint_value(self, value: Any) -> Any:
        normalized = self._normalize_scalar(value)
        if isinstance(normalized, dict):
            return json.dumps(normalized, sort_keys=True)
        if isinstance(normalized, list):
            return json.dumps(normalized, sort_keys=True)
        return normalized

    def _value_type(self, value: Any) -> str:
        if value is None:
            return "null"
        if isinstance(value, datetime):
            return "datetime"
        if isinstance(value, date) and not isinstance(value, datetime):
            return "date"
        if isinstance(value, time):
            return "time"
        if Decimal128 is not None and isinstance(value, Decimal128):
            return "decimal128"
        if isinstance(value, Decimal):
            return "decimal"
        if ObjectId is not None and isinstance(value, ObjectId):
            return "objectid"
        if Binary is not None and isinstance(value, Binary):
            return "binary"
        if isinstance(value, bytes):
            return "bytes"
        return type(value).__name__.lower()

    def _restore_checkpoint_value(self, value: Any, value_type: str) -> Any:
        if value_type == "null":
            return None
        if value_type == "datetime" and isinstance(value, str):
            return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
        if value_type == "date" and isinstance(value, str):
            return date.fromisoformat(value)
        if value_type == "time" and isinstance(value, str):
            return time.fromisoformat(value)
        if value_type == "decimal" and isinstance(value, str):
            return Decimal(value)
        if value_type == "decimal128" and isinstance(value, str):
            if Decimal128 is None:
                return value
            return Decimal128(value)
        if value_type == "objectid" and isinstance(value, str):
            if ObjectId is None:
                return value
            return ObjectId(value)
        if value_type == "bytes" and isinstance(value, str):
            return bytes.fromhex(value)
        if value_type == "binary" and isinstance(value, str):
            if Binary is None:
                return bytes.fromhex(value)
            return Binary(bytes.fromhex(value))
        return value

    def _spec_summary(self, spec: MongoCollectionSpec) -> dict[str, Any]:
        return {
            "database_name": spec.database_name,
            "collection_name": spec.collection_name,
            "watermark_column": spec.watermark_column,
            "commit_timestamp_column": spec.commit_timestamp_column,
            "primary_key_field": spec.primary_key_field,
        }

    def _spec_signature_entry(self, spec: MongoCollectionSpec) -> dict[str, Any]:
        return {
            "key": spec.key,
            "watermark_column": spec.watermark_column,
            "commit_timestamp_column": spec.commit_timestamp_column,
            "primary_key_field": spec.primary_key_field,
        }
