from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal


Mode = Literal["single", "batch"]
Operation = Literal["insert", "update", "delete", "snapshot"]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def canonical_json(payload: Any) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def sha256_hex(payload: str | bytes) -> str:
    encoded = payload if isinstance(payload, bytes) else payload.encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@dataclass
class SourceCheckpoint:
    source_id: str
    token: str
    updated_at: str


@dataclass
class ChangeEvent:
    source_id: str
    source_type: str
    source_instance: str
    database_name: str
    schema_or_namespace: str
    table_or_collection: str
    operation: Operation
    primary_key: dict[str, Any]
    change_version: str
    commit_timestamp: str
    after: dict[str, Any] | None = None
    before: dict[str, Any] | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    checkpoint_token: str = ""
    mode: Mode = "single"


@dataclass
class CanonicalEnvelope:
    source_type: str
    source_instance: str
    database_name: str
    schema_or_namespace: str
    table_or_collection: str
    operation: Operation
    primary_key: dict[str, Any]
    change_version: str
    commit_timestamp: str
    before_hash: str | None
    after_hash: str
    metadata_hash: str
    external_ref: str
    trace_id: str

    def to_prepare_payload(self, submitter: str, schema_id: int, policy_id: int) -> dict[str, Any]:
        return {
            "submitter": submitter,
            "external_ref": self.external_ref,
            "schema": {
                "id": schema_id,
                "version": "1.0.0",
                "active": True,
                "canonicalization_profile": "json-sorted-v1",
            },
            "policy": {
                "id": policy_id,
                "active": True,
                "allow_single": True,
                "allow_batch": False,
            },
            "document": {
                "source_type": self.source_type,
                "source_instance": self.source_instance,
                "database_name": self.database_name,
                "schema_or_namespace": self.schema_or_namespace,
                "table_or_collection": self.table_or_collection,
                "operation": self.operation,
                "primary_key": self.primary_key,
                "change_version": self.change_version,
                "commit_timestamp": self.commit_timestamp,
                "before_hash": self.before_hash,
                "after_hash": self.after_hash,
                "metadata_hash": self.metadata_hash,
                "trace_id": self.trace_id,
            },
        }


@dataclass
class DeliveryAttempt:
    request_id: str
    trace_id: str
    source_id: str
    external_ref: str
    tx_id: str | None
    status: str
    prepared_action: dict[str, Any] | None
    last_error: str | None
    updated_at: str


def event_identity_payload(event: ChangeEvent) -> dict[str, Any]:
    return {
        "source_type": event.source_type,
        "source_instance": event.source_instance,
        "database_name": event.database_name,
        "schema_or_namespace": event.schema_or_namespace,
        "table_or_collection": event.table_or_collection,
        "operation": event.operation,
        "primary_key": event.primary_key,
        "change_version": event.change_version,
    }


def event_identity_key(event: ChangeEvent) -> str:
    return sha256_hex(canonical_json(event_identity_payload(event)))


def event_debug_dict(event: ChangeEvent) -> dict[str, Any]:
    payload = asdict(event)
    payload["identity_key"] = event_identity_key(event)
    return payload
