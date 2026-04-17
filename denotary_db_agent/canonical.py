from __future__ import annotations

import uuid
from typing import Any

from denotary_db_agent.models import CanonicalEnvelope, ChangeEvent, canonical_json, event_identity_key, sha256_hex


def stable_row_hash(payload: dict[str, Any] | None) -> str | None:
    if payload is None:
        return None
    return sha256_hex(canonical_json(payload))


def stable_metadata_hash(payload: dict[str, Any]) -> str:
    return sha256_hex(canonical_json(payload))


def build_external_ref(event: ChangeEvent) -> str:
    identity = event_identity_key(event)
    checkpoint = event.checkpoint_token or event.change_version
    return sha256_hex(f"{identity}:{checkpoint}")


def build_trace_id() -> str:
    return str(uuid.uuid4())


def canonicalize_event(event: ChangeEvent) -> CanonicalEnvelope:
    return CanonicalEnvelope(
        source_type=event.source_type,
        source_instance=event.source_instance,
        database_name=event.database_name,
        schema_or_namespace=event.schema_or_namespace,
        table_or_collection=event.table_or_collection,
        operation=event.operation,
        primary_key=event.primary_key,
        change_version=event.change_version,
        commit_timestamp=event.commit_timestamp,
        before_hash=stable_row_hash(event.before),
        after_hash=stable_row_hash(event.after or {}),
        metadata_hash=stable_metadata_hash(event.metadata),
        external_ref=build_external_ref(event),
        trace_id=build_trace_id(),
    )

