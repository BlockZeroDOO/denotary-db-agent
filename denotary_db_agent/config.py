from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class SourceConfig:
    id: str
    adapter: str
    enabled: bool
    source_instance: str
    database_name: str
    include: dict[str, list[str]] = field(default_factory=dict)
    exclude: dict[str, list[str]] = field(default_factory=dict)
    checkpoint_policy: str = "after_ack"
    backfill_mode: str = "none"
    batch_enabled: bool = False
    batch_size: int = 100
    flush_interval_ms: int = 1000
    connection: dict[str, Any] = field(default_factory=dict)
    options: dict[str, Any] = field(default_factory=dict)


@dataclass
class DenotaryConfig:
    ingress_url: str
    watcher_url: str
    watcher_auth_token: str
    submitter: str
    schema_id: int
    policy_id: int
    billing_account: str = "verifbill"


@dataclass
class StorageConfig:
    state_db: str


@dataclass
class AgentConfig:
    agent_name: str
    log_level: str
    denotary: DenotaryConfig
    storage: StorageConfig
    sources: list[SourceConfig]


def _require_non_empty_string(mapping: dict[str, Any], field_name: str) -> str:
    value = mapping.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()


def _require_bool(mapping: dict[str, Any], field_name: str, default: bool = False) -> bool:
    value = mapping.get(field_name, default)
    if not isinstance(value, bool):
        raise ValueError(f"{field_name} must be boolean")
    return value


def _require_int(mapping: dict[str, Any], field_name: str) -> int:
    value = mapping.get(field_name)
    if not isinstance(value, int):
        raise ValueError(f"{field_name} must be integer")
    return value


def load_config(path: str | Path) -> AgentConfig:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("config root must be an object")

    denotary_raw = raw.get("denotary")
    storage_raw = raw.get("storage")
    sources_raw = raw.get("sources")
    if not isinstance(denotary_raw, dict):
        raise ValueError("denotary must be an object")
    if not isinstance(storage_raw, dict):
        raise ValueError("storage must be an object")
    if not isinstance(sources_raw, list) or not sources_raw:
        raise ValueError("sources must be a non-empty array")

    denotary = DenotaryConfig(
        ingress_url=_require_non_empty_string(denotary_raw, "ingress_url"),
        watcher_url=_require_non_empty_string(denotary_raw, "watcher_url"),
        watcher_auth_token=str(denotary_raw.get("watcher_auth_token", "")),
        submitter=_require_non_empty_string(denotary_raw, "submitter"),
        schema_id=_require_int(denotary_raw, "schema_id"),
        policy_id=_require_int(denotary_raw, "policy_id"),
        billing_account=str(denotary_raw.get("billing_account", "verifbill")),
    )
    storage = StorageConfig(state_db=_require_non_empty_string(storage_raw, "state_db"))

    sources: list[SourceConfig] = []
    for index, item in enumerate(sources_raw):
        if not isinstance(item, dict):
            raise ValueError(f"sources[{index}] must be an object")
        sources.append(
            SourceConfig(
                id=_require_non_empty_string(item, "id"),
                adapter=_require_non_empty_string(item, "adapter"),
                enabled=_require_bool(item, "enabled", True),
                source_instance=_require_non_empty_string(item, "source_instance"),
                database_name=_require_non_empty_string(item, "database_name"),
                include=item.get("include") or {},
                exclude=item.get("exclude") or {},
                checkpoint_policy=str(item.get("checkpoint_policy", "after_ack")),
                backfill_mode=str(item.get("backfill_mode", "none")),
                batch_enabled=_require_bool(item, "batch_enabled", False),
                batch_size=int(item.get("batch_size", 100)),
                flush_interval_ms=int(item.get("flush_interval_ms", 1000)),
                connection=item.get("connection") or {},
                options=item.get("options") or {},
            )
        )

    return AgentConfig(
        agent_name=str(raw.get("agent_name", "denotary-db-agent")),
        log_level=str(raw.get("log_level", "INFO")),
        denotary=denotary,
        storage=storage,
        sources=sources,
    )
