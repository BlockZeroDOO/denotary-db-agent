from __future__ import annotations

import json
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterator

from denotary_db_agent.adapters.base import AdapterCapabilities, BaseAdapter
from denotary_db_agent.models import ChangeEvent, SourceCheckpoint

try:
    import redis as redis_lib
except ImportError:  # pragma: no cover - handled in runtime validation
    redis_lib = None


@dataclass
class RedisKeySpec:
    database_number: int
    key_pattern: str

    @property
    def namespace_name(self) -> str:
        return f"db{self.database_number}"

    @property
    def key(self) -> str:
        return f"{self.namespace_name}:{self.key_pattern}"


class RedisAdapter(BaseAdapter):
    source_type = "redis"
    minimum_version = "7.0"
    adapter_notes = (
        "Wave 2 baseline implementation uses explicit key-pattern snapshot polling with deterministic key ordering "
        "and checkpoint resume. Native keyspace notification or stream-based CDC can be added later."
    )

    def discover_capabilities(self) -> AdapterCapabilities:
        return AdapterCapabilities(
            source_type=self.source_type,
            minimum_version=self.minimum_version,
            supports_cdc=False,
            supports_snapshot=True,
            operations=("snapshot",),
            notes=self.adapter_notes,
            capture_modes=("scan",),
            cdc_modes=(),
            default_capture_mode="scan",
            bootstrap_requirements=("configured key patterns",),
            checkpoint_strategy="key_lexicographic",
            activity_model="polling",
        )

    def validate_connection(self) -> None:
        if not self.config.connection.get("url") and not self.config.connection.get("host"):
            raise ValueError(f"{self.source_type} connection is missing required field: host or url")
        if self.config.connection.get("url") is None and not self.config.connection.get("port"):
            raise ValueError(f"{self.source_type} connection is missing required field: port")
        specs = self._configured_key_specs()
        if not specs:
            raise ValueError(f"{self.source_type} include must list explicit key patterns for the current baseline")
        if self.config.options.get("dry_run_events"):
            return
        if redis_lib is None:
            raise RuntimeError("redis is required for live redis adapter use")
        inspected_dbs: set[int] = set()
        for spec in specs:
            if spec.database_number in inspected_dbs:
                continue
            with self._connect(spec.database_number) as client:
                client.ping()
            inspected_dbs.add(spec.database_number)

    def bootstrap(self) -> dict:
        self.validate_connection()
        specs = self._configured_key_specs()
        tracked_items = [self._spec_summary(spec) for spec in specs]
        cdc = self.build_cdc_summary({"capture_mode": "scan"})
        return self.build_bootstrap_result(
            tracked_key="tracked_keys",
            tracked_items=tracked_items,
            cdc=cdc,
        )

    def inspect(self) -> dict:
        specs = self._configured_key_specs()
        tracked_items = [self._spec_summary(spec) for spec in specs]
        cdc = self.build_cdc_summary(
            {
                "capture_mode": "scan",
                "runtime": self.build_polling_runtime_summary(
                    configured_runtime_mode="scan",
                    effective_runtime_mode="scan",
                    notification_aware=False,
                    cursor={"tracked_pattern_count": len(specs)},
                ),
            }
        )
        return self.build_inspect_result(
            tracked_key="tracked_keys",
            tracked_items=tracked_items,
            cdc=cdc,
        )

    def runtime_signature(self) -> str:
        payload = {
            "adapter": self.config.adapter,
            "source_id": self.config.id,
            "capture_mode": self.capture_mode(),
            "tracked_keys": [self._spec_summary(spec) for spec in self._configured_key_specs()],
            "include": self.config.include,
        }
        return json.dumps(payload, sort_keys=True)

    def refresh_runtime(self) -> dict:
        return self.bootstrap()

    def start_stream(self, checkpoint: SourceCheckpoint | None):
        raise NotImplementedError("redis native CDC is not implemented yet; use capture_mode=scan")

    def stop_stream(self) -> None:
        return None

    def read_snapshot(self, checkpoint: SourceCheckpoint | None = None):
        dry_events = self.config.options.get("dry_run_events") or []
        for item in dry_events:
            yield ChangeEvent(
                source_id=self.config.id,
                source_type=self.source_type,
                source_instance=self.config.source_instance,
                database_name=str(item.get("database_name", self.config.database_name or "db0")),
                schema_or_namespace=str(item.get("schema_or_namespace", item.get("database_name", self.config.database_name or "db0"))),
                table_or_collection=str(item.get("table_or_collection", item.get("key_pattern", "keys"))),
                operation=str(item.get("operation", "snapshot")),
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

        specs = self._configured_key_specs()
        current_state = dict(checkpoint_state)
        candidates: list[tuple[RedisKeySpec, str, str, Any]] = []
        for spec in specs:
            last_key = None
            if spec.key in checkpoint_state:
                last_key = str(checkpoint_state[spec.key].get("last_key") or "")
            with self._connect(spec.database_number) as client:
                for redis_key in self._scan_keys(client, spec, last_key):
                    key_type = self._decode_key_type(client.type(redis_key))
                    candidates.append((spec, redis_key, key_type, self._read_value(client, redis_key, key_type)))

        candidates.sort(key=lambda item: (item[0].database_number, item[0].key_pattern, item[1]))
        for spec, redis_key, key_type, value in candidates:
            current_state[spec.key] = {"last_key": redis_key}
            commit_timestamp = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            yield ChangeEvent(
                source_id=self.config.id,
                source_type=self.source_type,
                source_instance=self.config.source_instance,
                database_name=spec.namespace_name,
                schema_or_namespace=spec.namespace_name,
                table_or_collection=spec.key_pattern,
                operation="snapshot",
                primary_key={"key": redis_key},
                change_version=f"{spec.namespace_name}:{redis_key}",
                commit_timestamp=commit_timestamp,
                before=None,
                after={
                    "key": redis_key,
                    "type": key_type,
                    "value": value,
                },
                metadata={
                    "capture_mode": "scan",
                    "database_number": spec.database_number,
                    "key_pattern": spec.key_pattern,
                    "key_type": key_type,
                },
                checkpoint_token=json.dumps(current_state, sort_keys=True),
            )

    def serialize_checkpoint(self, event: ChangeEvent) -> str:
        return event.checkpoint_token or event.change_version

    def resume_from_checkpoint(self, checkpoint: SourceCheckpoint | None) -> None:
        return None

    @contextmanager
    def _connect(self, database_number: int) -> Iterator[Any]:
        if redis_lib is None:
            raise RuntimeError("redis is required for live redis adapter use")
        url = self.config.connection.get("url")
        if url:
            client = redis_lib.Redis.from_url(
                str(url),
                db=database_number,
                decode_responses=False,
                socket_timeout=float(self.config.connection.get("socket_timeout_sec", 10)),
            )
        else:
            client = redis_lib.Redis(
                host=str(self.config.connection["host"]),
                port=int(self.config.connection["port"]),
                db=database_number,
                username=self.config.connection.get("username"),
                password=self.config.connection.get("password"),
                ssl=bool(self.config.connection.get("ssl", False)),
                decode_responses=False,
                socket_timeout=float(self.config.connection.get("socket_timeout_sec", 10)),
            )
        try:
            yield client
        finally:
            close = getattr(client, "close", None)
            if callable(close):
                close()

    def _configured_key_specs(self) -> list[RedisKeySpec]:
        include = self.config.include or {}
        default_db = int(self.config.connection.get("db", 0))
        specs: list[RedisKeySpec] = []
        if not include:
            raise ValueError(f"{self.source_type} include must list explicit key patterns for the current baseline")
        for namespace, patterns in include.items():
            database_number = default_db if namespace in {"", "*", "default"} else int(str(namespace))
            if not patterns:
                raise ValueError(f"{self.source_type} include must list explicit key patterns for database {database_number}")
            for pattern in patterns:
                specs.append(RedisKeySpec(database_number=database_number, key_pattern=str(pattern)))
        return specs

    def _parse_checkpoint(self, checkpoint: SourceCheckpoint | None) -> dict[str, dict[str, Any]]:
        if checkpoint is None or not checkpoint.token:
            return {}
        payload = json.loads(checkpoint.token)
        if not isinstance(payload, dict):
            raise ValueError(f"{self.source_type} checkpoint token must be a JSON object")
        return payload

    def _scan_keys(self, client: Any, spec: RedisKeySpec, last_key: str | None) -> list[str]:
        cursor = 0
        keys: list[str] = []
        while True:
            cursor, batch = client.scan(cursor=cursor, match=spec.key_pattern, count=int(self.config.options.get("scan_count", 100)))
            for raw_key in batch:
                redis_key = self._decode_value(raw_key)
                if last_key and redis_key <= last_key:
                    continue
                keys.append(redis_key)
            if int(cursor) == 0:
                break
        keys.sort()
        row_limit = int(self.config.options.get("row_limit", self.config.batch_size))
        if row_limit > 0:
            return keys[:row_limit]
        return keys

    def _read_value(self, client: Any, redis_key: str, key_type: str) -> Any:
        if key_type == "string":
            return self._normalize_value(client.get(redis_key))
        if key_type == "hash":
            values = client.hgetall(redis_key)
            return {self._decode_value(key): self._normalize_value(value) for key, value in values.items()}
        if key_type == "list":
            return [self._normalize_value(value) for value in client.lrange(redis_key, 0, -1)]
        if key_type == "set":
            return sorted(self._normalize_value(value) for value in client.smembers(redis_key))
        if key_type == "zset":
            return [
                {
                    "member": self._normalize_value(member),
                    "score": score,
                }
                for member, score in client.zrange(redis_key, 0, -1, withscores=True)
            ]
        serialized = client.dump(redis_key)
        return self._normalize_value(serialized)

    def _decode_key_type(self, raw_value: Any) -> str:
        decoded = self._decode_value(raw_value)
        return str(decoded or "none")

    def _decode_value(self, value: Any) -> Any:
        if isinstance(value, bytes):
            return value.decode("utf-8")
        return value

    def _normalize_value(self, value: Any) -> Any:
        if isinstance(value, bytes):
            return value.decode("utf-8")
        if isinstance(value, dict):
            return {self._decode_value(key): self._normalize_value(item) for key, item in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [self._normalize_value(item) for item in value]
        return value

    def _spec_summary(self, spec: RedisKeySpec) -> dict[str, Any]:
        return {
            "database_number": spec.database_number,
            "database_name": spec.namespace_name,
            "key_pattern": spec.key_pattern,
        }
