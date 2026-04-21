from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal
from typing import Any

from denotary_db_agent.adapters.base import AdapterCapabilities, BaseAdapter
from denotary_db_agent.models import ChangeEvent, SourceCheckpoint

try:
    from elasticsearch import Elasticsearch
except ImportError:  # pragma: no cover - handled in runtime validation
    Elasticsearch = None


@dataclass
class ElasticsearchIndexSpec:
    namespace_name: str
    index_name: str
    watermark_field: str
    commit_timestamp_field: str
    primary_key_field: str
    selected_fields: list[str]

    @property
    def key(self) -> str:
        return f"{self.namespace_name}.{self.index_name}"


class ElasticsearchAdapter(BaseAdapter):
    source_type = "elasticsearch"
    minimum_version = "8"
    adapter_notes = (
        "Wave 2 Elasticsearch support currently provides connection-shape validation, live cluster ping, "
        "tracked-index introspection, query-based snapshot polling, deterministic checkpoint resume, and "
        "dry-run snapshot playback. Native Elasticsearch CDC is not implemented in the current baseline."
    )

    def discover_capabilities(self) -> AdapterCapabilities:
        return AdapterCapabilities(
            source_type=self.source_type,
            minimum_version=self.minimum_version,
            supports_cdc=False,
            supports_snapshot=True,
            operations=("snapshot",),
            capture_modes=("watermark",),
            cdc_modes=(),
            default_capture_mode="watermark",
            bootstrap_requirements=("tracked indices visible", "watermark field configured"),
            checkpoint_strategy="document_watermark",
            activity_model="polling",
            notes=self.adapter_notes,
        )

    def validate_connection(self) -> None:
        if not self._hosts():
            raise ValueError(f"{self.source_type} connection is missing required field: url, host, or hosts")
        if self.config.options.get("dry_run_events"):
            self._configured_index_specs()
            return
        if Elasticsearch is None:
            raise RuntimeError("elasticsearch is required for live elasticsearch adapter use")
        client = self._client()
        try:
            client.info()
            self._load_index_specs(client)
        finally:
            client.close()

    def bootstrap(self) -> dict:
        self.validate_connection()
        specs = self._load_live_or_configured_specs()
        return self.build_bootstrap_result(
            tracked_key="tracked_tables",
            tracked_items=[self._spec_summary(spec) for spec in specs],
            cdc=self._cdc_summary(),
            extra={"tracking_model": "indices"},
        )

    def inspect(self) -> dict:
        self.validate_connection()
        specs = self._load_live_or_configured_specs()
        return self.build_inspect_result(
            tracked_key="tracked_tables",
            tracked_items=[self._spec_summary(spec) for spec in specs],
            cdc=self._cdc_summary(),
            extra={"tracking_model": "indices"},
        )

    def runtime_signature(self) -> str:
        specs = self._configured_index_specs()
        payload = {
            "adapter": self.config.adapter,
            "source_id": self.config.id,
            "capture_mode": self.capture_mode(),
            "tracked_tables": [self._spec_signature_entry(spec) for spec in specs],
            "include": self.config.include,
        }
        return json.dumps(payload, sort_keys=True)

    def refresh_runtime(self) -> dict:
        return self.bootstrap()

    def start_stream(self, checkpoint: SourceCheckpoint | None):
        raise NotImplementedError("elasticsearch native CDC is not implemented yet; use capture_mode=watermark")

    def stop_stream(self) -> None:
        return None

    def read_snapshot(self, checkpoint: SourceCheckpoint | None = None):
        dry_events = self.config.options.get("dry_run_events") or []
        for item in dry_events:
            yield ChangeEvent(
                source_id=self.config.id,
                source_type=self.source_type,
                source_instance=self.config.source_instance,
                database_name=self.config.database_name,
                schema_or_namespace=str(item.get("schema_or_namespace", "default")),
                table_or_collection=str(item.get("table_or_collection", "documents")),
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

        client = self._client()
        try:
            specs = self._load_index_specs(client)
            candidates: list[tuple[ElasticsearchIndexSpec, dict[str, Any]]] = []
            for spec in specs:
                index_state = checkpoint_state.get(spec.key)
                candidates.extend((spec, document) for document in self._fetch_documents(client, spec, index_state))
        finally:
            client.close()

        candidates.sort(key=lambda item: self._sort_key(item[0], item[1]))
        current_state = dict(checkpoint_state)
        for spec, document in candidates:
            commit_timestamp = self._normalize_timestamp(document.get(spec.commit_timestamp_field))
            primary_key = {spec.primary_key_field: document.get(spec.primary_key_field)}
            current_state[spec.key] = {
                "watermark": self._normalize_timestamp(document.get(spec.watermark_field)),
                "pk": self._coerce_pk_value(primary_key[spec.primary_key_field]),
            }
            payload_after = {field: self._normalize_value(document.get(field)) for field in spec.selected_fields if field in document}
            yield ChangeEvent(
                source_id=self.config.id,
                source_type=self.source_type,
                source_instance=self.config.source_instance,
                database_name=self.config.database_name,
                schema_or_namespace=spec.namespace_name,
                table_or_collection=spec.index_name,
                operation="snapshot",
                primary_key={key: self._normalize_value(value) for key, value in primary_key.items()},
                change_version=f"{spec.key}:{commit_timestamp}:{primary_key[spec.primary_key_field]}",
                commit_timestamp=commit_timestamp,
                before=None,
                after=payload_after,
                metadata={
                    "capture_mode": "watermark-poll",
                    "watermark_field": spec.watermark_field,
                    "commit_timestamp_field": spec.commit_timestamp_field,
                    "primary_key_field": spec.primary_key_field,
                },
                checkpoint_token=json.dumps(current_state, sort_keys=True),
            )

    def serialize_checkpoint(self, event: ChangeEvent) -> str:
        return event.checkpoint_token or event.change_version

    def resume_from_checkpoint(self, checkpoint: SourceCheckpoint | None) -> None:
        return None

    def _hosts(self) -> list[str]:
        url = str(self.config.connection.get("url", "")).strip()
        if url:
            return [url]
        raw_hosts = self.config.connection.get("hosts")
        if isinstance(raw_hosts, list):
            hosts = [str(item).strip() for item in raw_hosts if str(item).strip()]
            if hosts:
                return hosts
        host = str(self.config.connection.get("host", "")).strip()
        if not host:
            return []
        port = int(self.config.connection.get("port", 9200))
        scheme = str(self.config.connection.get("scheme", "http")).strip() or "http"
        return [f"{scheme}://{host}:{port}"]

    def _client(self):
        assert Elasticsearch is not None
        kwargs: dict[str, Any] = {
            "hosts": self._hosts(),
        }
        username = self.config.connection.get("username")
        password = self.config.connection.get("password")
        if username and password:
            kwargs["basic_auth"] = (str(username), str(password))
        verify_certs = self.config.connection.get("verify_certs")
        if verify_certs is not None:
            kwargs["verify_certs"] = bool(verify_certs)
        return Elasticsearch(**kwargs)

    def _load_live_or_configured_specs(self) -> list[ElasticsearchIndexSpec]:
        if self.config.options.get("dry_run_events"):
            return self._configured_index_specs()
        if Elasticsearch is None:
            return self._configured_index_specs()
        client = self._client()
        try:
            return self._load_index_specs(client)
        finally:
            client.close()

    def _configured_index_specs(self) -> list[ElasticsearchIndexSpec]:
        include = self.config.include or {}
        watermark_field = str(self.config.options.get("watermark_field", "updated_at")).strip() or "updated_at"
        commit_timestamp_field = str(self.config.options.get("commit_timestamp_field", watermark_field)).strip() or watermark_field
        primary_key_field = str(self.config.options.get("primary_key_field", "_id")).strip() or "_id"
        specs: list[ElasticsearchIndexSpec] = []
        for namespace_name, index_names in include.items():
            if not isinstance(namespace_name, str) or not namespace_name.strip():
                raise ValueError("elasticsearch include namespace names must be non-empty strings")
            normalized_namespace = namespace_name.strip()
            if not isinstance(index_names, list) or not all(isinstance(item, str) and item.strip() for item in index_names):
                raise ValueError(f"elasticsearch include[{normalized_namespace}] must be an array of non-empty index names")
            for raw_index_name in index_names:
                index_name = raw_index_name.strip()
                specs.append(
                    ElasticsearchIndexSpec(
                        namespace_name=normalized_namespace,
                        index_name=index_name,
                        watermark_field=watermark_field,
                        commit_timestamp_field=commit_timestamp_field,
                        primary_key_field=primary_key_field,
                        selected_fields=[primary_key_field, watermark_field, commit_timestamp_field],
                    )
                )
        return specs

    def _load_index_specs(self, client: Any) -> list[ElasticsearchIndexSpec]:
        configured = self._configured_index_specs()
        specs: list[ElasticsearchIndexSpec] = []
        for item in configured:
            mapping_response = client.indices.get_mapping(index=item.index_name)
            mapping = mapping_response.get(item.index_name) or next(iter(mapping_response.values()), {})
            properties = (
                mapping.get("mappings", {}).get("properties", {})
                if isinstance(mapping, dict)
                else {}
            )
            available_fields = [str(field) for field in properties.keys()]
            if item.watermark_field not in available_fields:
                raise ValueError(
                    f"{self.source_type} index {item.index_name} does not contain watermark field {item.watermark_field}"
                )
            if item.commit_timestamp_field not in available_fields:
                raise ValueError(
                    f"{self.source_type} index {item.index_name} does not contain commit timestamp field {item.commit_timestamp_field}"
                )
            if item.primary_key_field != "_id" and item.primary_key_field not in available_fields:
                raise ValueError(
                    f"{self.source_type} index {item.index_name} does not contain primary key field {item.primary_key_field}"
                )
            selected_fields = [item.primary_key_field, *available_fields]
            if item.commit_timestamp_field not in selected_fields:
                selected_fields.append(item.commit_timestamp_field)
            specs.append(
                ElasticsearchIndexSpec(
                    namespace_name=item.namespace_name,
                    index_name=item.index_name,
                    watermark_field=item.watermark_field,
                    commit_timestamp_field=item.commit_timestamp_field,
                    primary_key_field=item.primary_key_field,
                    selected_fields=selected_fields,
                )
            )
        return specs

    def _cdc_summary(self) -> dict[str, object]:
        return self.build_cdc_summary(
            {
                "runtime": self.build_polling_runtime_summary(
                    cursor=None,
                    configured_runtime_mode=self.capture_mode(),
                    effective_runtime_mode=self.capture_mode(),
                    notification_aware=False,
                ),
            }
        )

    def _parse_checkpoint(self, checkpoint: SourceCheckpoint | None) -> dict[str, dict[str, Any]]:
        if checkpoint is None or not checkpoint.token:
            return {}
        parsed = json.loads(checkpoint.token)
        if not isinstance(parsed, dict):
            raise ValueError(f"{self.source_type} checkpoint token must be a JSON object")
        return {str(key): value for key, value in parsed.items() if isinstance(value, dict)}

    def _fetch_documents(self, client: Any, spec: ElasticsearchIndexSpec, index_state: dict[str, Any] | None) -> list[dict[str, Any]]:
        query: dict[str, Any] = {
            "bool": {
                "filter": [
                    {"exists": {"field": spec.watermark_field}},
                ]
            }
        }
        if index_state:
            query["bool"]["filter"].append({"range": {spec.watermark_field: {"gte": index_state.get("watermark")}}})
        row_limit = int(self.config.options.get("row_limit", self.config.batch_size))
        response = client.search(
            index=spec.index_name,
            size=row_limit if row_limit > 0 else 100,
            sort=[
                {spec.watermark_field: {"order": "asc"}},
                {spec.primary_key_field: {"order": "asc"}},
            ],
            query=query,
        )
        hits = response.get("hits", {}).get("hits", [])
        documents = [self._normalize_hit(hit, spec) for hit in hits]
        if index_state:
            documents = [document for document in documents if self._document_after_checkpoint(spec, document, index_state)]
        return documents

    def _normalize_hit(self, hit: dict[str, Any], spec: ElasticsearchIndexSpec) -> dict[str, Any]:
        payload = dict(hit.get("_source") or {})
        if spec.primary_key_field == "_id":
            payload[spec.primary_key_field] = hit.get("_id")
        return payload

    def _document_after_checkpoint(self, spec: ElasticsearchIndexSpec, document: dict[str, Any], index_state: dict[str, Any]) -> bool:
        watermark_value = self._normalize_timestamp(document.get(spec.watermark_field))
        checkpoint_watermark = str(index_state.get("watermark"))
        if watermark_value > checkpoint_watermark:
            return True
        if watermark_value < checkpoint_watermark:
            return False
        pk_value = self._coerce_pk_value(document.get(spec.primary_key_field))
        return pk_value > index_state.get("pk")

    def _sort_key(self, spec: ElasticsearchIndexSpec, document: dict[str, Any]) -> tuple[Any, ...]:
        return (
            self._normalize_timestamp(document.get(spec.watermark_field)),
            self._coerce_pk_value(document.get(spec.primary_key_field)),
        )

    def _normalize_value(self, value: Any) -> Any:
        if isinstance(value, datetime):
            normalized = value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
            return normalized.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        if isinstance(value, date) and not isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, time):
            return value.replace(microsecond=0).isoformat()
        if isinstance(value, Decimal):
            return format(value, "f")
        if isinstance(value, bytes):
            return value.hex()
        if isinstance(value, dict):
            return {str(key): self._normalize_value(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self._normalize_value(item) for item in value]
        return value

    def _normalize_timestamp(self, value: Any) -> str:
        normalized = self._normalize_value(value)
        if isinstance(normalized, str):
            return normalized
        return str(normalized)

    def _coerce_pk_value(self, value: Any) -> Any:
        normalized = self._normalize_value(value)
        if isinstance(normalized, (str, int, float, bool)) or normalized is None:
            return normalized
        return str(normalized)

    def _spec_summary(self, spec: ElasticsearchIndexSpec) -> dict[str, object]:
        return {
            "schema_name": spec.namespace_name,
            "table_name": spec.index_name,
            "watermark_column": spec.watermark_field,
            "commit_timestamp_column": spec.commit_timestamp_field,
            "primary_key_columns": [spec.primary_key_field],
            "selected_columns": list(spec.selected_fields),
        }

    def _spec_signature_entry(self, spec: ElasticsearchIndexSpec) -> dict[str, object]:
        return {
            "key": spec.key,
            "watermark_column": spec.watermark_field,
            "commit_timestamp_column": spec.commit_timestamp_field,
            "primary_key_columns": [spec.primary_key_field],
            "selected_columns": list(spec.selected_fields),
        }
