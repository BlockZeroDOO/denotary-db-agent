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
    import snowflake.connector as snowflake_connector
except ImportError:  # pragma: no cover - handled in runtime validation
    snowflake_connector = None


@dataclass
class SnowflakeObjectSpec:
    database_name: str
    schema_name: str
    object_name: str
    object_type: str
    watermark_column: str
    commit_timestamp_column: str
    primary_key_columns: list[str]
    selected_columns: list[str]

    @property
    def key(self) -> str:
        return f"{self.database_name}.{self.schema_name}.{self.object_name}"


class SnowflakeAdapter(BaseAdapter):
    source_type = "snowflake"
    minimum_version = "current"
    adapter_notes = (
        "Wave 2 Snowflake support currently provides connection-shape validation, live warehouse ping, "
        "configured tracked-object introspection, shared bootstrap and inspect payloads, deterministic runtime "
        "signatures, and dry-run snapshot playback. Query-based incremental capture and production validation "
        "remain the next Snowflake-specific steps."
    )
    required_connection_fields = ("account", "username", "database", "schema", "warehouse")

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
            bootstrap_requirements=(
                "tracked tables or views configured",
                "warehouse access",
                "schema metadata access",
                "watermark columns configured",
            ),
            checkpoint_strategy="table_watermark",
            activity_model="polling",
            notes=self.adapter_notes,
        )

    def validate_connection(self) -> None:
        missing = [name for name in self.required_connection_fields if not self.config.connection.get(name)]
        if missing:
            field_label = "field" if len(missing) == 1 else "fields"
            raise ValueError(f"{self.source_type} connection is missing required {field_label}: {', '.join(missing)}")
        if self.config.options.get("dry_run_events"):
            self._configured_object_specs()
            return
        if snowflake_connector is None:
            raise RuntimeError("snowflake-connector-python is required for live snowflake adapter use")
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute("select current_version() as version")
                cursor.fetchone()
            self._load_object_specs(connection)

    def bootstrap(self) -> dict:
        self.validate_connection()
        specs = self._load_live_or_configured_specs()
        return self.build_bootstrap_result(
            tracked_key="tracked_tables",
            tracked_items=[self._spec_summary(spec) for spec in specs],
            cdc=self._cdc_summary(),
            extra={"tracking_model": "configured_objects"},
        )

    def inspect(self) -> dict:
        self.validate_connection()
        specs = self._load_live_or_configured_specs()
        return self.build_inspect_result(
            tracked_key="tracked_tables",
            tracked_items=[self._spec_summary(spec) for spec in specs],
            cdc=self._cdc_summary(),
            extra={"tracking_model": "configured_objects"},
        )

    def runtime_signature(self) -> str:
        specs = self._load_live_or_configured_specs()
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
        raise NotImplementedError("snowflake does not expose a native CDC stream in the current Wave 2 baseline")

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
                schema_or_namespace=str(item.get("schema_or_namespace", self.config.connection.get("schema", self.config.database_name))),
                table_or_collection=str(item.get("table_or_collection", item.get("table_name", "configured_object"))),
                operation=str(item.get("operation", "snapshot")),
                primary_key=dict(item.get("primary_key") or {}),
                change_version=str(item.get("change_version", "")),
                commit_timestamp=str(item.get("commit_timestamp", "")),
                after=dict(item.get("after") or {}),
                before=dict(item.get("before") or {}) or None,
                metadata=dict(item.get("metadata") or {}),
                checkpoint_token=str(item.get("checkpoint_token", "")),
                mode=str(item.get("mode", "single")),
            )
        if dry_events:
            return

        checkpoint_state = self._parse_checkpoint(checkpoint)
        if checkpoint is None and self.config.backfill_mode == "none":
            return

        with self._connect() as connection:
            specs = self._load_object_specs(connection)
            candidates: list[tuple[SnowflakeObjectSpec, dict[str, Any]]] = []
            for spec in specs:
                object_state = checkpoint_state.get(spec.key)
                candidates.extend((spec, row) for row in self._fetch_rows(connection, spec, object_state))

        candidates.sort(key=lambda item: self._sort_key(item[0], item[1]))
        current_state = dict(checkpoint_state)
        for spec, row in candidates:
            commit_timestamp = self._normalize_timestamp(self._row_get(row, spec.commit_timestamp_column))
            primary_key = {column: self._row_get(row, column) for column in spec.primary_key_columns}
            current_state[spec.key] = {
                "watermark": self._normalize_timestamp(self._row_get(row, spec.watermark_column)),
                "pk": [self._coerce_pk_value(primary_key[column]) for column in spec.primary_key_columns],
            }
            payload_after = {column: self._normalize_value(self._row_get(row, column)) for column in spec.selected_columns}
            yield ChangeEvent(
                source_id=self.config.id,
                source_type=self.source_type,
                source_instance=self.config.source_instance,
                database_name=self.config.database_name,
                schema_or_namespace=spec.schema_name,
                table_or_collection=spec.object_name,
                operation="snapshot",
                primary_key={key: self._normalize_value(value) for key, value in primary_key.items()},
                change_version=f"{spec.key}:{commit_timestamp}:{self._pk_marker(primary_key, spec.primary_key_columns)}",
                commit_timestamp=commit_timestamp,
                before=None,
                after=payload_after,
                metadata={
                    "capture_mode": "watermark-poll",
                    "object_type": spec.object_type,
                    "watermark_column": spec.watermark_column,
                    "commit_timestamp_column": spec.commit_timestamp_column,
                    "primary_key_columns": spec.primary_key_columns,
                },
                checkpoint_token=json.dumps(current_state, sort_keys=True),
            )

    def serialize_checkpoint(self, event: ChangeEvent) -> str:
        return event.checkpoint_token or event.change_version

    def resume_from_checkpoint(self, checkpoint: SourceCheckpoint | None) -> None:
        return None

    @contextmanager
    def _connect(self) -> Iterator[Any]:
        assert snowflake_connector is not None
        connection_kwargs: dict[str, Any] = {
            "account": self.config.connection.get("account"),
            "user": self.config.connection.get("username"),
            "warehouse": self.config.connection.get("warehouse"),
            "database": self.config.connection.get("database"),
            "schema": self.config.connection.get("schema"),
        }
        for optional_key, target_key in (
            ("password", "password"),
            ("role", "role"),
        ):
            value = self.config.connection.get(optional_key)
            if value:
                connection_kwargs[target_key] = value
        connection = snowflake_connector.connect(**connection_kwargs)
        try:
            yield connection
        finally:
            connection.close()

    def _load_live_or_configured_specs(self) -> list[SnowflakeObjectSpec]:
        if self.config.options.get("dry_run_events"):
            return self._configured_object_specs()
        if snowflake_connector is None:
            return self._configured_object_specs()
        with self._connect() as connection:
            return self._load_object_specs(connection)

    def _configured_object_specs(self) -> list[SnowflakeObjectSpec]:
        include = self.config.include or {}
        if not isinstance(include, dict):
            raise ValueError("snowflake include must be an object keyed by schema name")
        default_schema = str(self.config.connection.get("schema", self.config.database_name))
        default_object_type = str(self.config.options.get("tracked_object_type", "table")).strip().lower() or "table"
        if default_object_type not in {"table", "view"}:
            raise ValueError("snowflake tracked_object_type must be either 'table' or 'view'")
        watermark_column = str(self.config.options.get("watermark_column", "updated_at")).strip() or "updated_at"
        commit_timestamp_column = (
            str(self.config.options.get("commit_timestamp_column", watermark_column)).strip() or watermark_column
        )
        primary_key_columns = self._primary_key_columns()
        specs: list[SnowflakeObjectSpec] = []
        schema_items = include.items() or [(default_schema, [])]
        for schema_name, object_names in schema_items:
            if not isinstance(schema_name, str) or not schema_name.strip():
                raise ValueError("snowflake include schema names must be non-empty strings")
            normalized_schema = schema_name.strip()
            if not isinstance(object_names, list) or not all(isinstance(item, str) and item.strip() for item in object_names):
                raise ValueError(f"snowflake include[{normalized_schema}] must be an array of non-empty object names")
            for raw_name in object_names:
                object_type, object_name = self._parse_object_ref(raw_name, default_object_type)
                specs.append(
                    SnowflakeObjectSpec(
                        database_name=self.config.database_name,
                        schema_name=normalized_schema,
                        object_name=object_name,
                        object_type=object_type,
                        watermark_column=watermark_column,
                        commit_timestamp_column=commit_timestamp_column,
                        primary_key_columns=list(primary_key_columns),
                        selected_columns=[*primary_key_columns, watermark_column, commit_timestamp_column],
                    )
                )
        return specs

    def _load_object_specs(self, connection: Any) -> list[SnowflakeObjectSpec]:
        configured = self._configured_object_specs()
        specs: list[SnowflakeObjectSpec] = []
        for item in configured:
            columns = self._load_object_columns(connection, item)
            column_lookup = {column.lower(): column for column in columns}
            actual_watermark_column = column_lookup.get(item.watermark_column.lower())
            if actual_watermark_column is None:
                raise ValueError(
                    f"{self.source_type} object {item.schema_name}.{item.object_name} does not contain watermark column {item.watermark_column}"
                )
            actual_commit_timestamp_column = column_lookup.get(item.commit_timestamp_column.lower())
            if actual_commit_timestamp_column is None:
                raise ValueError(
                    f"{self.source_type} object {item.schema_name}.{item.object_name} does not contain commit timestamp column {item.commit_timestamp_column}"
                )
            actual_primary_key_columns = [column_lookup.get(column.lower()) for column in item.primary_key_columns]
            if any(column is None for column in actual_primary_key_columns):
                missing = [column for column, actual in zip(item.primary_key_columns, actual_primary_key_columns) if actual is None]
                raise ValueError(
                    f"{self.source_type} object {item.schema_name}.{item.object_name} does not contain primary key column(s): {', '.join(missing)}"
                )
            discovered_primary_keys = self._load_primary_key_columns(connection, item)
            primary_key_columns = discovered_primary_keys or [str(column) for column in actual_primary_key_columns]
            specs.append(
                SnowflakeObjectSpec(
                    database_name=item.database_name,
                    schema_name=item.schema_name,
                    object_name=item.object_name,
                    object_type=item.object_type,
                    watermark_column=actual_watermark_column,
                    commit_timestamp_column=actual_commit_timestamp_column,
                    primary_key_columns=primary_key_columns,
                    selected_columns=columns,
                )
            )
        return specs

    def _load_object_columns(self, connection: Any, spec: SnowflakeObjectSpec) -> list[str]:
        with self._cursor(connection) as cursor:
            cursor.execute(
                """
                select column_name
                from information_schema.columns
                where table_catalog = %s
                  and table_schema = %s
                  and table_name = %s
                order by ordinal_position
                """,
                (spec.database_name, spec.schema_name, spec.object_name),
            )
            rows = cursor.fetchall()
        columns = [str(self._row_get(row, "column_name")) for row in rows if self._row_get(row, "column_name") is not None]
        if not columns:
            raise ValueError(f"{self.source_type} object {spec.schema_name}.{spec.object_name} is not visible in information_schema.columns")
        return columns

    def _load_primary_key_columns(self, connection: Any, spec: SnowflakeObjectSpec) -> list[str]:
        if spec.object_type == "view":
            return []
        with self._cursor(connection) as cursor:
            cursor.execute(
                """
                select kcu.column_name
                from information_schema.table_constraints tc
                join information_schema.key_column_usage kcu
                  on tc.constraint_catalog = kcu.constraint_catalog
                 and tc.constraint_schema = kcu.constraint_schema
                 and tc.constraint_name = kcu.constraint_name
                where tc.table_catalog = %s
                  and tc.table_schema = %s
                  and tc.table_name = %s
                  and tc.constraint_type = 'PRIMARY KEY'
                order by kcu.ordinal_position
                """,
                (spec.database_name, spec.schema_name, spec.object_name),
            )
            rows = cursor.fetchall()
        return [str(self._row_get(row, "column_name")) for row in rows if self._row_get(row, "column_name") is not None]

    def _cursor(self, connection: Any):
        dict_cursor = getattr(snowflake_connector, "DictCursor", None) if snowflake_connector is not None else None
        if dict_cursor is not None:
            try:
                return connection.cursor(dict_cursor)
            except TypeError:
                pass
        return connection.cursor()

    def _primary_key_columns(self) -> list[str]:
        columns = self.config.options.get("primary_key_columns")
        if isinstance(columns, list) and columns and all(isinstance(item, str) and item.strip() for item in columns):
            return [item.strip() for item in columns]
        single_column = str(self.config.options.get("primary_key_column", "id")).strip() or "id"
        return [single_column]

    def _parse_object_ref(self, raw_name: str, default_object_type: str) -> tuple[str, str]:
        name = raw_name.strip()
        if ":" in name:
            prefix, _, suffix = name.partition(":")
            object_type = prefix.strip().lower()
            if object_type not in {"table", "view"}:
                raise ValueError(f"snowflake tracked object prefix must be 'table' or 'view': {raw_name}")
            if not suffix.strip():
                raise ValueError(f"snowflake tracked object name must be non-empty: {raw_name}")
            return object_type, suffix.strip()
        return default_object_type, name

    def _cdc_summary(self) -> dict[str, object]:
        return self.build_cdc_summary(
            extra={
                "runtime": self.build_polling_runtime_summary(
                    cursor=None,
                    configured_runtime_mode=self.capture_mode(),
                    effective_runtime_mode=self.capture_mode(),
                    notification_aware=False,
                ),
            }
        )

    def _row_get(self, row: Any, key: str) -> Any:
        if isinstance(row, dict):
            if key in row:
                return row[key]
            lowered = key.lower()
            for candidate_key, value in row.items():
                if str(candidate_key).lower() == lowered:
                    return value
        return getattr(row, key, None)

    def _parse_checkpoint(self, checkpoint: SourceCheckpoint | None) -> dict[str, dict[str, Any]]:
        if checkpoint is None or not checkpoint.token:
            return {}
        parsed = json.loads(checkpoint.token)
        if not isinstance(parsed, dict):
            raise ValueError(f"{self.source_type} checkpoint token must be a JSON object")
        return {str(key): value for key, value in parsed.items() if isinstance(value, dict)}

    def _fetch_rows(self, connection: Any, spec: SnowflakeObjectSpec, object_state: dict[str, Any] | None) -> list[dict[str, Any]]:
        select_columns = ", ".join(self._quote_identifier(column) for column in spec.selected_columns)
        order_columns = [spec.watermark_column, *spec.primary_key_columns]
        order_by = ", ".join(self._quote_identifier(column) for column in order_columns)
        sql = (
            f"select {select_columns} "
            f"from {self._qualified_object(spec.database_name, spec.schema_name, spec.object_name)} "
            f"where {self._quote_identifier(spec.watermark_column)} is not null"
        )
        params: list[Any] = []
        if object_state:
            pk_values = list(object_state.get("pk") or [])
            if len(pk_values) != len(spec.primary_key_columns):
                raise ValueError(f"{self.source_type} checkpoint for {spec.key} has invalid primary key state")
            watermark_value = object_state.get("watermark")
            comparisons = [f"{self._quote_identifier(column)} > %s" for column in spec.primary_key_columns]
            sql += (
                f" and ("
                f"{self._quote_identifier(spec.watermark_column)} > %s "
                f"or ("
                f"{self._quote_identifier(spec.watermark_column)} = %s and ("
                + " or ".join(self._lexicographic_pk_predicates(spec.primary_key_columns))
                + ")))"
            )
            params.extend([watermark_value, watermark_value, *pk_values * len(spec.primary_key_columns)])
        sql += f" order by {order_by}"
        row_limit = int(self.config.options.get("row_limit", self.config.batch_size))
        if row_limit > 0:
            sql += " limit %s"
            params.append(row_limit)
        with self._cursor(connection) as cursor:
            cursor.execute(sql, params or None)
            rows = cursor.fetchall()
        return list(rows)

    def _lexicographic_pk_predicates(self, primary_key_columns: list[str]) -> list[str]:
        predicates: list[str] = []
        for index, column in enumerate(primary_key_columns):
            parts = [f"{self._quote_identifier(previous)} = %s" for previous in primary_key_columns[:index]]
            parts.append(f"{self._quote_identifier(column)} > %s")
            predicates.append("(" + " and ".join(parts) + ")")
        return predicates

    def _sort_key(self, spec: SnowflakeObjectSpec, row: dict[str, Any]) -> tuple[Any, ...]:
        return (
            self._normalize_timestamp(self._row_get(row, spec.watermark_column)),
            *[self._coerce_pk_value(self._row_get(row, column)) for column in spec.primary_key_columns],
        )

    def _normalize_value(self, value: Any) -> Any:
        if isinstance(value, datetime):
            normalized = value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
            return normalized.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        if isinstance(value, date) and not isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, time):
            normalized = value.replace(microsecond=0)
            return normalized.isoformat()
        if isinstance(value, Decimal):
            return format(value, "f")
        if isinstance(value, bytes):
            return value.hex()
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

    def _pk_marker(self, primary_key: dict[str, Any], primary_key_columns: list[str]) -> str:
        return ":".join(str(self._coerce_pk_value(primary_key[column])) for column in primary_key_columns)

    def _qualified_object(self, database_name: str, schema_name: str, object_name: str) -> str:
        return ".".join(
            (
                self._quote_identifier(database_name),
                self._quote_identifier(schema_name),
                self._quote_identifier(object_name),
            )
        )

    def _quote_identifier(self, value: str) -> str:
        return '"' + value.replace('"', '""') + '"'

    def _spec_summary(self, spec: SnowflakeObjectSpec) -> dict[str, object]:
        return {
            "database_name": spec.database_name,
            "schema_name": spec.schema_name,
            "table_name": spec.object_name,
            "object_type": spec.object_type,
            "watermark_column": spec.watermark_column,
            "commit_timestamp_column": spec.commit_timestamp_column,
            "primary_key_columns": list(spec.primary_key_columns),
            "selected_columns": list(spec.selected_columns),
        }

    def _spec_signature_entry(self, spec: SnowflakeObjectSpec) -> dict[str, object]:
        return {
            "key": spec.key,
            "object_type": spec.object_type,
            "watermark_column": spec.watermark_column,
            "commit_timestamp_column": spec.commit_timestamp_column,
            "primary_key_columns": list(spec.primary_key_columns),
            "selected_columns": list(spec.selected_columns),
        }
