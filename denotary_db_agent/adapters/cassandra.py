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
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.cluster import Cluster
except ImportError:  # pragma: no cover - handled in runtime validation
    Cluster = None
    PlainTextAuthProvider = None


@dataclass
class CassandraTableSpec:
    keyspace_name: str
    table_name: str
    watermark_column: str
    commit_timestamp_column: str
    primary_key_columns: list[str]
    selected_columns: list[str]

    @property
    def key(self) -> str:
        return f"{self.keyspace_name}.{self.table_name}"


class CassandraAdapter(BaseAdapter):
    source_type = "cassandra"
    minimum_version = "4.0"
    adapter_notes = (
        "Wave 2 Cassandra support currently provides connection-shape validation, live cluster ping, "
        "tracked-table introspection, watermark snapshot polling, deterministic checkpoint resume, and "
        "dry-run snapshot playback. Native Cassandra CDC can be added later if commercially justified."
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
            bootstrap_requirements=("tracked tables visible", "watermark columns configured"),
            checkpoint_strategy="table_watermark",
            activity_model="polling",
            notes=self.adapter_notes,
        )

    def validate_connection(self) -> None:
        if not self._contact_points():
            raise ValueError(f"{self.source_type} connection is missing required field: host or hosts")
        if self.config.options.get("dry_run_events"):
            self._configured_table_specs()
            return
        if Cluster is None:
            raise RuntimeError("cassandra-driver is required for live cassandra adapter use")
        with self._connect() as session:
            session.execute("select release_version from system.local")
            self._load_table_specs(session)

    def bootstrap(self) -> dict:
        self.validate_connection()
        specs = self._load_live_or_configured_specs()
        return self.build_bootstrap_result(
            tracked_key="tracked_tables",
            tracked_items=[self._spec_summary(spec) for spec in specs],
            cdc=self._cdc_summary(),
        )

    def inspect(self) -> dict:
        self.validate_connection()
        specs = self._load_live_or_configured_specs()
        return self.build_inspect_result(
            tracked_key="tracked_tables",
            tracked_items=[self._spec_summary(spec) for spec in specs],
            cdc=self._cdc_summary(),
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
        raise NotImplementedError("cassandra native CDC is not implemented yet; use capture_mode=watermark")

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
                schema_or_namespace=str(item.get("schema_or_namespace", self.config.database_name)),
                table_or_collection=str(item.get("table_or_collection", "records")),
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

        with self._connect() as session:
            specs = self._load_table_specs(session)
            candidates: list[tuple[CassandraTableSpec, dict[str, Any]]] = []
            for spec in specs:
                table_state = checkpoint_state.get(spec.key)
                candidates.extend((spec, row) for row in self._fetch_rows(session, spec, table_state))

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
                schema_or_namespace=spec.keyspace_name,
                table_or_collection=spec.table_name,
                operation="snapshot",
                primary_key={key: self._normalize_value(value) for key, value in primary_key.items()},
                change_version=f"{spec.key}:{commit_timestamp}:{self._pk_marker(primary_key, spec.primary_key_columns)}",
                commit_timestamp=commit_timestamp,
                before=None,
                after=payload_after,
                metadata={
                    "capture_mode": "watermark-poll",
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
        assert Cluster is not None
        cluster_kwargs: dict[str, Any] = {
            "contact_points": self._contact_points(),
            "port": int(self.config.connection.get("port", 9042)),
        }
        username = self.config.connection.get("username")
        password = self.config.connection.get("password")
        if username and password and PlainTextAuthProvider is not None:
            cluster_kwargs["auth_provider"] = PlainTextAuthProvider(username=str(username), password=str(password))
        cluster = Cluster(**cluster_kwargs)
        session = cluster.connect()
        try:
            yield session
        finally:
            cluster.shutdown()

    def _contact_points(self) -> list[str]:
        raw_hosts = self.config.connection.get("hosts")
        if isinstance(raw_hosts, list):
            hosts = [str(item).strip() for item in raw_hosts if str(item).strip()]
            if hosts:
                return hosts
        host = str(self.config.connection.get("host", "")).strip()
        return [host] if host else []

    def _load_live_or_configured_specs(self) -> list[CassandraTableSpec]:
        if self.config.options.get("dry_run_events"):
            return self._configured_table_specs()
        if Cluster is None:
            return self._configured_table_specs()
        with self._connect() as session:
            return self._load_table_specs(session)

    def _configured_table_specs(self) -> list[CassandraTableSpec]:
        include = self.config.include or {}
        watermark_column = str(self.config.options.get("watermark_column", "updated_at")).strip() or "updated_at"
        commit_timestamp_column = str(self.config.options.get("commit_timestamp_column", watermark_column)).strip() or watermark_column
        primary_key_columns = self._primary_key_columns()
        specs: list[CassandraTableSpec] = []
        for keyspace_name, table_names in include.items():
            if not isinstance(keyspace_name, str) or not keyspace_name.strip():
                raise ValueError("cassandra include keyspace names must be non-empty strings")
            normalized_keyspace = keyspace_name.strip()
            if not isinstance(table_names, list) or not all(isinstance(item, str) and item.strip() for item in table_names):
                raise ValueError(f"cassandra include[{normalized_keyspace}] must be an array of non-empty table names")
            for raw_table_name in table_names:
                table_name = raw_table_name.strip()
                specs.append(
                    CassandraTableSpec(
                        keyspace_name=normalized_keyspace,
                        table_name=table_name,
                        watermark_column=watermark_column,
                        commit_timestamp_column=commit_timestamp_column,
                        primary_key_columns=list(primary_key_columns),
                        selected_columns=[*primary_key_columns, watermark_column, commit_timestamp_column],
                    )
                )
        return specs

    def _load_table_specs(self, session: Any) -> list[CassandraTableSpec]:
        configured = self._configured_table_specs()
        specs: list[CassandraTableSpec] = []
        for item in configured:
            columns = self._load_table_columns(session, item)
            column_lookup = {column.lower(): column for column in columns}
            actual_watermark_column = column_lookup.get(item.watermark_column.lower())
            if actual_watermark_column is None:
                raise ValueError(
                    f"{self.source_type} table {item.keyspace_name}.{item.table_name} does not contain watermark column {item.watermark_column}"
                )
            actual_commit_timestamp_column = column_lookup.get(item.commit_timestamp_column.lower())
            if actual_commit_timestamp_column is None:
                raise ValueError(
                    f"{self.source_type} table {item.keyspace_name}.{item.table_name} does not contain commit timestamp column {item.commit_timestamp_column}"
                )
            actual_primary_key_columns = [column_lookup.get(column.lower()) for column in item.primary_key_columns]
            if any(column is None for column in actual_primary_key_columns):
                missing = [column for column, actual in zip(item.primary_key_columns, actual_primary_key_columns) if actual is None]
                raise ValueError(
                    f"{self.source_type} table {item.keyspace_name}.{item.table_name} does not contain primary key column(s): {', '.join(missing)}"
                )
            discovered_primary_keys = self._load_primary_key_columns(session, item)
            primary_key_columns = discovered_primary_keys or [str(column) for column in actual_primary_key_columns]
            specs.append(
                CassandraTableSpec(
                    keyspace_name=item.keyspace_name,
                    table_name=item.table_name,
                    watermark_column=actual_watermark_column,
                    commit_timestamp_column=actual_commit_timestamp_column,
                    primary_key_columns=primary_key_columns,
                    selected_columns=columns,
                )
            )
        return specs

    def _load_table_columns(self, session: Any, spec: CassandraTableSpec) -> list[str]:
        rows = session.execute(
            """
            select column_name
            from system_schema.columns
            where keyspace_name = %s
              and table_name = %s
            """,
            (spec.keyspace_name, spec.table_name),
        )
        columns = [
            str(self._row_get(row, "column_name"))
            for row in sorted(list(rows), key=lambda item: int(self._row_get(item, "position") or 0))
            if self._row_get(row, "column_name") is not None
        ]
        if not columns:
            raise ValueError(f"{self.source_type} table {spec.keyspace_name}.{spec.table_name} is not visible in system_schema.columns")
        return columns

    def _load_primary_key_columns(self, session: Any, spec: CassandraTableSpec) -> list[str]:
        rows = session.execute(
            """
            select column_name, kind, position
            from system_schema.columns
            where keyspace_name = %s
              and table_name = %s
            """,
            (spec.keyspace_name, spec.table_name),
        )
        key_rows = [
            row
            for row in list(rows)
            if str(self._row_get(row, "kind") or "") in {"partition_key", "clustering"}
        ]
        key_rows.sort(key=lambda item: (0 if str(self._row_get(item, "kind")) == "partition_key" else 1, int(self._row_get(item, "position") or 0)))
        return [str(self._row_get(row, "column_name")) for row in key_rows if self._row_get(row, "column_name") is not None]

    def _primary_key_columns(self) -> list[str]:
        columns = self.config.options.get("primary_key_columns")
        if isinstance(columns, list) and columns and all(isinstance(item, str) and item.strip() for item in columns):
            return [item.strip() for item in columns]
        single_column = str(self.config.options.get("primary_key_column", "id")).strip() or "id"
        return [single_column]

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

    def _fetch_rows(self, session: Any, spec: CassandraTableSpec, table_state: dict[str, Any] | None) -> list[dict[str, Any]]:
        select_columns = ", ".join(self._quote_identifier(column) for column in spec.selected_columns)
        sql = (
            f"select {select_columns} "
            f"from {self._qualified_table(spec.keyspace_name, spec.table_name)} "
            f"where {self._quote_identifier(spec.watermark_column)} is not null"
        )
        params: list[Any] = []
        if table_state:
            watermark_value = table_state.get("watermark")
            sql += f" and {self._quote_identifier(spec.watermark_column)} >= %s"
            params.append(watermark_value)
        row_limit = int(self.config.options.get("row_limit", self.config.batch_size))
        if row_limit > 0:
            sql += " limit %s"
            params.append(row_limit)
        sql += " allow filtering"
        rows = list(session.execute(sql, tuple(params) if params else None))
        normalized_rows = [self._normalize_row(row) for row in rows]
        if table_state:
            normalized_rows = [
                row
                for row in normalized_rows
                if self._row_after_checkpoint(spec, row, table_state)
            ]
        return normalized_rows

    def _row_after_checkpoint(self, spec: CassandraTableSpec, row: dict[str, Any], table_state: dict[str, Any]) -> bool:
        watermark_value = self._normalize_timestamp(row.get(spec.watermark_column))
        checkpoint_watermark = str(table_state.get("watermark"))
        if watermark_value > checkpoint_watermark:
            return True
        if watermark_value < checkpoint_watermark:
            return False
        checkpoint_pk = list(table_state.get("pk") or [])
        row_pk = [self._coerce_pk_value(row.get(column)) for column in spec.primary_key_columns]
        return tuple(row_pk) > tuple(checkpoint_pk)

    def _normalize_row(self, row: Any) -> dict[str, Any]:
        if isinstance(row, dict):
            return dict(row)
        if hasattr(row, "_asdict"):
            return dict(row._asdict())
        if hasattr(row, "_fields"):
            return {field: getattr(row, field) for field in row._fields}
        return dict(getattr(row, "__dict__", {}))

    def _sort_key(self, spec: CassandraTableSpec, row: dict[str, Any]) -> tuple[Any, ...]:
        return (
            self._normalize_timestamp(row.get(spec.watermark_column)),
            *[self._coerce_pk_value(row.get(column)) for column in spec.primary_key_columns],
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

    def _qualified_table(self, keyspace_name: str, table_name: str) -> str:
        return ".".join((self._quote_identifier(keyspace_name), self._quote_identifier(table_name)))

    def _quote_identifier(self, value: str) -> str:
        return '"' + value.replace('"', '""') + '"'

    def _spec_summary(self, spec: CassandraTableSpec) -> dict[str, object]:
        return {
            "schema_name": spec.keyspace_name,
            "table_name": spec.table_name,
            "watermark_column": spec.watermark_column,
            "commit_timestamp_column": spec.commit_timestamp_column,
            "primary_key_columns": list(spec.primary_key_columns),
            "selected_columns": list(spec.selected_columns),
        }

    def _spec_signature_entry(self, spec: CassandraTableSpec) -> dict[str, object]:
        return {
            "key": spec.key,
            "watermark_column": spec.watermark_column,
            "commit_timestamp_column": spec.commit_timestamp_column,
            "primary_key_columns": list(spec.primary_key_columns),
            "selected_columns": list(spec.selected_columns),
        }
