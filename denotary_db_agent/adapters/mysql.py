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
    import pymysql
    from pymysql.cursors import DictCursor
except ImportError:  # pragma: no cover - handled in runtime validation
    pymysql = None
    DictCursor = None

try:
    from pymysqlreplication import BinLogStreamReader
    from pymysqlreplication.row_event import DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent
except ImportError:  # pragma: no cover - handled in runtime validation
    BinLogStreamReader = None
    DeleteRowsEvent = None
    UpdateRowsEvent = None
    WriteRowsEvent = None


@dataclass
class MySqlTableSpec:
    schema_name: str
    table_name: str
    watermark_column: str
    commit_timestamp_column: str
    primary_key_columns: list[str]
    selected_columns: list[str]

    @property
    def key(self) -> str:
        return f"{self.schema_name}.{self.table_name}"


@dataclass
class MySqlBinlogCheckpoint:
    log_file: str
    log_pos: int
    row_index: int = 0

    @property
    def token(self) -> str:
        return json.dumps(
            {
                "mode": "binlog",
                "log_file": self.log_file,
                "log_pos": self.log_pos,
                "row_index": self.row_index,
            },
            sort_keys=True,
        )


class MySqlAdapter(BaseAdapter):
    source_type = "mysql"
    minimum_version = "8.0"
    adapter_notes = (
        "MySQL supports both watermark-based snapshot polling and a shared row-based binlog CDC baseline. "
        "Live binlog harness validation remains the next MySQL-specific step."
    )

    def __init__(self, config):
        super().__init__(config)
        self._binlog_stream: Any | None = None
        self._binlog_stream_identity: tuple[str, int] | None = None

    def discover_capabilities(self) -> AdapterCapabilities:
        capture_mode = self._capture_mode()
        return AdapterCapabilities(
            source_type=self.source_type,
            minimum_version=self.minimum_version,
            supports_cdc=True,
            supports_snapshot=True,
            operations=("snapshot",) if capture_mode == "watermark" else ("insert", "update", "delete"),
            capture_modes=("watermark", "binlog"),
            cdc_modes=("binlog",),
            default_capture_mode="watermark",
            bootstrap_requirements=(
                ("tracked tables visible", "watermark columns configured")
                if capture_mode == "watermark"
                else ("tracked tables visible", "row-based binlog enabled", "replication privileges configured")
            ),
            notes=self.adapter_notes,
        )

    def validate_connection(self) -> None:
        required = ("host", "port", "username", "database")
        missing = [name for name in required if not self.config.connection.get(name)]
        if missing:
            raise ValueError(f"{self.source_type} connection is missing required fields: {', '.join(missing)}")
        if pymysql is None:
            raise RuntimeError(f"PyMySQL is required for live {self.source_type} adapter use")
        if self.config.options.get("dry_run_events"):
            return
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute("select version() as version")
                cursor.fetchone()
            self._load_table_specs(connection)
            if self._capture_mode() == "binlog":
                self._validate_binlog_prerequisites(connection)

    def bootstrap(self) -> dict:
        if self.config.options.get("dry_run_events"):
            summary = super().bootstrap()
            summary.update(
                {
                    "capture_mode": self._capture_mode(),
                    "tracked_tables": [],
                    "cdc": None,
                }
            )
            return summary
        self.validate_connection()
        with self._connect() as connection:
            specs = self._load_table_specs(connection)
            cdc = self._cdc_summary(connection)
        return {
            "source_id": self.config.id,
            "adapter": self.config.adapter,
            "capture_mode": self._capture_mode(),
            "tracked_tables": [self._spec_summary(spec) for spec in specs],
            "cdc": cdc,
        }

    def inspect(self) -> dict:
        if self.config.options.get("dry_run_events"):
            details = super().inspect()
            details.update(
                {
                    "capture_mode": self._capture_mode(),
                    "tracked_tables": [],
                    "cdc": None,
                }
            )
            return details
        capabilities = self.discover_capabilities()
        with self._connect() as connection:
            specs = self._load_table_specs(connection)
            cdc = self._cdc_summary(connection)
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
            "tracked_tables": [self._spec_summary(spec) for spec in specs],
            "cdc": cdc,
            "notes": capabilities.notes,
        }

    def runtime_signature(self) -> str:
        if self.config.options.get("dry_run_events"):
            return super().runtime_signature()
        with self._connect() as connection:
            specs = self._load_table_specs(connection)
        payload = {
            "adapter": self.config.adapter,
            "source_id": self.config.id,
            "capture_mode": self._capture_mode(),
            "tracked_tables": [self._spec_signature_entry(spec) for spec in specs],
            "include": self.config.include,
        }
        return json.dumps(payload, sort_keys=True)

    def refresh_runtime(self) -> dict:
        return self.bootstrap()

    def start_stream(self, checkpoint: SourceCheckpoint | None):
        if self._capture_mode() != "binlog":
            raise NotImplementedError(f"{self.source_type} CDC streaming is only available when capture_mode is set to binlog")
        if self.config.options.get("dry_run_events"):
            return iter(())

        with self._connect() as connection:
            specs = self._load_table_specs(connection)
            self._validate_binlog_prerequisites(connection)

        spec_map = {spec.key: spec for spec in specs}
        checkpoint_state = self._parse_binlog_checkpoint(checkpoint)
        stream = self._ensure_binlog_stream(checkpoint_state)
        row_limit = int(self.config.options.get("row_limit", self.config.batch_size))
        pending: list[ChangeEvent] = []
        for raw_event in stream:
            event_rows = self._decode_binlog_event_rows(raw_event, spec_map)
            if not event_rows:
                continue
            for row_index, event in enumerate(event_rows, start=1):
                if (
                    checkpoint_state is not None
                    and event["log_file"] == checkpoint_state.log_file
                    and int(event["log_pos"]) == checkpoint_state.log_pos
                    and row_index <= checkpoint_state.row_index
                ):
                    continue
                pending.append(
                    self._build_binlog_change_event(
                        spec=event["spec"],
                        operation=str(event["operation"]),
                        before_row=event["before_row"],
                        after_row=event["after_row"],
                        event_timestamp=event["event_timestamp"],
                        log_file=event["log_file"],
                        log_pos=int(event["log_pos"]),
                        row_index=row_index,
                    )
                )
                if len(pending) >= row_limit:
                    return iter(pending)
        return iter(pending)

    def stop_stream(self) -> None:
        if self._binlog_stream is not None:
            try:
                self._binlog_stream.close()
            except Exception:
                pass
        self._binlog_stream = None
        self._binlog_stream_identity = None
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

        with self._connect() as connection:
            specs = self._load_table_specs(connection)
            candidates: list[tuple[MySqlTableSpec, dict[str, Any]]] = []
            for spec in specs:
                table_state = checkpoint_state.get(spec.key)
                candidates.extend((spec, row) for row in self._fetch_rows(connection, spec, table_state))

        candidates.sort(key=lambda item: self._sort_key(item[0], item[1]))
        current_state = dict(checkpoint_state)
        for spec, row in candidates:
            commit_timestamp = self._normalize_timestamp(row[spec.commit_timestamp_column])
            primary_key = {column: row[column] for column in spec.primary_key_columns}
            current_state[spec.key] = {
                "watermark": self._normalize_timestamp(row[spec.watermark_column]),
                "pk": [self._coerce_pk_value(primary_key[column]) for column in spec.primary_key_columns],
            }
            payload_after = {column: self._normalize_value(row[column]) for column in spec.selected_columns}
            yield ChangeEvent(
                source_id=self.config.id,
                source_type=self.source_type,
                source_instance=self.config.source_instance,
                database_name=self.config.database_name,
                schema_or_namespace=spec.schema_name,
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
        if pymysql is None:
            raise RuntimeError(f"PyMySQL is required for live {self.source_type} adapter use")
        connection = pymysql.connect(
            host=str(self.config.connection["host"]),
            port=int(self.config.connection["port"]),
            user=str(self.config.connection["username"]),
            password=str(self.config.connection.get("password", "")),
            database=str(self.config.connection["database"]),
            cursorclass=DictCursor,
            autocommit=True,
        )
        try:
            yield connection
        finally:
            connection.close()

    def _capture_mode(self) -> str:
        return self.capture_mode()

    def _cdc_summary(self, connection: Any) -> dict[str, Any] | None:
        if self._capture_mode() != "binlog":
            return None
        binlog_status = self._read_server_variable(connection, "log_bin")
        binlog_format = self._read_server_variable(connection, "binlog_format")
        binlog_row_image = self._read_server_variable(connection, "binlog_row_image")
        return {
            "capture_mode": "binlog",
            "log_bin": str(binlog_status).upper() in {"ON", "1"},
            "binlog_format": str(binlog_format or "").upper(),
            "binlog_row_image": str(binlog_row_image or "").upper(),
            "stream_active": self._binlog_stream is not None,
            "server_id": int(self.config.options.get("binlog_server_id", 1001)),
        }

    def _row_get(self, row: dict[str, Any], key: str) -> Any:
        if key in row:
            return row[key]
        upper_key = key.upper()
        if upper_key in row:
            return row[upper_key]
        lower_map = {str(existing).lower(): value for existing, value in row.items()}
        if key.lower() in lower_map:
            return lower_map[key.lower()]
        raise KeyError(key)

    def _load_table_specs(self, connection: Any) -> list[MySqlTableSpec]:
        include = self.config.include or {self.config.database_name: []}
        watermark_column = str(self.config.options.get("watermark_column", "updated_at"))
        commit_timestamp_column = str(self.config.options.get("commit_timestamp_column", watermark_column))
        specs: list[MySqlTableSpec] = []
        with connection.cursor() as cursor:
            for schema_name, tables in include.items():
                target_schema = schema_name or self.config.database_name
                if not tables:
                    raise ValueError(f"{self.source_type} include must list explicit tables for the current baseline")
                for table_name in tables:
                    cursor.execute(
                        """
                        select
                            column_name,
                            data_type,
                            ordinal_position,
                            column_key = 'PRI' as is_primary_key
                        from information_schema.columns
                        where table_schema = %s and table_name = %s
                        order by ordinal_position
                        """,
                        (target_schema, table_name),
                    )
                    rows = cursor.fetchall()
                    if not rows:
                        raise ValueError(f"{self.source_type} table {target_schema}.{table_name} was not found")
                    columns = [str(self._row_get(row, "column_name")) for row in rows]
                    primary_key_columns = [
                        str(self._row_get(row, "column_name"))
                        for row in rows
                        if self._row_get(row, "is_primary_key")
                    ]
                    if not primary_key_columns:
                        raise ValueError(f"{self.source_type} table {target_schema}.{table_name} must have a primary key")
                    if watermark_column not in columns:
                        raise ValueError(
                            f"{self.source_type} table {target_schema}.{table_name} does not contain watermark column {watermark_column}"
                        )
                    if commit_timestamp_column not in columns:
                        raise ValueError(
                            f"{self.source_type} table {target_schema}.{table_name} does not contain commit timestamp column {commit_timestamp_column}"
                        )
                    specs.append(
                        MySqlTableSpec(
                            schema_name=target_schema,
                            table_name=table_name,
                            watermark_column=watermark_column,
                            commit_timestamp_column=commit_timestamp_column,
                            primary_key_columns=primary_key_columns,
                            selected_columns=columns,
                        )
                    )
        return specs

    def _read_server_variable(self, connection: Any, variable_name: str) -> str:
        with connection.cursor() as cursor:
            cursor.execute("show variables like %s", (variable_name,))
            row = cursor.fetchone() or {}
        return str(self._row_get(row, "value")) if row else ""

    def _validate_binlog_prerequisites(self, connection: Any) -> None:
        if BinLogStreamReader is None or WriteRowsEvent is None or UpdateRowsEvent is None or DeleteRowsEvent is None:
            raise RuntimeError("mysql-replication is required for binlog capture mode")
        if str(self._read_server_variable(connection, "log_bin")).upper() not in {"ON", "1"}:
            raise ValueError(f"{self.source_type} log_bin must be enabled for capture_mode=binlog")
        if str(self._read_server_variable(connection, "binlog_format")).upper() != "ROW":
            raise ValueError(f"{self.source_type} binlog_format must be ROW for capture_mode=binlog")
        row_image = str(self._read_server_variable(connection, "binlog_row_image")).upper()
        if row_image and row_image != "FULL":
            raise ValueError(f"{self.source_type} binlog_row_image must be FULL for capture_mode=binlog")

    def _fetch_rows(self, connection: Any, spec: MySqlTableSpec, table_state: dict[str, Any] | None) -> list[dict[str, Any]]:
        select_columns = ", ".join(self._quote_identifier(column) for column in spec.selected_columns)
        order_columns = [spec.watermark_column, *spec.primary_key_columns]
        order_by = ", ".join(self._quote_identifier(column) for column in order_columns)
        sql = (
            f"select {select_columns} "
            f"from {self._qualified_table(spec.schema_name, spec.table_name)} "
            f"where {self._quote_identifier(spec.watermark_column)} is not null"
        )
        params: list[Any] = []
        if table_state:
            pk_values = list(table_state.get("pk") or [])
            if len(pk_values) != len(spec.primary_key_columns):
                raise ValueError(f"{self.source_type} checkpoint for {spec.key} has invalid primary key state")
            watermark_value = table_state.get("watermark")
            comparisons = [
                f"{self._quote_identifier(column)} > %s"
                for column in spec.primary_key_columns
            ]
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
        with connection.cursor() as cursor:
            cursor.execute(sql, params)
            return list(cursor.fetchall())

    def _parse_binlog_checkpoint(self, checkpoint: SourceCheckpoint | None) -> MySqlBinlogCheckpoint | None:
        if checkpoint is None or not checkpoint.token:
            return None
        payload = json.loads(checkpoint.token)
        if not isinstance(payload, dict):
            raise ValueError(f"{self.source_type} binlog checkpoint token must be a JSON object")
        if payload.get("mode") != "binlog":
            return None
        return MySqlBinlogCheckpoint(
            log_file=str(payload["log_file"]),
            log_pos=int(payload["log_pos"]),
            row_index=int(payload.get("row_index", 0)),
        )

    def _ensure_binlog_stream(self, checkpoint: MySqlBinlogCheckpoint | None) -> Any:
        requested_identity = (
            checkpoint.log_file if checkpoint is not None else str(self.config.options.get("binlog_start_file", "")),
            checkpoint.log_pos if checkpoint is not None else int(self.config.options.get("binlog_start_pos", 4)),
        )
        if self._binlog_stream is not None and self._binlog_stream_identity == requested_identity:
            return self._binlog_stream
        if self._binlog_stream is not None:
            try:
                self._binlog_stream.close()
            except Exception:
                pass
        stream_kwargs = {
            "connection_settings": {
                "host": str(self.config.connection["host"]),
                "port": int(self.config.connection["port"]),
                "user": str(self.config.connection["username"]),
                "passwd": str(self.config.connection.get("password", "")),
            },
            "server_id": int(self.config.options.get("binlog_server_id", 1001)),
            "blocking": bool(self.config.options.get("binlog_blocking", False)),
            "only_events": [WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
            "only_schemas": list(self.config.include.keys()) if self.config.include else [self.config.database_name],
            "resume_stream": checkpoint is not None or bool(requested_identity[0]) or requested_identity[1] > 4,
        }
        if requested_identity[0]:
            stream_kwargs["log_file"] = requested_identity[0]
        if requested_identity[1]:
            stream_kwargs["log_pos"] = requested_identity[1]
        self._binlog_stream = BinLogStreamReader(**stream_kwargs)
        self._binlog_stream_identity = requested_identity
        return self._binlog_stream

    def _decode_binlog_event_rows(self, raw_event: Any, spec_map: dict[str, MySqlTableSpec]) -> list[dict[str, Any]]:
        schema_name = self._decode_binlog_identifier(getattr(raw_event, "schema", ""))
        table_name = self._decode_binlog_identifier(getattr(raw_event, "table", ""))
        spec = spec_map.get(f"{schema_name}.{table_name}")
        if spec is None:
            return []
        packet = getattr(raw_event, "packet", None)
        stream_log_pos = int(getattr(self._binlog_stream, "log_pos", 0) or 0)
        packet_log_pos = int(getattr(packet, "log_pos", 0) or 0)
        next_log_pos = int(packet_log_pos or stream_log_pos or 0)
        event_size = int(getattr(packet, "event_size", 0) or 0)
        if packet_log_pos and event_size:
            event_log_pos = max(4, packet_log_pos - event_size)
        else:
            event_log_pos = next_log_pos if next_log_pos else max(4, stream_log_pos)
        log_file = str(getattr(self._binlog_stream, "log_file", "")) or str(self.config.options.get("binlog_start_file", ""))
        event_timestamp = self._normalize_binlog_timestamp(getattr(raw_event, "timestamp", 0))
        decoded: list[dict[str, Any]] = []
        for row in list(getattr(raw_event, "rows", []) or []):
            if WriteRowsEvent is not None and isinstance(raw_event, WriteRowsEvent):
                after_row = self._normalize_row_payload(spec, row.get("values") or {})
                decoded.append(
                    {
                        "spec": spec,
                        "operation": "insert",
                        "before_row": None,
                        "after_row": after_row,
                        "event_timestamp": event_timestamp,
                        "log_file": log_file,
                        "log_pos": event_log_pos,
                    }
                )
            elif UpdateRowsEvent is not None and isinstance(raw_event, UpdateRowsEvent):
                before_row = self._normalize_row_payload(spec, row.get("before_values") or {})
                after_row = self._normalize_row_payload(spec, row.get("after_values") or {})
                decoded.append(
                    {
                        "spec": spec,
                        "operation": "update",
                        "before_row": before_row,
                        "after_row": after_row,
                        "event_timestamp": event_timestamp,
                        "log_file": log_file,
                        "log_pos": event_log_pos,
                    }
                )
            elif DeleteRowsEvent is not None and isinstance(raw_event, DeleteRowsEvent):
                before_row = self._normalize_row_payload(spec, row.get("values") or {})
                decoded.append(
                    {
                        "spec": spec,
                        "operation": "delete",
                        "before_row": before_row,
                        "after_row": None,
                        "event_timestamp": event_timestamp,
                        "log_file": log_file,
                        "log_pos": event_log_pos,
                    }
                )
        return decoded

    def _build_binlog_change_event(
        self,
        spec: MySqlTableSpec,
        operation: str,
        before_row: dict[str, Any] | None,
        after_row: dict[str, Any] | None,
        event_timestamp: str,
        log_file: str,
        log_pos: int,
        row_index: int,
    ) -> ChangeEvent:
        primary_key_source = after_row or before_row or {}
        primary_key = {
            column: primary_key_source.get(column)
            for column in spec.primary_key_columns
        }
        checkpoint = MySqlBinlogCheckpoint(log_file=log_file, log_pos=log_pos, row_index=row_index)
        return ChangeEvent(
            source_id=self.config.id,
            source_type=self.source_type,
            source_instance=self.config.source_instance,
            database_name=self.config.database_name,
            schema_or_namespace=spec.schema_name,
            table_or_collection=spec.table_name,
            operation=operation,  # type: ignore[arg-type]
            primary_key=primary_key,
            change_version=f"{log_file}:{log_pos}:{row_index}",
            commit_timestamp=event_timestamp,
            before=before_row,
            after=after_row,
            metadata={
                "capture_mode": "binlog-cdc",
                "log_file": log_file,
                "log_pos": log_pos,
                "row_index": row_index,
                "primary_key_columns": spec.primary_key_columns,
            },
            checkpoint_token=checkpoint.token,
        )

    def _parse_checkpoint(self, checkpoint: SourceCheckpoint | None) -> dict[str, dict[str, Any]]:
        if checkpoint is None or not checkpoint.token:
            return {}
        payload = json.loads(checkpoint.token)
        if not isinstance(payload, dict):
            raise ValueError(f"{self.source_type} checkpoint token must be a JSON object")
        return payload

    def _decode_binlog_identifier(self, value: Any) -> str:
        if isinstance(value, bytes):
            return value.decode("utf-8")
        return str(value)

    def _normalize_binlog_timestamp(self, value: Any) -> str:
        if isinstance(value, (int, float)) and value > 0:
            timestamp = datetime.fromtimestamp(float(value), tz=timezone.utc).replace(microsecond=0)
            return timestamp.isoformat().replace("+00:00", "Z")
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    def _normalize_row_payload(self, spec: MySqlTableSpec, row: dict[str, Any]) -> dict[str, Any]:
        normalized_row = row
        if row and all(str(column).startswith("UNKNOWN_COL") for column in row):
            normalized_row = {
                spec.selected_columns[index]: value
                for index, value in enumerate(row.values())
                if index < len(spec.selected_columns)
            }
        return {
            str(column): self._normalize_value(value)
            for column, value in normalized_row.items()
        }

    def _sort_key(self, spec: MySqlTableSpec, row: dict[str, Any]) -> tuple[Any, ...]:
        return (
            self._normalize_timestamp(row[spec.watermark_column]),
            *[self._coerce_pk_value(row[column]) for column in spec.primary_key_columns],
        )

    def _coerce_pk_value(self, value: Any) -> Any:
        if isinstance(value, Decimal):
            return format(value, "f")
        if isinstance(value, (datetime, date, time)):
            return self._normalize_value(value)
        return value

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
        if not isinstance(normalized, str):
            raise ValueError(f"{self.source_type} timestamp value must normalize to string, got {type(normalized)!r}")
        return normalized

    def _pk_marker(self, primary_key: dict[str, Any], primary_key_columns: list[str]) -> str:
        return ":".join(str(self._coerce_pk_value(primary_key[column])) for column in primary_key_columns)

    def _lexicographic_pk_predicates(self, columns: list[str]) -> list[str]:
        predicates: list[str] = []
        for index, column in enumerate(columns):
            equalities = [
                f"{self._quote_identifier(previous)} = %s"
                for previous in columns[:index]
            ]
            predicate_parts = [*equalities, f"{self._quote_identifier(column)} > %s"]
            predicates.append("(" + " and ".join(predicate_parts) + ")")
        return predicates

    def _qualified_table(self, schema_name: str, table_name: str) -> str:
        return f"{self._quote_identifier(schema_name)}.{self._quote_identifier(table_name)}"

    def _quote_identifier(self, value: str) -> str:
        return f"`{value.replace('`', '``')}`"

    def _spec_summary(self, spec: MySqlTableSpec) -> dict[str, Any]:
        return {
            "schema_name": spec.schema_name,
            "table_name": spec.table_name,
            "watermark_column": spec.watermark_column,
            "commit_timestamp_column": spec.commit_timestamp_column,
            "primary_key_columns": list(spec.primary_key_columns),
            "selected_columns": list(spec.selected_columns),
        }

    def _spec_signature_entry(self, spec: MySqlTableSpec) -> dict[str, Any]:
        return {
            "key": spec.key,
            "watermark_column": spec.watermark_column,
            "commit_timestamp_column": spec.commit_timestamp_column,
            "primary_key_columns": list(spec.primary_key_columns),
            "selected_columns": list(spec.selected_columns),
        }
