from __future__ import annotations

import json
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal
from typing import Any, Iterator

from denotary_db_agent.adapters.base import AdapterCapabilities, BaseAdapter
from denotary_db_agent.models import ChangeEvent, SourceCheckpoint, utc_now

try:
    import pytds
except ImportError:  # pragma: no cover - handled in runtime validation
    pytds = None


@dataclass
class SqlServerTableSpec:
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
class SqlServerChangeTrackingCheckpoint:
    version: int
    table_key: str | None = None
    pk: list[Any] | None = None

    @property
    def token(self) -> str:
        payload: dict[str, Any] = {
            "mode": "change_tracking",
            "version": self.version,
        }
        if self.table_key:
            payload["table_key"] = self.table_key
        if self.pk is not None:
            payload["pk"] = list(self.pk)
        return json.dumps(payload, sort_keys=True)


class SqlServerAdapter(BaseAdapter):
    source_type = "sqlserver"
    minimum_version = "2019"
    adapter_notes = (
        "SQL Server supports both watermark-based snapshot polling and a native Change Tracking CDC baseline. "
        "Full SQL Server CDC capture remains the next SQL Server-specific step."
    )

    def __init__(self, config):
        super().__init__(config)
        self._change_tracking_baseline_version: int | None = None

    def discover_capabilities(self) -> AdapterCapabilities:
        capture_mode = self._capture_mode()
        return AdapterCapabilities(
            source_type=self.source_type,
            minimum_version=self.minimum_version,
            supports_cdc=True,
            supports_snapshot=True,
            operations=("snapshot",) if capture_mode == "watermark" else ("insert", "update", "delete"),
            capture_modes=("watermark", "change_tracking"),
            cdc_modes=("change_tracking",),
            default_capture_mode="watermark",
            bootstrap_requirements=(
                ("tracked tables visible", "watermark columns configured")
                if capture_mode == "watermark"
                else ("tracked tables visible", "database change tracking enabled", "tracked tables change tracking enabled")
            ),
            notes=self.adapter_notes,
        )

    def validate_connection(self) -> None:
        required = ("host", "port", "username", "database")
        missing = [name for name in required if not self.config.connection.get(name)]
        if missing:
            raise ValueError(f"{self.source_type} connection is missing required fields: {', '.join(missing)}")
        if pytds is None:
            raise RuntimeError("python-tds is required for live sqlserver adapter use")
        if self.config.options.get("dry_run_events"):
            return
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute("select @@version as version")
                cursor.fetchone()
            self._load_table_specs(connection)
            if self._capture_mode() == "change_tracking":
                self._validate_change_tracking_prerequisites(connection)

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
            "cdc_modes": list(capabilities.cdc_modes),
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
        if self._capture_mode() != "change_tracking":
            raise NotImplementedError(
                f"{self.source_type} CDC streaming is only available when capture_mode is set to change_tracking"
            )
        if self.config.options.get("dry_run_events"):
            return iter(())

        with self._connect() as connection:
            specs = self._load_table_specs(connection)
            current_version = self._current_change_tracking_version(connection)
            self._validate_change_tracking_prerequisites(connection, current_version=current_version)
            checkpoint_state = self._parse_change_tracking_checkpoint(checkpoint)
            if checkpoint_state is None:
                baseline_version = self._change_tracking_baseline_version
                if baseline_version is None:
                    if self.config.backfill_mode == "none":
                        baseline_version = current_version
                        self._change_tracking_baseline_version = baseline_version
                        return iter(())
                    baseline_version = 0
                checkpoint_state = SqlServerChangeTrackingCheckpoint(version=baseline_version)
            candidates = self._fetch_change_tracking_rows(connection, specs, checkpoint_state)
            if not candidates:
                self._change_tracking_baseline_version = checkpoint_state.version
                return iter(())

        row_limit = int(self.config.options.get("row_limit", self.config.batch_size))
        limited = candidates[: row_limit if row_limit > 0 else len(candidates)]
        pending: list[ChangeEvent] = []
        for index, item in enumerate(limited):
            next_item = limited[index + 1] if index + 1 < len(limited) else None
            has_more_same_version = next_item is not None and int(next_item["change_version"]) == int(item["change_version"])
            checkpoint_token = SqlServerChangeTrackingCheckpoint(
                version=int(item["change_version"]),
                table_key=str(item["table_key"]) if has_more_same_version else None,
                pk=list(item["pk_marker"]) if has_more_same_version else None,
            ).token
            pending.append(
                ChangeEvent(
                    source_id=self.config.id,
                    source_type=self.source_type,
                    source_instance=self.config.source_instance,
                    database_name=self.config.database_name,
                    schema_or_namespace=item["spec"].schema_name,
                    table_or_collection=item["spec"].table_name,
                    operation=item["operation"],
                    primary_key=item["primary_key"],
                    change_version=f"{item['table_key']}:{item['change_version']}:{self._pk_marker(item['primary_key'], item['spec'].primary_key_columns)}",
                    commit_timestamp=item["commit_timestamp"],
                    before=item["before"],
                    after=item["after"],
                    metadata=item["metadata"],
                    checkpoint_token=checkpoint_token,
                )
            )
        return iter(pending)

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
                schema_or_namespace=str(item.get("schema_or_namespace", "dbo")),
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
            candidates: list[tuple[SqlServerTableSpec, dict[str, Any]]] = []
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
        if pytds is None:
            raise RuntimeError("python-tds is required for live sqlserver adapter use")
        connection = pytds.connect(
            dsn=str(self.config.connection["host"]),
            port=int(self.config.connection["port"]),
            database=str(self.config.connection["database"]),
            user=str(self.config.connection["username"]),
            password=str(self.config.connection.get("password", "")),
            as_dict=True,
            login_timeout=int(self.config.connection.get("login_timeout", 5)),
            timeout=int(self.config.connection.get("timeout", 30)),
        )
        try:
            yield connection
        finally:
            connection.close()

    def _capture_mode(self) -> str:
        return self.capture_mode()

    def _cdc_summary(self, connection: Any) -> dict[str, Any] | None:
        if self._capture_mode() != "change_tracking":
            return None
        current_version = self._current_change_tracking_version(connection)
        tracked_tables: list[dict[str, Any]] = []
        database_enabled = self._is_change_tracking_enabled(connection)
        for spec in self._load_table_specs(connection):
            tracked_tables.append(self._change_tracking_table_status(connection, spec))
        return {
            "capture_mode": "change_tracking",
            "database_enabled": database_enabled,
            "current_version": current_version,
            "tracked_tables": tracked_tables,
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

    def _row_has(self, row: dict[str, Any], key: str) -> bool:
        try:
            self._row_get(row, key)
            return True
        except KeyError:
            return False

    def _load_table_specs(self, connection: Any) -> list[SqlServerTableSpec]:
        include = self.config.include or {"dbo": []}
        watermark_column = str(self.config.options.get("watermark_column", "updated_at"))
        commit_timestamp_column = str(self.config.options.get("commit_timestamp_column", watermark_column))
        specs: list[SqlServerTableSpec] = []
        with connection.cursor() as cursor:
            for schema_name, tables in include.items():
                target_schema = schema_name or "dbo"
                if not tables:
                    raise ValueError(f"{self.source_type} include must list explicit tables for the current baseline")
                for table_name in tables:
                    cursor.execute(
                        """
                        select
                            c.COLUMN_NAME as column_name,
                            c.DATA_TYPE as data_type,
                            c.ORDINAL_POSITION as ordinal_position,
                            case when pk.COLUMN_NAME is not null then 1 else 0 end as is_primary_key
                        from INFORMATION_SCHEMA.COLUMNS c
                        left join (
                            select ku.TABLE_SCHEMA, ku.TABLE_NAME, ku.COLUMN_NAME
                            from INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                            join INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
                              on tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                             and tc.TABLE_SCHEMA = ku.TABLE_SCHEMA
                             and tc.TABLE_NAME = ku.TABLE_NAME
                            where tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                        ) pk
                          on pk.TABLE_SCHEMA = c.TABLE_SCHEMA
                         and pk.TABLE_NAME = c.TABLE_NAME
                         and pk.COLUMN_NAME = c.COLUMN_NAME
                        where c.TABLE_SCHEMA = %s and c.TABLE_NAME = %s
                        order by c.ORDINAL_POSITION
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
                        SqlServerTableSpec(
                            schema_name=target_schema,
                            table_name=table_name,
                            watermark_column=watermark_column,
                            commit_timestamp_column=commit_timestamp_column,
                            primary_key_columns=primary_key_columns,
                            selected_columns=columns,
                        )
                    )
        return specs

    def _is_change_tracking_enabled(self, connection: Any) -> bool:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                select case when exists (
                    select 1 from sys.change_tracking_databases where database_id = db_id()
                ) then 1 else 0 end as enabled
                """
            )
            row = cursor.fetchone() or {}
        return bool(self._row_get(row, "enabled"))

    def _current_change_tracking_version(self, connection: Any) -> int:
        with connection.cursor() as cursor:
            cursor.execute("select change_tracking_current_version() as current_version")
            row = cursor.fetchone() or {}
        raw = self._row_get(row, "current_version")
        return int(raw or 0)

    def _change_tracking_table_status(self, connection: Any, spec: SqlServerTableSpec) -> dict[str, Any]:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                select
                    ctt.begin_version as begin_version,
                    ctt.min_valid_version as min_valid_version,
                    ctt.cleanup_version as cleanup_version,
                    ctt.is_track_columns_updated_on as is_track_columns_updated_on
                from sys.change_tracking_tables ctt
                join sys.tables t on ctt.object_id = t.object_id
                join sys.schemas s on t.schema_id = s.schema_id
                where s.name = %s and t.name = %s
                """,
                (spec.schema_name, spec.table_name),
            )
            row = cursor.fetchone() or {}
        enabled = bool(row)
        return {
            "table_key": spec.key,
            "enabled": enabled,
            "begin_version": int(self._row_get(row, "begin_version") or 0) if enabled else None,
            "min_valid_version": int(self._row_get(row, "min_valid_version") or 0) if enabled else None,
            "cleanup_version": int(self._row_get(row, "cleanup_version") or 0) if enabled else None,
            "track_columns_updated": bool(self._row_get(row, "is_track_columns_updated_on")) if enabled else None,
        }

    def _validate_change_tracking_prerequisites(self, connection: Any, current_version: int | None = None) -> None:
        if not self._is_change_tracking_enabled(connection):
            raise ValueError(f"{self.source_type} database change tracking must be enabled for capture_mode=change_tracking")
        for spec in self._load_table_specs(connection):
            status = self._change_tracking_table_status(connection, spec)
            if not status["enabled"]:
                raise ValueError(f"{self.source_type} table {spec.key} must have change tracking enabled")
            if current_version is not None and int(status["min_valid_version"] or 0) > current_version:
                raise ValueError(f"{self.source_type} change tracking min_valid_version for {spec.key} is ahead of current version")

    def _fetch_rows(
        self,
        connection: Any,
        spec: SqlServerTableSpec,
        table_state: dict[str, Any] | None,
    ) -> list[dict[str, Any]]:
        select_columns = ", ".join(self._quote_identifier(column) for column in spec.selected_columns)
        order_columns = [spec.watermark_column, *spec.primary_key_columns]
        order_by = ", ".join(self._quote_identifier(column) for column in order_columns)
        row_limit = int(self.config.options.get("row_limit", self.config.batch_size))
        top_clause = f"top {row_limit} " if row_limit > 0 else ""
        sql = (
            f"select {top_clause}{select_columns} "
            f"from {self._qualified_table(spec.schema_name, spec.table_name)} "
            f"where {self._quote_identifier(spec.watermark_column)} is not null"
        )
        params: list[Any] = []
        if table_state:
            pk_values = list(table_state.get("pk") or [])
            if len(pk_values) != len(spec.primary_key_columns):
                raise ValueError(f"{self.source_type} checkpoint for {spec.key} has invalid primary key state")
            watermark_value = table_state.get("watermark")
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
        with connection.cursor() as cursor:
            cursor.execute(sql, tuple(params))
            return list(cursor.fetchall())

    def _fetch_change_tracking_rows(
        self,
        connection: Any,
        specs: list[SqlServerTableSpec],
        checkpoint: SqlServerChangeTrackingCheckpoint,
    ) -> list[dict[str, Any]]:
        candidates: list[dict[str, Any]] = []
        base_version = checkpoint.version
        if checkpoint.table_key and base_version > 0:
            base_version -= 1
        poll_timestamp = utc_now().replace(microsecond=0).isoformat().replace("+00:00", "Z")
        for spec in specs:
            table_status = self._change_tracking_table_status(connection, spec)
            min_valid_version = int(table_status["min_valid_version"] or 0)
            if checkpoint.version and min_valid_version and checkpoint.version < min_valid_version:
                raise ValueError(
                    f"{self.source_type} checkpoint version {checkpoint.version} is older than min valid change tracking version "
                    f"{min_valid_version} for {spec.key}; reset checkpoint or re-bootstrap the source"
                )
            rows = self._fetch_change_tracking_table_rows(connection, spec, base_version)
            for row in rows:
                change_version = int(self._row_get(row, "sys_change_version"))
                pk_marker = [self._coerce_pk_value(self._row_get(row, column)) for column in spec.primary_key_columns]
                if checkpoint.table_key and change_version == checkpoint.version:
                    if spec.key < checkpoint.table_key:
                        continue
                    if spec.key == checkpoint.table_key and checkpoint.pk is not None and pk_marker <= list(checkpoint.pk):
                        continue
                operation = self._map_change_tracking_operation(str(self._row_get(row, "sys_change_operation")))
                primary_key = {
                    column: self._normalize_value(self._row_get(row, column))
                    for column in spec.primary_key_columns
                }
                after_row = None
                commit_timestamp = poll_timestamp
                if operation != "delete":
                    after_row = {
                        column: self._normalize_value(self._row_get(row, column))
                        for column in spec.selected_columns
                    }
                    commit_raw = self._row_get(row, spec.commit_timestamp_column) if self._row_has(row, spec.commit_timestamp_column) else None
                    if commit_raw is not None:
                        commit_timestamp = self._normalize_timestamp(commit_raw)
                candidates.append(
                    {
                        "spec": spec,
                        "table_key": spec.key,
                        "change_version": change_version,
                        "operation": operation,
                        "primary_key": primary_key,
                        "pk_marker": pk_marker,
                        "before": None,
                        "after": after_row,
                        "commit_timestamp": commit_timestamp,
                        "metadata": {
                            "capture_mode": "change_tracking",
                            "change_tracking_version": change_version,
                            "change_tracking_operation": str(self._row_get(row, "sys_change_operation")),
                            "change_tracking_columns": self._normalize_value(self._row_get(row, "sys_change_columns")) if self._row_has(row, "sys_change_columns") else None,
                            "primary_key_columns": spec.primary_key_columns,
                        },
                    }
                )
        candidates.sort(key=lambda item: (int(item["change_version"]), str(item["table_key"]), *list(item["pk_marker"])))
        return candidates

    def _fetch_change_tracking_table_rows(self, connection: Any, spec: SqlServerTableSpec, version: int) -> list[dict[str, Any]]:
        ct_pk_columns = ", ".join(f"CT.{self._quote_identifier(column)} as {self._quote_identifier(column)}" for column in spec.primary_key_columns)
        row_columns = ", ".join(
            f"T.{self._quote_identifier(column)} as {self._quote_identifier(column)}"
            for column in spec.selected_columns
        )
        join_predicate = " and ".join(
            f"T.{self._quote_identifier(column)} = CT.{self._quote_identifier(column)}"
            for column in spec.primary_key_columns
        )
        sql = f"""
            select
                CT.SYS_CHANGE_VERSION as sys_change_version,
                CT.SYS_CHANGE_OPERATION as sys_change_operation,
                CT.SYS_CHANGE_COLUMNS as sys_change_columns,
                {ct_pk_columns},
                {row_columns}
            from CHANGETABLE(CHANGES {self._qualified_table(spec.schema_name, spec.table_name)}, %s) as CT
            left join {self._qualified_table(spec.schema_name, spec.table_name)} as T
              on {join_predicate}
            order by CT.SYS_CHANGE_VERSION, {", ".join(f"CT.{self._quote_identifier(column)}" for column in spec.primary_key_columns)}
        """
        with connection.cursor() as cursor:
            cursor.execute(sql, (version,))
            return list(cursor.fetchall())

    def _parse_checkpoint(self, checkpoint: SourceCheckpoint | None) -> dict[str, dict[str, Any]]:
        if checkpoint is None or not checkpoint.token:
            return {}
        payload = json.loads(checkpoint.token)
        if not isinstance(payload, dict):
            raise ValueError(f"{self.source_type} checkpoint token must be a JSON object")
        return payload

    def _parse_change_tracking_checkpoint(
        self,
        checkpoint: SourceCheckpoint | None,
    ) -> SqlServerChangeTrackingCheckpoint | None:
        if checkpoint is None or not checkpoint.token:
            return None
        payload = json.loads(checkpoint.token)
        if not isinstance(payload, dict):
            raise ValueError(f"{self.source_type} change tracking checkpoint token must be a JSON object")
        if payload.get("mode") != "change_tracking":
            return None
        table_key = payload.get("table_key")
        pk = payload.get("pk")
        return SqlServerChangeTrackingCheckpoint(
            version=int(payload.get("version", 0)),
            table_key=str(table_key) if table_key else None,
            pk=list(pk) if isinstance(pk, list) else None,
        )

    def _map_change_tracking_operation(self, value: str) -> str:
        mapping = {"I": "insert", "U": "update", "D": "delete"}
        operation = mapping.get(value.upper())
        if operation is None:
            raise ValueError(f"{self.source_type} returned unsupported change tracking operation {value!r}")
        return operation

    def _sort_key(self, spec: SqlServerTableSpec, row: dict[str, Any]) -> tuple[Any, ...]:
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
            equalities = [f"{self._quote_identifier(previous)} = %s" for previous in columns[:index]]
            predicate_parts = [*equalities, f"{self._quote_identifier(column)} > %s"]
            predicates.append("(" + " and ".join(predicate_parts) + ")")
        return predicates

    def _qualified_table(self, schema_name: str, table_name: str) -> str:
        return f"{self._quote_identifier(schema_name)}.{self._quote_identifier(table_name)}"

    def _quote_identifier(self, value: str) -> str:
        return f"[{value.replace(']', ']]')}]"

    def _spec_summary(self, spec: SqlServerTableSpec) -> dict[str, Any]:
        return {
            "schema_name": spec.schema_name,
            "table_name": spec.table_name,
            "watermark_column": spec.watermark_column,
            "commit_timestamp_column": spec.commit_timestamp_column,
            "primary_key_columns": list(spec.primary_key_columns),
            "selected_columns": list(spec.selected_columns),
        }

    def _spec_signature_entry(self, spec: SqlServerTableSpec) -> dict[str, Any]:
        return {
            "key": spec.key,
            "watermark_column": spec.watermark_column,
            "commit_timestamp_column": spec.commit_timestamp_column,
            "primary_key_columns": list(spec.primary_key_columns),
            "selected_columns": list(spec.selected_columns),
        }
