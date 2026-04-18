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


class MySqlAdapter(BaseAdapter):
    source_type = "mysql"

    def discover_capabilities(self) -> AdapterCapabilities:
        capture_mode = self._capture_mode()
        return AdapterCapabilities(
            source_type=self.source_type,
            minimum_version="8.0",
            supports_cdc=False,
            supports_snapshot=True,
            operations=("snapshot",),
            capture_modes=("watermark",),
            bootstrap_requirements=("tracked tables visible", "watermark columns configured"),
            notes=(
                "Live baseline implementation uses MySQL watermark-based snapshot polling. "
                "Row-based binlog CDC remains the next MySQL-specific step."
            ),
        )

    def validate_connection(self) -> None:
        required = ("host", "port", "username", "database")
        missing = [name for name in required if not self.config.connection.get(name)]
        if missing:
            raise ValueError(f"mysql connection is missing required fields: {', '.join(missing)}")
        if pymysql is None:
            raise RuntimeError("PyMySQL is required for live mysql adapter use")
        if self.config.options.get("dry_run_events"):
            return
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute("select version() as version")
                cursor.fetchone()
            self._load_table_specs(connection)

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
        return {
            "source_id": self.config.id,
            "adapter": self.config.adapter,
            "capture_mode": self._capture_mode(),
            "tracked_tables": [self._spec_summary(spec) for spec in specs],
            "cdc": None,
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
            "cdc": None,
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
        raise NotImplementedError("mysql CDC streaming is not implemented yet; use watermark snapshot mode")

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
            raise RuntimeError("PyMySQL is required for live mysql adapter use")
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
        return str(self.config.options.get("capture_mode", "watermark")).lower()

    def _load_table_specs(self, connection: Any) -> list[MySqlTableSpec]:
        include = self.config.include or {self.config.database_name: []}
        watermark_column = str(self.config.options.get("watermark_column", "updated_at"))
        commit_timestamp_column = str(self.config.options.get("commit_timestamp_column", watermark_column))
        specs: list[MySqlTableSpec] = []
        with connection.cursor() as cursor:
            for schema_name, tables in include.items():
                target_schema = schema_name or self.config.database_name
                if not tables:
                    raise ValueError("mysql include must list explicit tables for the current baseline")
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
                        raise ValueError(f"mysql table {target_schema}.{table_name} was not found")
                    columns = [str(row["column_name"]) for row in rows]
                    primary_key_columns = [str(row["column_name"]) for row in rows if row["is_primary_key"]]
                    if not primary_key_columns:
                        raise ValueError(f"mysql table {target_schema}.{table_name} must have a primary key")
                    if watermark_column not in columns:
                        raise ValueError(
                            f"mysql table {target_schema}.{table_name} does not contain watermark column {watermark_column}"
                        )
                    if commit_timestamp_column not in columns:
                        raise ValueError(
                            f"mysql table {target_schema}.{table_name} does not contain commit timestamp column {commit_timestamp_column}"
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
                raise ValueError(f"mysql checkpoint for {spec.key} has invalid primary key state")
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

    def _parse_checkpoint(self, checkpoint: SourceCheckpoint | None) -> dict[str, dict[str, Any]]:
        if checkpoint is None or not checkpoint.token:
            return {}
        payload = json.loads(checkpoint.token)
        if not isinstance(payload, dict):
            raise ValueError("mysql checkpoint token must be a JSON object")
        return payload

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
            raise ValueError(f"mysql timestamp value must normalize to string, got {type(normalized)!r}")
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
