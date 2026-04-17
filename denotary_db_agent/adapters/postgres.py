from __future__ import annotations

import json
from contextlib import contextmanager
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterator

from denotary_db_agent.adapters.base import AdapterCapabilities, BaseAdapter
from denotary_db_agent.models import ChangeEvent, SourceCheckpoint

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:  # pragma: no cover - handled in runtime validation
    psycopg = None
    dict_row = None


@dataclass
class PostgresTableSpec:
    schema_name: str
    table_name: str
    watermark_column: str
    commit_timestamp_column: str
    primary_key_columns: list[str]
    selected_columns: list[str]

    @property
    def key(self) -> str:
        return f"{self.schema_name}.{self.table_name}"


class PostgresAdapter(BaseAdapter):
    source_type = "postgresql"

    def discover_capabilities(self) -> AdapterCapabilities:
        capture_mode = str(self.config.options.get("capture_mode", "watermark")).lower()
        return AdapterCapabilities(
            source_type=self.source_type,
            minimum_version="14",
            supports_cdc=capture_mode == "trigger",
            supports_snapshot=True,
            operations=("snapshot",) if capture_mode != "trigger" else ("insert", "update", "delete"),
            notes=(
                "Trigger-based built-in CDC is enabled for PostgreSQL."
                if capture_mode == "trigger"
                else "Live baseline implementation uses watermark-based snapshot polling. Logical decoding / WAL CDC remains the next PostgreSQL upgrade step."
            ),
        )

    def validate_connection(self) -> None:
        required = ("host", "port", "username", "database")
        missing = [name for name in required if not self.config.connection.get(name)]
        if missing:
            raise ValueError(f"postgresql connection is missing required fields: {', '.join(missing)}")
        if self.config.options.get("dry_run_events"):
            return
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute("select version()")
                cursor.fetchone()
            specs = self._load_table_specs(connection)
            if self._capture_mode() == "trigger":
                self._ensure_trigger_cdc_setup(connection, specs)

    def start_stream(self, checkpoint: SourceCheckpoint | None) -> Iterable[ChangeEvent]:
        if self._capture_mode() != "trigger":
            raise NotImplementedError(
                "postgresql logical decoding streaming is not implemented yet; use read_snapshot() watermark polling in the current baseline"
            )

        last_event_id = self._parse_trigger_checkpoint(checkpoint)
        with self._connect() as connection:
            specs = self._load_table_specs(connection)
            self._ensure_trigger_cdc_setup(connection, specs)
            spec_map = {spec.key: spec for spec in specs}
            for row in self._fetch_trigger_events(connection, last_event_id):
                spec_key = f"{row['source_schema']}.{row['source_table']}"
                spec = spec_map.get(spec_key)
                if spec is None:
                    continue
                event_id = int(row["event_id"])
                commit_timestamp = self._normalize_timestamp(row["commit_timestamp"])
                primary_key = dict(row["primary_key"] or {})
                operation = str(row["operation"]).lower()
                before_row = row.get("before_row")
                after_row = row.get("after_row")
                checkpoint_token = json.dumps({"mode": "trigger_cdc", "event_id": event_id}, sort_keys=True)
                yield ChangeEvent(
                    source_id=self.config.id,
                    source_type=self.source_type,
                    source_instance=self.config.source_instance,
                    database_name=self.config.database_name,
                    schema_or_namespace=spec.schema_name,
                    table_or_collection=spec.table_name,
                    operation=operation,  # type: ignore[arg-type]
                    primary_key=primary_key,
                    change_version=f"event:{event_id}",
                    commit_timestamp=commit_timestamp,
                    before=before_row,
                    after=after_row,
                    metadata={
                        "capture_mode": "trigger-cdc",
                        "event_id": event_id,
                        "primary_key_columns": spec.primary_key_columns,
                    },
                    checkpoint_token=checkpoint_token,
                )

    def stop_stream(self) -> None:
        return None

    def read_snapshot(self, checkpoint: SourceCheckpoint | None = None) -> Iterable[ChangeEvent]:
        if self._capture_mode() == "trigger":
            return

        dry_events = self.config.options.get("dry_run_events") or []
        for item in dry_events:
            yield ChangeEvent(
                source_id=self.config.id,
                source_type=self.source_type,
                source_instance=self.config.source_instance,
                database_name=self.config.database_name,
                schema_or_namespace=str(item.get("schema_or_namespace", "public")),
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
            candidates: list[tuple[PostgresTableSpec, dict[str, Any]]] = []
            for spec in specs:
                table_state = checkpoint_state.get(spec.key)
                candidates.extend((spec, row) for row in self._fetch_rows(connection, spec, table_state))

        candidates.sort(key=lambda item: self._sort_key(item[0], item[1]))
        current_state = dict(checkpoint_state)
        for spec, row in candidates:
            watermark_value = self._normalize_timestamp(row[spec.commit_timestamp_column])
            primary_key = {column: row[column] for column in spec.primary_key_columns}
            current_state[spec.key] = {
                "watermark": self._normalize_timestamp(row[spec.watermark_column]),
                "pk": [self._coerce_pk_value(primary_key[column]) for column in spec.primary_key_columns],
            }
            payload_after = {column: row[column] for column in spec.selected_columns}
            yield ChangeEvent(
                source_id=self.config.id,
                source_type=self.source_type,
                source_instance=self.config.source_instance,
                database_name=self.config.database_name,
                schema_or_namespace=spec.schema_name,
                table_or_collection=spec.table_name,
                operation="snapshot",
                primary_key=primary_key,
                change_version=f"{spec.key}:{watermark_value}:{self._pk_marker(primary_key, spec.primary_key_columns)}",
                commit_timestamp=watermark_value,
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
        if psycopg is None:
            raise RuntimeError("psycopg is required for live PostgreSQL connectivity; install psycopg[binary]>=3.1")
        connection = psycopg.connect(
            host=self.config.connection.get("host"),
            port=self.config.connection.get("port"),
            user=self.config.connection.get("username"),
            password=self.config.connection.get("password"),
            dbname=self.config.connection.get("database"),
            sslmode=self.config.connection.get("sslmode", "prefer"),
            connect_timeout=int(self.config.connection.get("connect_timeout", 5)),
            row_factory=dict_row,
        )
        try:
            yield connection
            connection.commit()
        finally:
            connection.close()

    def _load_table_specs(self, connection: Any) -> list[PostgresTableSpec]:
        include = self.config.include or {}
        if not include:
            raise ValueError("postgresql source must define include schemas/tables")

        watermark_column = str(self.config.options.get("watermark_column", "updated_at"))
        commit_timestamp_column = str(self.config.options.get("commit_timestamp_column", watermark_column))
        pk_overrides = self.config.options.get("primary_key_columns") or {}

        specs: list[PostgresTableSpec] = []
        with connection.cursor() as cursor:
            for schema_name, tables in include.items():
                for table_name in tables:
                    column_rows = cursor.execute(
                        """
                        select column_name
                        from information_schema.columns
                        where table_schema = %s and table_name = %s
                        order by ordinal_position
                        """,
                        (schema_name, table_name),
                    ).fetchall()
                    if not column_rows:
                        raise ValueError(f"postgresql table not found: {schema_name}.{table_name}")
                    columns = [str(row["column_name"]) for row in column_rows]
                    if watermark_column not in columns:
                        raise ValueError(
                            f"postgresql table {schema_name}.{table_name} does not contain watermark column {watermark_column}"
                        )
                    if commit_timestamp_column not in columns:
                        raise ValueError(
                            f"postgresql table {schema_name}.{table_name} does not contain commit timestamp column {commit_timestamp_column}"
                        )

                    override_key = f"{schema_name}.{table_name}"
                    primary_key_columns = pk_overrides.get(override_key)
                    if primary_key_columns is None:
                        pk_rows = cursor.execute(
                            """
                            select kcu.column_name
                            from information_schema.table_constraints tc
                            join information_schema.key_column_usage kcu
                              on tc.constraint_name = kcu.constraint_name
                             and tc.table_schema = kcu.table_schema
                             and tc.table_name = kcu.table_name
                            where tc.constraint_type = 'PRIMARY KEY'
                              and tc.table_schema = %s
                              and tc.table_name = %s
                            order by kcu.ordinal_position
                            """,
                            (schema_name, table_name),
                        ).fetchall()
                        primary_key_columns = [str(row["column_name"]) for row in pk_rows]
                    if not primary_key_columns:
                        raise ValueError(f"postgresql table {schema_name}.{table_name} must have a primary key or override")

                    specs.append(
                        PostgresTableSpec(
                            schema_name=schema_name,
                            table_name=table_name,
                            watermark_column=watermark_column,
                            commit_timestamp_column=commit_timestamp_column,
                            primary_key_columns=[str(item) for item in primary_key_columns],
                            selected_columns=columns,
                        )
                    )
        return specs

    def _ensure_trigger_cdc_setup(self, connection: Any, specs: list[PostgresTableSpec]) -> None:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                create schema if not exists denotary_cdc;

                create table if not exists denotary_cdc.events (
                    event_id bigserial primary key,
                    source_schema text not null,
                    source_table text not null,
                    operation text not null,
                    primary_key jsonb not null,
                    before_row jsonb,
                    after_row jsonb,
                    commit_timestamp timestamptz not null default now()
                );

                create or replace function denotary_cdc.capture_change() returns trigger
                language plpgsql
                as $$
                declare
                    source_row jsonb;
                    pk jsonb := '{}'::jsonb;
                    col text;
                begin
                    if TG_OP = 'DELETE' then
                        source_row := to_jsonb(OLD);
                    else
                        source_row := to_jsonb(NEW);
                    end if;

                    foreach col in array TG_ARGV loop
                        pk := pk || jsonb_build_object(col, source_row -> col);
                    end loop;

                    insert into denotary_cdc.events (
                        source_schema,
                        source_table,
                        operation,
                        primary_key,
                        before_row,
                        after_row,
                        commit_timestamp
                    ) values (
                        TG_TABLE_SCHEMA,
                        TG_TABLE_NAME,
                        lower(TG_OP),
                        pk,
                        case when TG_OP in ('UPDATE', 'DELETE') then to_jsonb(OLD) else null end,
                        case when TG_OP in ('INSERT', 'UPDATE') then to_jsonb(NEW) else null end,
                        now()
                    );

                    if TG_OP = 'DELETE' then
                        return OLD;
                    end if;
                    return NEW;
                end;
                $$;
                """
            )
            for spec in specs:
                trigger_name = f"dn_cdc_{spec.schema_name}_{spec.table_name}"
                quoted_args = ", ".join(f"'{column}'" for column in spec.primary_key_columns)
                cursor.execute(
                    f"""
                    drop trigger if exists {self._quote_identifier(trigger_name)}
                    on {self._quote_identifier(spec.schema_name)}.{self._quote_identifier(spec.table_name)};

                    create trigger {self._quote_identifier(trigger_name)}
                    after insert or update or delete
                    on {self._quote_identifier(spec.schema_name)}.{self._quote_identifier(spec.table_name)}
                    for each row execute function denotary_cdc.capture_change({quoted_args});
                    """
                )

    def _fetch_trigger_events(self, connection: Any, after_event_id: int) -> list[dict[str, Any]]:
        row_limit = int(self.config.options.get("row_limit", self.config.batch_size))
        with connection.cursor() as cursor:
            return list(
                cursor.execute(
                    """
                    select
                        event_id,
                        source_schema,
                        source_table,
                        operation,
                        primary_key,
                        before_row,
                        after_row,
                        commit_timestamp
                    from denotary_cdc.events
                    where event_id > %s
                    order by event_id
                    limit %s
                    """,
                    (after_event_id, row_limit),
                ).fetchall()
            )

    def _fetch_rows(
        self,
        connection: Any,
        spec: PostgresTableSpec,
        table_state: dict[str, Any] | None,
    ) -> list[dict[str, Any]]:
        selected_sql = ", ".join(self._quote_identifier(column) for column in spec.selected_columns)
        order_by = ", ".join(
            [self._quote_identifier(spec.watermark_column)]
            + [f"{self._quote_identifier(column)}::text" for column in spec.primary_key_columns]
        )
        base_sql = (
            f"select {selected_sql} "
            f"from {self._quote_identifier(spec.schema_name)}.{self._quote_identifier(spec.table_name)} "
            f"where {self._quote_identifier(spec.watermark_column)} is not null"
        )
        params: list[Any] = []
        if table_state:
            watermark_value = table_state["watermark"]
            pk_values = list(table_state.get("pk") or [])
            if len(pk_values) != len(spec.primary_key_columns):
                raise ValueError(f"checkpoint primary key shape mismatch for {spec.key}")
            left_row = ", ".join(f"{self._quote_identifier(column)}::text" for column in spec.primary_key_columns)
            right_row = ", ".join(["%s"] * len(spec.primary_key_columns))
            base_sql += (
                f" and ("
                f"{self._quote_identifier(spec.watermark_column)} > %s "
                f"or ("
                f"{self._quote_identifier(spec.watermark_column)} = %s and "
                f"row({left_row}) > row({right_row})"
                f"))"
            )
            params.extend([watermark_value, watermark_value, *pk_values])

        row_limit = int(self.config.options.get("row_limit", self.config.batch_size))
        query = f"{base_sql} order by {order_by} limit %s"
        params.append(row_limit)
        with connection.cursor() as cursor:
            return list(cursor.execute(query, params).fetchall())

    def _parse_checkpoint(self, checkpoint: SourceCheckpoint | None) -> dict[str, dict[str, Any]]:
        if checkpoint is None or not checkpoint.token:
            return {}
        try:
            value = json.loads(checkpoint.token)
        except json.JSONDecodeError as exc:
            raise ValueError("postgresql checkpoint token is not valid JSON") from exc
        if not isinstance(value, dict):
            raise ValueError("postgresql checkpoint token must be a JSON object")
        normalized: dict[str, dict[str, Any]] = {}
        for key, item in value.items():
            if not isinstance(item, dict):
                continue
            watermark = item.get("watermark")
            pk_values = item.get("pk") or []
            if isinstance(watermark, str) and isinstance(pk_values, list):
                normalized[str(key)] = {"watermark": watermark, "pk": [str(element) for element in pk_values]}
        return normalized

    def _parse_trigger_checkpoint(self, checkpoint: SourceCheckpoint | None) -> int:
        if checkpoint is None or not checkpoint.token:
            return 0
        try:
            value = json.loads(checkpoint.token)
        except json.JSONDecodeError:
            return 0
        if isinstance(value, dict) and str(value.get("mode")) == "trigger_cdc":
            raw_id = value.get("event_id", 0)
            return int(raw_id) if isinstance(raw_id, int) or str(raw_id).isdigit() else 0
        return 0

    def _sort_key(self, spec: PostgresTableSpec, row: dict[str, Any]) -> tuple[Any, ...]:
        commit_timestamp = self._normalize_timestamp(row[spec.commit_timestamp_column])
        pk_marker = tuple(self._coerce_pk_value(row[column]) for column in spec.primary_key_columns)
        return (commit_timestamp, spec.schema_name, spec.table_name, *pk_marker)

    def _normalize_timestamp(self, value: Any) -> str:
        if isinstance(value, datetime):
            normalized = value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
            return normalized.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        if isinstance(value, str):
            return value
        raise ValueError(f"unsupported PostgreSQL timestamp value: {value!r}")

    def _pk_marker(self, primary_key: dict[str, Any], columns: list[str]) -> str:
        return "|".join(self._coerce_pk_value(primary_key[column]) for column in columns)

    def _coerce_pk_value(self, value: Any) -> str:
        if value is None:
            return ""
        return str(value)

    def _quote_identifier(self, value: str) -> str:
        return '"' + value.replace('"', '""') + '"'

    def _capture_mode(self) -> str:
        return str(self.config.options.get("capture_mode", "watermark")).lower()
