from __future__ import annotations

import json
import re
import struct
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


@dataclass
class PostgresLogicalRelationColumn:
    name: str
    type_oid: int
    type_modifier: int


@dataclass
class PostgresLogicalRelation:
    relid: int
    schema_name: str
    table_name: str
    replica_identity: str
    columns: list[PostgresLogicalRelationColumn]

    @property
    def key(self) -> str:
        return f"{self.schema_name}.{self.table_name}"


class PostgresAdapter(BaseAdapter):
    source_type = "postgresql"

    def __init__(self, config):
        super().__init__(config)
        self._listener_connection: Any | None = None
        self._logical_warning_emitted = False
        self._logical_relation_cache: dict[int, PostgresLogicalRelation] = {}

    def discover_capabilities(self) -> AdapterCapabilities:
        capture_mode = str(self.config.options.get("capture_mode", "watermark")).lower()
        logical_plugin = self._logical_output_plugin()
        return AdapterCapabilities(
            source_type=self.source_type,
            minimum_version="14",
            supports_cdc=capture_mode in {"trigger", "logical"},
            supports_snapshot=True,
            operations=("snapshot",) if capture_mode == "watermark" else ("insert", "update", "delete"),
            notes=(
                "Trigger-based built-in CDC is enabled for PostgreSQL with optional LISTEN/NOTIFY wakeups."
                if capture_mode == "trigger"
                else (
                    (
                        "Logical decoding / WAL CDC is enabled through a logical replication slot and "
                        "pgoutput publication setup. Runtime binary decoding is not yet implemented, "
                        "so pgoutput is currently bootstrap/inspect ready only."
                    )
                    if logical_plugin == "pgoutput"
                    else "Logical decoding / WAL CDC is enabled through a logical replication slot and test_decoding output."
                    if capture_mode == "logical"
                    else "Live baseline implementation uses watermark-based snapshot polling."
                )
            ),
        )

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
            cdc_state = self._inspect_cdc_state(connection, specs)
        return {
            "source_id": self.config.id,
            "adapter": self.config.adapter,
            "capture_mode": self._capture_mode(),
            "tracked_tables": [self._spec_summary(spec) for spec in specs],
            "cdc": cdc_state,
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
            cdc_state = self._inspect_cdc_state(connection, specs)
        return {
            "source_id": self.config.id,
            "adapter": self.config.adapter,
            "source_type": capabilities.source_type,
            "capture_mode": self._capture_mode(),
            "supports_cdc": capabilities.supports_cdc,
            "supports_snapshot": capabilities.supports_snapshot,
            "tracked_tables": [self._spec_summary(spec) for spec in specs],
            "cdc": cdc_state,
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
        if self.config.options.get("dry_run_events"):
            return self.bootstrap()
        return self.bootstrap()

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
            elif self._capture_mode() == "logical":
                self._ensure_logical_cdc_setup(connection, specs)

    def start_stream(self, checkpoint: SourceCheckpoint | None) -> Iterable[ChangeEvent]:
        capture_mode = self._capture_mode()
        with self._connect() as connection:
            specs = self._load_table_specs(connection)
            if capture_mode == "trigger":
                self._ensure_trigger_cdc_setup(connection, specs)
                spec_map = {spec.key: spec for spec in specs}
                last_event_id = self._parse_trigger_checkpoint(checkpoint)
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
                return

            if capture_mode == "logical":
                self._ensure_logical_cdc_setup(connection, specs)
                spec_map = {spec.key: spec for spec in specs}
                last_xid, last_event_index, _commit_lsn, _advance_lsn = self._parse_logical_checkpoint_state(checkpoint)
                row_limit = int(self.config.options.get("row_limit", self.config.batch_size))
                plugin = self._logical_output_plugin()
                decoded_rows = (
                    self._fetch_pgoutput_decoded_rows(connection, spec_map, row_limit)
                    if plugin == "pgoutput"
                    else self._fetch_logical_decoded_rows(connection, spec_map, row_limit)
                )
                processed_count = 0
                for xid, lsn, commit_lsn, advance_lsn, event_index, spec, operation, before_row, after_row in decoded_rows:
                    if xid == last_xid and event_index <= last_event_index:
                        continue
                    if processed_count >= row_limit:
                        break
                    primary_key = self._derive_primary_key(spec, before_row, after_row)
                    commit_timestamp = self._derive_commit_timestamp(spec, before_row, after_row)
                    checkpoint_token = json.dumps(
                        {
                            "mode": "logical_cdc",
                            "xid": xid,
                            "lsn": lsn,
                            "commit_lsn": commit_lsn,
                            "event_index": event_index,
                            "advance_lsn": advance_lsn,
                        },
                        sort_keys=True,
                    )
                    yield ChangeEvent(
                        source_id=self.config.id,
                        source_type=self.source_type,
                        source_instance=self.config.source_instance,
                        database_name=self.config.database_name,
                        schema_or_namespace=spec.schema_name,
                        table_or_collection=spec.table_name,
                        operation=operation,  # type: ignore[arg-type]
                        primary_key=primary_key,
                        change_version=f"lsn:{lsn}:{event_index}",
                        commit_timestamp=commit_timestamp,
                        before=before_row,
                        after=after_row,
                        metadata={
                            "capture_mode": "logical-cdc",
                            "xid": xid,
                            "lsn": lsn,
                            "commit_lsn": commit_lsn,
                            "event_index": event_index,
                            "plugin": self._logical_output_plugin(),
                            "slot_name": self._logical_slot_name(),
                            "primary_key_columns": spec.primary_key_columns,
                        },
                        checkpoint_token=checkpoint_token,
                    )
                    processed_count += 1
                return

        raise NotImplementedError(
            "postgresql start_stream() is only available for trigger or logical capture modes"
        )

    def stop_stream(self) -> None:
        if self._listener_connection is not None:
            self._listener_connection.close()
            self._listener_connection = None
        return None

    def read_snapshot(self, checkpoint: SourceCheckpoint | None = None) -> Iterable[ChangeEvent]:
        if self._capture_mode() in {"trigger", "logical"}:
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

    def wait_for_changes(self, timeout_sec: float) -> bool:
        if self._capture_mode() == "logical":
            return super().wait_for_changes(timeout_sec)
        if self._capture_mode() != "trigger":
            return super().wait_for_changes(timeout_sec)
        try:
            listener = self._ensure_listener()
            for _ in listener.notifies(timeout=timeout_sec, stop_after=1):
                return True
        except Exception:
            if self._listener_connection is not None:
                try:
                    self._listener_connection.close()
                except Exception:
                    pass
                self._listener_connection = None
        return False

    def after_checkpoint_advanced(self, token: str) -> None:
        capture_mode = self._capture_mode()
        if capture_mode == "trigger":
            if not bool(self.config.options.get("cleanup_processed_events", True)):
                return
            event_id = self._parse_trigger_checkpoint(
                SourceCheckpoint(source_id=self.config.id, token=token, updated_at="")
            )
            if event_id <= 0:
                return
            with self._connect() as connection:
                with connection.cursor() as cursor:
                    cursor.execute("delete from denotary_cdc.events where event_id <= %s", (event_id,))
            return

        if capture_mode == "logical":
            _xid, _event_index, commit_lsn, advance_lsn = self._parse_logical_checkpoint_state(
                SourceCheckpoint(source_id=self.config.id, token=token, updated_at="")
            )
            if not commit_lsn or not advance_lsn:
                return
            with self._connect() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        "select end_lsn from pg_replication_slot_advance(%s, %s)",
                        (self._logical_slot_name(), commit_lsn),
                    )

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
                    inserted_event_id bigint;
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
                    ) returning event_id into inserted_event_id;

                    perform pg_notify('denotary_cdc_events', inserted_event_id::text);

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

    def _ensure_logical_cdc_setup(self, connection: Any, specs: list[PostgresTableSpec]) -> None:
        plugin = self._logical_output_plugin()
        with connection.cursor() as cursor:
            wal_level_row = cursor.execute("show wal_level").fetchone()
            if str(wal_level_row["wal_level"]).lower() != "logical":
                raise ValueError("postgresql wal_level must be logical for capture_mode=logical")

            if bool(self.config.options.get("replica_identity_full", True)):
                for spec in specs:
                    cursor.execute(
                        f"""
                        alter table {self._quote_identifier(spec.schema_name)}.{self._quote_identifier(spec.table_name)}
                        replica identity full
                        """
                    )
                connection.commit()

            if plugin == "pgoutput":
                self._ensure_pgoutput_publication(cursor, specs)
                connection.commit()

            slot_row = cursor.execute(
                """
                select slot_name, plugin, confirmed_flush_lsn, restart_lsn
                from pg_replication_slots
                where slot_name = %s
                """,
                (self._logical_slot_name(),),
            ).fetchone()
            if slot_row is None and bool(self.config.options.get("auto_create_slot", True)):
                cursor.execute(
                    "select slot_name, lsn from pg_create_logical_replication_slot(%s, %s)",
                    (self._logical_slot_name(), plugin),
                )
            elif slot_row is not None and str(slot_row["plugin"]) != plugin:
                raise ValueError(
                    f"postgresql logical slot {self._logical_slot_name()} exists with plugin {slot_row['plugin']}, expected {plugin}"
                )

    def _ensure_pgoutput_publication(self, cursor: Any, specs: list[PostgresTableSpec]) -> None:
        publication_name = self._logical_publication_name()
        publication_row = cursor.execute(
            """
            select pubname
            from pg_publication
            where pubname = %s
            """,
            (publication_name,),
        ).fetchone()
        table_sql = ", ".join(
            f"{self._quote_identifier(spec.schema_name)}.{self._quote_identifier(spec.table_name)}"
            for spec in specs
        )
        if publication_row is None:
            if not bool(self.config.options.get("auto_create_publication", True)):
                raise ValueError(
                    f"postgresql publication {publication_name} does not exist and auto_create_publication is disabled"
                )
            cursor.execute(
                f"create publication {self._quote_identifier(publication_name)} for table {table_sql}"
            )
            return

        cursor.execute(
            f"alter publication {self._quote_identifier(publication_name)} set table {table_sql}"
        )

    def _fetch_logical_changes(self, connection: Any, fetch_limit: int | None = None) -> list[dict[str, Any]]:
        row_limit = fetch_limit or int(self.config.options.get("row_limit", self.config.batch_size))
        plugin = self._logical_output_plugin()
        slot_name = self._logical_slot_name()
        options = []
        if plugin == "test_decoding":
            options = ["include-xids", "0", "skip-empty-xacts", "1"]
            placeholders = ", ".join(["%s"] * len(options))
            sql = (
                f"select lsn, xid, data from pg_logical_slot_peek_changes(%s, null, %s"
                + (f", {placeholders}" if placeholders else "")
                + ")"
            )
            params: list[Any] = [slot_name, row_limit, *options]
        elif plugin == "pgoutput":
            options = ["proto_version", "1", "publication_names", self._logical_publication_name()]
            placeholders = ", ".join(["%s"] * len(options))
            sql = (
                f"select lsn, xid, data from pg_logical_slot_peek_binary_changes(%s, null, %s"
                + (f", {placeholders}" if placeholders else "")
                + ")"
            )
            params = [slot_name, row_limit, *options]
        else:
            raise ValueError(f"unsupported PostgreSQL logical output plugin: {plugin}")
        with connection.cursor() as cursor:
            return list(cursor.execute(sql, params).fetchall())

    def _fetch_logical_decoded_rows(
        self,
        connection: Any,
        spec_map: dict[str, PostgresTableSpec],
        row_limit: int,
    ) -> list[tuple[str, str, str, bool, int, PostgresTableSpec, str, dict[str, Any] | None, dict[str, Any] | None]]:
        fetch_limit = max(row_limit + 4, row_limit * 4)
        while True:
            raw_rows = self._fetch_logical_changes(connection, fetch_limit=fetch_limit)
            entries: list[dict[str, Any]] = []
            for row in raw_rows:
                xid = str(row["xid"])
                lsn = str(row["lsn"])
                data = str(row["data"])
                if data.startswith("BEGIN") or data.startswith("COMMIT"):
                    entries.append({"kind": "commit" if data.startswith("COMMIT") else "begin", "xid": xid, "lsn": lsn})
                    continue
                decoded = self._parse_test_decoding_change(data, spec_map)
                if decoded is None:
                    continue
                spec, operation, before_row, after_row = decoded
                entries.append(
                    {
                        "kind": "data",
                        "xid": xid,
                        "lsn": lsn,
                        "spec": spec,
                        "operation": operation,
                        "before_row": before_row,
                        "after_row": after_row,
                    }
                )

            data_entries = [entry for entry in entries if entry["kind"] == "data"]
            if len(data_entries) <= row_limit:
                return self._materialize_logical_entries(entries)

            boundary_xid = str(data_entries[row_limit - 1]["xid"])
            boundary_position = next(
                index
                for index, entry in enumerate(entries)
                if entry["kind"] == "data"
                and entry["xid"] == data_entries[row_limit - 1]["xid"]
                and entry["lsn"] == data_entries[row_limit - 1]["lsn"]
                and entry["after_row"] == data_entries[row_limit - 1]["after_row"]
                and entry["before_row"] == data_entries[row_limit - 1]["before_row"]
            )
            if any(
                entry["kind"] == "commit" and entry["xid"] == boundary_xid
                for entry in entries[boundary_position + 1 :]
            ):
                return self._materialize_logical_entries(entries)

            if len(raw_rows) < fetch_limit:
                return self._materialize_logical_entries(entries)

            fetch_limit *= 2

    def _fetch_pgoutput_decoded_rows(
        self,
        connection: Any,
        spec_map: dict[str, PostgresTableSpec],
        row_limit: int,
    ) -> list[tuple[str, str, str, bool, int, PostgresTableSpec, str, dict[str, Any] | None, dict[str, Any] | None]]:
        fetch_limit = max(row_limit + 8, row_limit * 8)
        while True:
            raw_rows = self._fetch_logical_changes(connection, fetch_limit=fetch_limit)
            entries: list[dict[str, Any]] = []
            current_xid = ""
            for row in raw_rows:
                lsn = str(row["lsn"])
                xid_value = row.get("xid") if isinstance(row, dict) else None
                parsed = self._parse_pgoutput_message(bytes(row["data"]), spec_map)
                if parsed is None:
                    continue
                kind = str(parsed["kind"])
                if kind == "begin":
                    current_xid = str(parsed["xid"])
                    entries.append({"kind": "begin", "xid": current_xid, "lsn": lsn})
                    continue
                if kind == "commit":
                    entries.append({"kind": "commit", "xid": current_xid or str(xid_value or ""), "lsn": parsed["commit_lsn"] or lsn})
                    current_xid = ""
                    continue
                if kind != "data":
                    continue
                entries.append(
                    {
                        "kind": "data",
                        "xid": current_xid or str(xid_value or ""),
                        "lsn": lsn,
                        "spec": parsed["spec"],
                        "operation": parsed["operation"],
                        "before_row": parsed["before_row"],
                        "after_row": parsed["after_row"],
                    }
                )

            data_entries = [entry for entry in entries if entry["kind"] == "data"]
            if len(data_entries) <= row_limit:
                return self._materialize_logical_entries(entries)

            boundary_xid = str(data_entries[row_limit - 1]["xid"])
            boundary_position = next(
                index
                for index, entry in enumerate(entries)
                if entry["kind"] == "data"
                and entry["xid"] == data_entries[row_limit - 1]["xid"]
                and entry["lsn"] == data_entries[row_limit - 1]["lsn"]
                and entry["after_row"] == data_entries[row_limit - 1]["after_row"]
                and entry["before_row"] == data_entries[row_limit - 1]["before_row"]
            )
            if any(
                entry["kind"] == "commit" and entry["xid"] == boundary_xid
                for entry in entries[boundary_position + 1 :]
            ):
                return self._materialize_logical_entries(entries)

            if len(raw_rows) < fetch_limit:
                return self._materialize_logical_entries(entries)

            fetch_limit *= 2

    def _materialize_logical_entries(
        self,
        entries: list[dict[str, Any]],
    ) -> list[tuple[str, str, str, bool, int, PostgresTableSpec, str, dict[str, Any] | None, dict[str, Any] | None]]:
        commit_lsn_by_xid: dict[str, str] = {}
        total_by_xid: dict[str, int] = {}
        for entry in entries:
            if entry["kind"] == "commit":
                commit_lsn_by_xid[str(entry["xid"])] = str(entry["lsn"])
            if entry["kind"] == "data":
                xid = str(entry["xid"])
                total_by_xid[xid] = total_by_xid.get(xid, 0) + 1

        materialized: list[tuple[str, str, str, bool, int, PostgresTableSpec, str, dict[str, Any] | None, dict[str, Any] | None]] = []
        running_by_xid: dict[str, int] = {}
        for entry in entries:
            if entry["kind"] != "data":
                continue
            xid = str(entry["xid"])
            running_by_xid[xid] = running_by_xid.get(xid, 0) + 1
            event_index = running_by_xid[xid]
            commit_lsn = commit_lsn_by_xid.get(xid, "")
            advance_lsn = bool(commit_lsn) and event_index == total_by_xid[xid]
            materialized.append(
                (
                    xid,
                    str(entry["lsn"]),
                    commit_lsn,
                    advance_lsn,
                    event_index,
                    entry["spec"],
                    str(entry["operation"]),
                    entry["before_row"],
                    entry["after_row"],
                )
            )
        return materialized

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

    def _parse_logical_checkpoint(self, checkpoint: SourceCheckpoint | None) -> str:
        _xid, _event_index, commit_lsn, _advance_lsn = self._parse_logical_checkpoint_state(checkpoint)
        if commit_lsn:
            return commit_lsn
        try:
            value = json.loads(checkpoint.token) if checkpoint and checkpoint.token else {}
        except json.JSONDecodeError:
            return ""
        if isinstance(value, dict):
            lsn = value.get("lsn", "")
            return str(lsn) if isinstance(lsn, str) else ""
        return ""

    def _parse_logical_checkpoint_state(self, checkpoint: SourceCheckpoint | None) -> tuple[str, int, str, bool]:
        if checkpoint is None or not checkpoint.token:
            return "", 0, "", False
        try:
            value = json.loads(checkpoint.token)
        except json.JSONDecodeError:
            return "", 0, "", False
        if isinstance(value, dict) and str(value.get("mode")) == "logical_cdc":
            xid = value.get("xid", "")
            event_index = value.get("event_index", 0)
            commit_lsn = value.get("commit_lsn", "")
            advance_lsn = bool(value.get("advance_lsn", True))
            return (
                str(xid) if isinstance(xid, str) or isinstance(xid, int) else "",
                int(event_index) if isinstance(event_index, int) or str(event_index).isdigit() else 0,
                str(commit_lsn) if isinstance(commit_lsn, str) else "",
                advance_lsn,
            )
        return "", 0, "", False

    def _inspect_cdc_state(self, connection: Any, specs: list[PostgresTableSpec]) -> dict[str, Any] | None:
        if self._capture_mode() == "trigger":
            return self._inspect_trigger_cdc_state(connection, specs)
        if self._capture_mode() == "logical":
            return self._inspect_logical_cdc_state(connection, specs)
        return None

    def _inspect_trigger_cdc_state(self, connection: Any, specs: list[PostgresTableSpec]) -> dict[str, Any]:
        trigger_names = [f"dn_cdc_{spec.schema_name}_{spec.table_name}" for spec in specs]
        with connection.cursor() as cursor:
            schema_exists_row = cursor.execute(
                "select exists(select 1 from pg_namespace where nspname = 'denotary_cdc') as exists"
            ).fetchone()
            events_table_row = cursor.execute(
                """
                select exists(
                    select 1
                    from pg_class c
                    join pg_namespace n on n.oid = c.relnamespace
                    where n.nspname = 'denotary_cdc' and c.relname = 'events'
                ) as exists
                """
            ).fetchone()
            trigger_count_row = cursor.execute(
                """
                select count(*) as total
                from pg_trigger
                where tgname = any(%s) and not tgisinternal
                """,
                (trigger_names,),
            ).fetchone()
            pending_row = cursor.execute("select count(*) as total from denotary_cdc.events").fetchone()
        return {
            "schema": "denotary_cdc",
            "events_table": "denotary_cdc.events",
            "listener_channel": "denotary_cdc_events",
            "schema_exists": bool(schema_exists_row["exists"]),
            "events_table_exists": bool(events_table_row["exists"]),
            "configured_trigger_count": len(trigger_names),
            "installed_trigger_count": int(trigger_count_row["total"]),
            "pending_event_rows": int(pending_row["total"]),
        }

    def _inspect_logical_cdc_state(self, connection: Any, specs: list[PostgresTableSpec]) -> dict[str, Any]:
        publication_name = self._logical_publication_name()
        with connection.cursor() as cursor:
            wal_level_row = cursor.execute("show wal_level").fetchone()
            slot_row = cursor.execute(
                """
                select slot_name, plugin, active, restart_lsn, confirmed_flush_lsn, wal_status
                from pg_replication_slots
                where slot_name = %s
                """,
                (self._logical_slot_name(),),
            ).fetchone()
            publication_row = cursor.execute(
                """
                select pubname
                from pg_publication
                where pubname = %s
                """,
                (publication_name,),
            ).fetchone()
            publication_tables = []
            if publication_row is not None:
                publication_table_rows = cursor.execute(
                    """
                    select schemaname, tablename
                    from pg_publication_tables
                    where pubname = %s
                    order by schemaname, tablename
                    """,
                    (publication_name,),
                ).fetchall()
                publication_tables = [
                    f"{row['schemaname']}.{row['tablename']}"
                    for row in publication_table_rows
                ]
        return {
            "mode": "logical",
            "slot_name": self._logical_slot_name(),
            "plugin": self._logical_output_plugin(),
            "publication_name": publication_name,
            "publication_exists": publication_row is not None,
            "publication_tables": publication_tables,
            "tracked_table_count": len(specs),
            "wal_level": str(wal_level_row["wal_level"]),
            "slot_exists": slot_row is not None,
            "slot_active": bool(slot_row["active"]) if slot_row else False,
            "restart_lsn": str(slot_row["restart_lsn"]) if slot_row and slot_row["restart_lsn"] is not None else "",
            "confirmed_flush_lsn": (
                str(slot_row["confirmed_flush_lsn"])
                if slot_row and slot_row["confirmed_flush_lsn"] is not None
                else ""
            ),
            "wal_status": str(slot_row["wal_status"]) if slot_row and slot_row["wal_status"] is not None else "",
        }

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
        if value is None:
            return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        raise ValueError(f"unsupported PostgreSQL timestamp value: {value!r}")

    def _pk_marker(self, primary_key: dict[str, Any], columns: list[str]) -> str:
        return "|".join(self._coerce_pk_value(primary_key[column]) for column in columns)

    def _coerce_pk_value(self, value: Any) -> str:
        if value is None:
            return ""
        return str(value)

    def _quote_identifier(self, value: str) -> str:
        return '"' + value.replace('"', '""') + '"'

    def _logical_slot_name(self) -> str:
        return str(self.config.options.get("slot_name", f"denotary_{self.config.id}_slot"))

    def _logical_output_plugin(self) -> str:
        return str(self.config.options.get("output_plugin", "test_decoding"))

    def _logical_publication_name(self) -> str:
        return str(self.config.options.get("publication_name", f"denotary_{self.config.id}_pub"))

    def _parse_test_decoding_change(
        self,
        data: str,
        spec_map: dict[str, PostgresTableSpec],
    ) -> tuple[PostgresTableSpec, str, dict[str, Any] | None, dict[str, Any] | None] | None:
        match = re.match(r"table\s+([^.]+)\.([^:]+):\s+([A-Z]+):\s*(.*)$", data)
        if not match:
            return None
        schema_name, table_name, operation, remainder = match.groups()
        spec = spec_map.get(f"{schema_name}.{table_name}")
        if spec is None:
            return None

        operation_lower = operation.lower()
        if operation_lower == "insert":
            after_row = self._parse_test_decoding_tuple(remainder)
            return spec, operation_lower, None, after_row

        old_tuple = self._extract_named_tuple(remainder, "old-tuple:")
        new_tuple = self._extract_named_tuple(remainder, "new-tuple:")
        old_key = self._extract_named_tuple(remainder, "old-key:")

        if operation_lower == "update":
            before_row = old_tuple or old_key or None
            after_row = new_tuple or self._parse_test_decoding_tuple(remainder)
            return spec, operation_lower, before_row, after_row
        if operation_lower == "delete":
            before_row = old_tuple or old_key or self._parse_test_decoding_tuple(remainder)
            return spec, operation_lower, before_row, None
        return None

    def _parse_pgoutput_message(
        self,
        payload: bytes,
        spec_map: dict[str, PostgresTableSpec],
    ) -> dict[str, Any] | None:
        if not payload:
            return None
        cursor = 0
        message_type = chr(payload[cursor])
        cursor += 1

        if message_type == "B":
            _final_lsn, cursor = self._read_int64(payload, cursor)
            _commit_time, cursor = self._read_int64(payload, cursor)
            xid, cursor = self._read_int32(payload, cursor)
            return {"kind": "begin", "xid": str(xid)}

        if message_type == "C":
            _flags, cursor = self._read_int8(payload, cursor)
            commit_lsn, cursor = self._read_int64(payload, cursor)
            _end_lsn, cursor = self._read_int64(payload, cursor)
            _commit_time, cursor = self._read_int64(payload, cursor)
            return {"kind": "commit", "commit_lsn": self._format_pg_lsn(commit_lsn)}

        if message_type == "R":
            relation, _cursor = self._parse_pgoutput_relation(payload, cursor)
            self._logical_relation_cache[relation.relid] = relation
            return {"kind": "relation"}

        if message_type == "I":
            relid, cursor = self._read_int32(payload, cursor)
            relation = self._logical_relation_cache.get(relid)
            if relation is None:
                return None
            tuple_kind, cursor = self._read_char(payload, cursor)
            if tuple_kind != "N":
                return None
            after_row, cursor = self._parse_pgoutput_tuple(payload, cursor, relation)
            spec = spec_map.get(relation.key)
            if spec is None:
                return None
            return {"kind": "data", "spec": spec, "operation": "insert", "before_row": None, "after_row": after_row}

        if message_type == "U":
            relid, cursor = self._read_int32(payload, cursor)
            relation = self._logical_relation_cache.get(relid)
            if relation is None:
                return None
            before_row = None
            tag, cursor = self._read_char(payload, cursor)
            if tag in {"K", "O"}:
                before_row, cursor = self._parse_pgoutput_tuple(payload, cursor, relation)
                tag, cursor = self._read_char(payload, cursor)
            if tag != "N":
                return None
            after_row, cursor = self._parse_pgoutput_tuple(payload, cursor, relation)
            spec = spec_map.get(relation.key)
            if spec is None:
                return None
            return {"kind": "data", "spec": spec, "operation": "update", "before_row": before_row, "after_row": after_row}

        if message_type == "D":
            relid, cursor = self._read_int32(payload, cursor)
            relation = self._logical_relation_cache.get(relid)
            if relation is None:
                return None
            tag, cursor = self._read_char(payload, cursor)
            if tag not in {"K", "O"}:
                return None
            before_row, cursor = self._parse_pgoutput_tuple(payload, cursor, relation)
            spec = spec_map.get(relation.key)
            if spec is None:
                return None
            return {"kind": "data", "spec": spec, "operation": "delete", "before_row": before_row, "after_row": None}

        if message_type in {"Y", "O", "T"}:
            return {"kind": "other"}
        return None

    def _parse_pgoutput_relation(self, payload: bytes, cursor: int) -> tuple[PostgresLogicalRelation, int]:
        relid, cursor = self._read_int32(payload, cursor)
        schema_name, cursor = self._read_cstring(payload, cursor)
        table_name, cursor = self._read_cstring(payload, cursor)
        replica_identity, cursor = self._read_char(payload, cursor)
        column_count, cursor = self._read_int16(payload, cursor)
        columns: list[PostgresLogicalRelationColumn] = []
        for _ in range(column_count):
            _flags, cursor = self._read_int8(payload, cursor)
            column_name, cursor = self._read_cstring(payload, cursor)
            type_oid, cursor = self._read_int32(payload, cursor)
            type_modifier, cursor = self._read_int32(payload, cursor)
            columns.append(
                PostgresLogicalRelationColumn(
                    name=column_name,
                    type_oid=type_oid,
                    type_modifier=type_modifier,
                )
            )
        return (
            PostgresLogicalRelation(
                relid=relid,
                schema_name=schema_name,
                table_name=table_name,
                replica_identity=replica_identity,
                columns=columns,
            ),
            cursor,
        )

    def _parse_pgoutput_tuple(
        self,
        payload: bytes,
        cursor: int,
        relation: PostgresLogicalRelation,
    ) -> tuple[dict[str, Any], int]:
        column_count, cursor = self._read_int16(payload, cursor)
        values: dict[str, Any] = {}
        for index in range(column_count):
            column = relation.columns[index]
            kind, cursor = self._read_char(payload, cursor)
            if kind == "n":
                values[column.name] = None
                continue
            if kind == "u":
                values[column.name] = None
                continue
            length, cursor = self._read_int32(payload, cursor)
            raw = payload[cursor : cursor + length]
            cursor += length
            values[column.name] = self._decode_pgoutput_value(raw, column.type_oid, kind)
        return values, cursor

    def _decode_pgoutput_value(self, raw: bytes, type_oid: int, kind: str) -> Any:
        if kind == "b":
            return raw.hex()
        text = raw.decode("utf-8")
        if type_oid in {20, 21, 23, 26}:
            return int(text)
        if type_oid in {700, 701}:
            return float(text)
        if type_oid == 16:
            return text in {"t", "true", "1"}
        if type_oid == 1700:
            return text
        if type_oid in {114, 3802}:
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return text
        if type_oid == 17:
            return text
        return text

    def _read_int8(self, payload: bytes, cursor: int) -> tuple[int, int]:
        return payload[cursor], cursor + 1

    def _read_int16(self, payload: bytes, cursor: int) -> tuple[int, int]:
        return struct.unpack_from("!H", payload, cursor)[0], cursor + 2

    def _read_int32(self, payload: bytes, cursor: int) -> tuple[int, int]:
        return struct.unpack_from("!I", payload, cursor)[0], cursor + 4

    def _read_int64(self, payload: bytes, cursor: int) -> tuple[int, int]:
        return struct.unpack_from("!Q", payload, cursor)[0], cursor + 8

    def _read_char(self, payload: bytes, cursor: int) -> tuple[str, int]:
        return chr(payload[cursor]), cursor + 1

    def _read_cstring(self, payload: bytes, cursor: int) -> tuple[str, int]:
        end = payload.index(0, cursor)
        return payload[cursor:end].decode("utf-8"), end + 1

    def _format_pg_lsn(self, value: int) -> str:
        upper = value >> 32
        lower = value & 0xFFFFFFFF
        return f"{upper:X}/{lower:X}"

    def _extract_named_tuple(self, text: str, marker: str) -> dict[str, Any]:
        marker_index = text.find(marker)
        if marker_index < 0:
            return {}
        start = marker_index + len(marker)
        next_indices = [
            index
            for index in (text.find(" old-key:", start), text.find(" old-tuple:", start), text.find(" new-tuple:", start))
            if index >= 0
        ]
        end = min(next_indices) if next_indices else len(text)
        return self._parse_test_decoding_tuple(text[start:end].strip())

    def _parse_test_decoding_tuple(self, text: str) -> dict[str, Any]:
        values: dict[str, Any] = {}
        token_pattern = re.compile(r"([A-Za-z_][A-Za-z0-9_]*)\[[^\]]+\]:(NULL|'(?:''|[^'])*'|[^\s]+)")
        for match in token_pattern.finditer(text):
            key = match.group(1)
            raw_value = match.group(2)
            values[key] = self._decode_test_decoding_value(raw_value)
        return values

    def _decode_test_decoding_value(self, raw_value: str) -> Any:
        if raw_value.upper() == "NULL":
            return None
        if raw_value.startswith("'") and raw_value.endswith("'"):
            return raw_value[1:-1].replace("''", "'")
        if re.fullmatch(r"-?\d+", raw_value):
            return int(raw_value)
        if raw_value.lower() in {"true", "false"}:
            return raw_value.lower() == "true"
        return raw_value

    def _derive_primary_key(
        self,
        spec: PostgresTableSpec,
        before_row: dict[str, Any] | None,
        after_row: dict[str, Any] | None,
    ) -> dict[str, Any]:
        source_row = after_row or before_row or {}
        return {column: source_row.get(column) for column in spec.primary_key_columns}

    def _derive_commit_timestamp(
        self,
        spec: PostgresTableSpec,
        before_row: dict[str, Any] | None,
        after_row: dict[str, Any] | None,
    ) -> str:
        source_row = after_row or before_row or {}
        value = source_row.get(spec.commit_timestamp_column)
        return self._normalize_timestamp(value)

    def _lsn_compare(self, left: str, right: str) -> int:
        if not right:
            return 1
        return (self._lsn_to_int(left) > self._lsn_to_int(right)) - (self._lsn_to_int(left) < self._lsn_to_int(right))

    def _lsn_to_int(self, value: str) -> int:
        upper, lower = value.split("/")
        return (int(upper, 16) << 32) + int(lower, 16)

    def _capture_mode(self) -> str:
        return str(self.config.options.get("capture_mode", "watermark")).lower()

    def _spec_summary(self, spec: PostgresTableSpec) -> dict[str, Any]:
        return {
            "schema": spec.schema_name,
            "table": spec.table_name,
            "watermark_column": spec.watermark_column,
            "commit_timestamp_column": spec.commit_timestamp_column,
            "primary_key_columns": spec.primary_key_columns,
            "selected_columns": spec.selected_columns,
        }

    def _spec_signature_entry(self, spec: PostgresTableSpec) -> dict[str, Any]:
        return {
            "schema": spec.schema_name,
            "table": spec.table_name,
            "watermark_column": spec.watermark_column,
            "commit_timestamp_column": spec.commit_timestamp_column,
            "primary_key_columns": spec.primary_key_columns,
            "selected_columns": spec.selected_columns,
        }

    def _ensure_listener(self) -> Any:
        if self._listener_connection is not None and not self._listener_connection.closed:
            return self._listener_connection
        if psycopg is None:
            raise RuntimeError("psycopg is required for PostgreSQL LISTEN/NOTIFY")
        listener = psycopg.connect(
            host=self.config.connection.get("host"),
            port=self.config.connection.get("port"),
            user=self.config.connection.get("username"),
            password=self.config.connection.get("password"),
            dbname=self.config.connection.get("database"),
            sslmode=self.config.connection.get("sslmode", "prefer"),
            connect_timeout=int(self.config.connection.get("connect_timeout", 5)),
            autocommit=True,
        )
        with listener.cursor() as cursor:
            cursor.execute("listen denotary_cdc_events")
        self._listener_connection = listener
        return listener
