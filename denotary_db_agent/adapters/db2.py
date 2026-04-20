from __future__ import annotations

import json
import os
import site
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterator

from denotary_db_agent.adapters.base import AdapterCapabilities, BaseAdapter
from denotary_db_agent.models import ChangeEvent, SourceCheckpoint

_DB2_DLL_BOOTSTRAPPED = False


def _bootstrap_db2_windows_dlls() -> None:
    global _DB2_DLL_BOOTSTRAPPED
    if _DB2_DLL_BOOTSTRAPPED or os.name != "nt" or not hasattr(os, "add_dll_directory"):
        return
    candidates = [Path(path) for path in site.getsitepackages()] + [Path(site.getusersitepackages())]
    for base in candidates:
        clidriver_bin = base / "clidriver" / "bin"
        if not clidriver_bin.exists():
            continue
        os.add_dll_directory(str(clidriver_bin))
        for extra in ("amd64.VC14.CRT", "amd64.VC12.CRT"):
            extra_path = clidriver_bin / extra
            if extra_path.exists():
                os.add_dll_directory(str(extra_path))
        _DB2_DLL_BOOTSTRAPPED = True
        return


_bootstrap_db2_windows_dlls()

try:
    import ibm_db_dbi
except ImportError:  # pragma: no cover - handled in runtime validation
    ibm_db_dbi = None


@dataclass
class Db2TableSpec:
    schema_name: str
    table_name: str
    watermark_column: str
    commit_timestamp_column: str
    primary_key_columns: list[str]
    selected_columns: list[str]

    @property
    def key(self) -> str:
        return f"{self.schema_name}.{self.table_name}"


class Db2Adapter(BaseAdapter):
    source_type = "db2"
    minimum_version = "11.5"
    adapter_notes = (
        "Wave 2 Db2 support currently provides connection-shape validation, live ping, tracked-table introspection, "
        "watermark snapshot polling, deterministic checkpoint resume, and dry-run playback. Native Db2 change data "
        "capture can be added later if commercially justified."
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
        required = ("host", "port", "username", "password", "database")
        missing = [name for name in required if not self.config.connection.get(name)]
        if missing:
            raise ValueError(f"{self.source_type} connection is missing required fields: {', '.join(missing)}")
        if self.config.options.get("dry_run_events"):
            self._configured_table_specs()
            return
        if ibm_db_dbi is None:
            raise RuntimeError("ibm_db is required for live db2 adapter use")
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute("select 1 as ok from sysibm.sysdummy1")
                cursor.fetchone()
            self._load_table_specs(connection)

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
        raise NotImplementedError("db2 native CDC is not implemented yet; use capture_mode=watermark")

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
                schema_or_namespace=str(item.get("schema_or_namespace", "DB2INST1")),
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

        with self._connect() as connection:
            specs = self._load_table_specs(connection)
            candidates: list[tuple[Db2TableSpec, dict[str, Any]]] = []
            for spec in specs:
                table_state = checkpoint_state.get(spec.key)
                candidates.extend((spec, row) for row in self._fetch_rows(connection, spec, table_state))

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
        assert ibm_db_dbi is not None
        dsn = (
            f"DATABASE={self.config.connection['database']};"
            f"HOSTNAME={self.config.connection['host']};"
            f"PORT={int(self.config.connection['port'])};"
            "PROTOCOL=TCPIP;"
            f"UID={self.config.connection['username']};"
            f"PWD={self.config.connection['password']};"
        )
        connection = ibm_db_dbi.connect(dsn, "", "")
        try:
            yield connection
        finally:
            connection.close()

    def _load_live_or_configured_specs(self) -> list[Db2TableSpec]:
        if self.config.options.get("dry_run_events"):
            return self._configured_table_specs()
        if ibm_db_dbi is None:
            return self._configured_table_specs()
        with self._connect() as connection:
            return self._load_table_specs(connection)

    def _configured_table_specs(self) -> list[Db2TableSpec]:
        include = self.config.include or {}
        watermark_column = str(self.config.options.get("watermark_column", "updated_at")).strip() or "updated_at"
        commit_timestamp_column = str(self.config.options.get("commit_timestamp_column", watermark_column)).strip() or watermark_column
        primary_key_columns = self._primary_key_columns()
        specs: list[Db2TableSpec] = []
        for schema_name, table_names in include.items():
            if not isinstance(schema_name, str) or not schema_name.strip():
                raise ValueError("db2 include schema names must be non-empty strings")
            normalized_schema = schema_name.strip().upper()
            if not isinstance(table_names, list) or not all(isinstance(item, str) and item.strip() for item in table_names):
                raise ValueError(f"db2 include[{normalized_schema}] must be an array of non-empty table names")
            for raw_table_name in table_names:
                table_name = raw_table_name.strip().upper()
                specs.append(
                    Db2TableSpec(
                        schema_name=normalized_schema,
                        table_name=table_name,
                        watermark_column=watermark_column.upper(),
                        commit_timestamp_column=commit_timestamp_column.upper(),
                        primary_key_columns=[column.upper() for column in primary_key_columns],
                        selected_columns=[*(column.upper() for column in primary_key_columns), watermark_column.upper(), commit_timestamp_column.upper()],
                    )
                )
        return specs

    def _load_table_specs(self, connection: Any) -> list[Db2TableSpec]:
        configured = self._configured_table_specs()
        specs: list[Db2TableSpec] = []
        for item in configured:
            columns = self._load_table_columns(connection, item)
            column_lookup = {column.lower(): column for column in columns}
            actual_watermark_column = column_lookup.get(item.watermark_column.lower())
            if actual_watermark_column is None:
                raise ValueError(
                    f"{self.source_type} table {item.schema_name}.{item.table_name} does not contain watermark column {item.watermark_column}"
                )
            actual_commit_timestamp_column = column_lookup.get(item.commit_timestamp_column.lower())
            if actual_commit_timestamp_column is None:
                raise ValueError(
                    f"{self.source_type} table {item.schema_name}.{item.table_name} does not contain commit timestamp column {item.commit_timestamp_column}"
                )
            actual_primary_key_columns = [column_lookup.get(column.lower()) for column in item.primary_key_columns]
            if any(column is None for column in actual_primary_key_columns):
                missing = [column for column, actual in zip(item.primary_key_columns, actual_primary_key_columns) if actual is None]
                raise ValueError(
                    f"{self.source_type} table {item.schema_name}.{item.table_name} does not contain primary key column(s): {', '.join(missing)}"
                )
            discovered_primary_keys = self._load_primary_key_columns(connection, item)
            primary_key_columns = discovered_primary_keys or [str(column) for column in actual_primary_key_columns]
            specs.append(
                Db2TableSpec(
                    schema_name=item.schema_name,
                    table_name=item.table_name,
                    watermark_column=actual_watermark_column,
                    commit_timestamp_column=actual_commit_timestamp_column,
                    primary_key_columns=primary_key_columns,
                    selected_columns=columns,
                )
            )
        return specs

    def _load_table_columns(self, connection: Any, spec: Db2TableSpec) -> list[str]:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                select colname
                from syscat.columns
                where tabschema = ?
                  and tabname = ?
                order by colno
                """,
                (spec.schema_name, spec.table_name),
            )
            rows = self._fetchall_rows(cursor)
        columns = [str(self._row_get(row, "colname")) for row in rows if self._row_get(row, "colname") is not None]
        if not columns:
            raise ValueError(f"{self.source_type} table {spec.schema_name}.{spec.table_name} is not visible in syscat.columns")
        return columns

    def _load_primary_key_columns(self, connection: Any, spec: Db2TableSpec) -> list[str]:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                select k.colname
                from syscat.tabconst c
                join syscat.keycoluse k
                  on c.tabschema = k.tabschema
                 and c.tabname = k.tabname
                 and c.constname = k.constname
                where c.tabschema = ?
                  and c.tabname = ?
                  and c.type = 'P'
                order by k.colseq
                """,
                (spec.schema_name, spec.table_name),
            )
            rows = self._fetchall_rows(cursor)
        return [str(self._row_get(row, "colname")) for row in rows if self._row_get(row, "colname") is not None]

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

    def _fetch_rows(self, connection: Any, spec: Db2TableSpec, table_state: dict[str, Any] | None) -> list[dict[str, Any]]:
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
            watermark_value = self._db2_timestamp_param(table_state.get("watermark"))
            sql += (
                f" and ("
                f"{self._quote_identifier(spec.watermark_column)} > ? "
                f"or ("
                f"{self._quote_identifier(spec.watermark_column)} = ? and ("
                + " or ".join(self._lexicographic_pk_predicates(spec.primary_key_columns))
                + ")))"
            )
            params.extend([watermark_value, watermark_value, *pk_values * len(spec.primary_key_columns)])
        sql += f" order by {order_by}"
        row_limit = int(self.config.options.get("row_limit", self.config.batch_size))
        if row_limit > 0:
            sql += " fetch first ? rows only"
            params.append(row_limit)
        with connection.cursor() as cursor:
            cursor.execute(sql, tuple(params) if params else None)
            rows = self._fetchall_rows(cursor)
        return list(rows)

    def _fetchall_rows(self, cursor: Any) -> list[Any]:
        rows = cursor.fetchall()
        description = getattr(cursor, "description", None) or []
        column_names = [str(item[0]).lower() for item in description if item and item[0]]
        if not column_names:
            return list(rows)
        normalized: list[Any] = []
        for row in rows:
            if isinstance(row, dict):
                normalized.append(row)
                continue
            if isinstance(row, (list, tuple)) and len(row) == len(column_names):
                normalized.append({name: value for name, value in zip(column_names, row)})
                continue
            normalized.append(row)
        return normalized

    def _lexicographic_pk_predicates(self, primary_key_columns: list[str]) -> list[str]:
        predicates: list[str] = []
        for index, column in enumerate(primary_key_columns):
            parts = [f"{self._quote_identifier(previous)} = ?" for previous in primary_key_columns[:index]]
            parts.append(f"{self._quote_identifier(column)} > ?")
            predicates.append("(" + " and ".join(parts) + ")")
        return predicates

    def _sort_key(self, spec: Db2TableSpec, row: dict[str, Any]) -> tuple[Any, ...]:
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

    def _db2_timestamp_param(self, value: Any) -> Any:
        if isinstance(value, str):
            candidate = value.strip()
            if candidate.endswith("Z"):
                candidate = candidate[:-1] + "+00:00"
            try:
                parsed = datetime.fromisoformat(candidate)
            except ValueError:
                return value
            if parsed.tzinfo is not None:
                parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
            return parsed
        return value

    def _coerce_pk_value(self, value: Any) -> Any:
        normalized = self._normalize_value(value)
        if isinstance(normalized, (str, int, float, bool)) or normalized is None:
            return normalized
        return str(normalized)

    def _pk_marker(self, primary_key: dict[str, Any], primary_key_columns: list[str]) -> str:
        return ":".join(str(self._coerce_pk_value(primary_key[column])) for column in primary_key_columns)

    def _qualified_table(self, schema_name: str, table_name: str) -> str:
        return ".".join((self._quote_identifier(schema_name), self._quote_identifier(table_name)))

    def _quote_identifier(self, value: str) -> str:
        return '"' + value.replace('"', '""') + '"'

    def _spec_summary(self, spec: Db2TableSpec) -> dict[str, object]:
        return {
            "schema_name": spec.schema_name,
            "table_name": spec.table_name,
            "watermark_column": spec.watermark_column,
            "commit_timestamp_column": spec.commit_timestamp_column,
            "primary_key_columns": list(spec.primary_key_columns),
            "selected_columns": list(spec.selected_columns),
        }

    def _spec_signature_entry(self, spec: Db2TableSpec) -> dict[str, object]:
        return {
            "key": spec.key,
            "watermark_column": spec.watermark_column,
            "commit_timestamp_column": spec.commit_timestamp_column,
            "primary_key_columns": list(spec.primary_key_columns),
            "selected_columns": list(spec.selected_columns),
        }
