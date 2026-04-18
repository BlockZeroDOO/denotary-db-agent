from __future__ import annotations

import json
import re
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal
from typing import Any, Iterator

from denotary_db_agent.adapters.base import AdapterCapabilities, BaseAdapter
from denotary_db_agent.models import ChangeEvent, SourceCheckpoint

try:
    import oracledb
except ImportError:  # pragma: no cover - handled in runtime validation
    oracledb = None


@dataclass
class OracleTableSpec:
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
class OracleLogMinerCheckpoint:
    scn: int
    rs_id: str
    ssn: int
    row_index: int = 0

    @property
    def token(self) -> str:
        return json.dumps(
            {
                "mode": "logminer",
                "scn": self.scn,
                "rs_id": self.rs_id,
                "ssn": self.ssn,
                "row_index": self.row_index,
            },
            sort_keys=True,
        )


class OracleAdapter(BaseAdapter):
    source_type = "oracle"
    minimum_version = "19c"
    adapter_notes = (
        "Oracle supports both watermark-based snapshot polling and a root-admin LogMiner CDC baseline. "
        "The current LogMiner path mines recent online redo logs and resumes from an SCN checkpoint."
    )

    def __init__(self, config):
        super().__init__(config)
        self._logminer_initial_scn: int | None = None

    def discover_capabilities(self) -> AdapterCapabilities:
        capture_mode = self._capture_mode()
        return AdapterCapabilities(
            source_type=self.source_type,
            minimum_version=self.minimum_version,
            supports_cdc=True,
            supports_snapshot=True,
            operations=("snapshot",) if capture_mode == "watermark" else ("insert", "update", "delete"),
            capture_modes=("watermark", "logminer"),
            cdc_modes=("logminer",),
            default_capture_mode="watermark",
            checkpoint_strategy="table_watermark" if capture_mode == "watermark" else "logminer_scn",
            activity_model="polling",
            bootstrap_requirements=(
                ("tracked tables visible", "watermark columns configured")
                if capture_mode == "watermark"
                else (
                    "tracked tables visible",
                    "LogMiner root-admin connection configured",
                    "database supplemental logging enabled",
                    "accessible online redo members discovered",
                    "redo checkpoints must remain within the online redo window",
                )
            ),
            notes=self.adapter_notes,
        )

    def validate_connection(self) -> None:
        required = ("host", "port", "username", "service_name")
        missing = [name for name in required if not self.config.connection.get(name)]
        if missing:
            raise ValueError(f"{self.source_type} connection is missing required fields: {', '.join(missing)}")
        if oracledb is None:
            raise RuntimeError("oracledb is required for live oracle adapter use")
        if self.config.options.get("dry_run_events"):
            return
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute("select 1 as ok from dual")
                cursor.fetchone()
            self._load_table_specs(connection)
        if self._capture_mode() == "logminer":
            with self._connect_logminer_admin() as admin_connection:
                self._validate_logminer_prerequisites(admin_connection)

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
        cdc = self._cdc_summary() if self._capture_mode() == "logminer" else None
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
        cdc = self._cdc_summary() if self._capture_mode() == "logminer" else None
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
        if self._capture_mode() != "logminer":
            raise NotImplementedError("oracle CDC streaming is only available when capture_mode is set to logminer")
        if self.config.options.get("dry_run_events"):
            return iter(())

        with self._connect() as connection:
            specs = self._load_table_specs(connection)
            source_container_name = self._read_source_container_name(connection)
        checkpoint_state = self._parse_logminer_checkpoint(checkpoint)
        with self._connect_logminer_admin() as admin_connection:
            self._validate_logminer_prerequisites(admin_connection)
            if checkpoint_state is None:
                if self._logminer_initial_scn is None:
                    self._logminer_initial_scn = self._current_root_scn(admin_connection)
                return iter(())
            self._start_logminer_session(admin_connection, checkpoint_state.scn)
            try:
                rows = self._fetch_logminer_rows(admin_connection, specs, source_container_name, checkpoint_state)
            finally:
                self._end_logminer_session(admin_connection)

        pending: list[ChangeEvent] = []
        for row_index, row in enumerate(rows, start=1):
            if (
                int(self._row_get(row, "scn")) == checkpoint_state.scn
                and str(self._row_get(row, "rs_id")) == checkpoint_state.rs_id
                and int(self._row_get(row, "ssn")) == checkpoint_state.ssn
                and row_index <= checkpoint_state.row_index
            ):
                continue
            spec = self._find_spec(specs, str(self._row_get(row, "seg_owner")), str(self._row_get(row, "table_name")))
            if spec is None:
                continue
            pending.append(self._build_logminer_change_event(spec, row, row_index))
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
            candidates: list[tuple[OracleTableSpec, dict[str, Any]]] = []
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
        if oracledb is None:
            raise RuntimeError("oracledb is required for live oracle adapter use")
        connection = oracledb.connect(
            user=str(self.config.connection["username"]),
            password=str(self.config.connection.get("password", "")),
            host=str(self.config.connection["host"]),
            port=int(self.config.connection["port"]),
            service_name=str(self.config.connection["service_name"]),
            tcp_connect_timeout=float(self.config.connection.get("tcp_connect_timeout", 10)),
        )
        try:
            yield connection
        finally:
            connection.close()

    def _capture_mode(self) -> str:
        return self.capture_mode()

    @contextmanager
    def _connect_logminer_admin(self) -> Iterator[Any]:
        if oracledb is None:
            raise RuntimeError("oracledb is required for live oracle adapter use")
        admin_username = str(self.config.connection.get("admin_username", "")).strip()
        admin_password = str(self.config.connection.get("admin_password", ""))
        admin_service_name = str(
            self.config.connection.get(
                "admin_service_name",
                self.config.options.get("logminer_root_service_name", ""),
            )
        ).strip()
        if not admin_username or not admin_service_name:
            raise ValueError(
                "oracle capture_mode=logminer requires admin_username and admin_service_name "
                "(or options.logminer_root_service_name) in connection settings"
            )
        connection = oracledb.connect(
            user=admin_username,
            password=admin_password,
            host=str(self.config.connection["host"]),
            port=int(self.config.connection["port"]),
            service_name=admin_service_name,
            tcp_connect_timeout=float(self.config.connection.get("tcp_connect_timeout", 10)),
        )
        try:
            yield connection
        finally:
            connection.close()

    def _cdc_summary(self) -> dict[str, Any]:
        with self._connect_logminer_admin() as admin_connection:
            return self.build_cdc_summary(self._read_logminer_summary(admin_connection))

    def _read_logminer_summary(self, admin_connection: Any) -> dict[str, Any]:
        with admin_connection.cursor() as cursor:
            cursor.execute("select sys_context('userenv', 'con_name') as con_name from dual")
            con_name_row = self._fetch_dict_rows(cursor)[0]
            cursor.execute("select name, open_mode, log_mode, supplemental_log_data_min, supplemental_log_data_pk from v$database")
            database_row = self._fetch_dict_rows(cursor)[0]
            cursor.execute(
                """
                select lf.member as member, l.status as status, l.sequence# as sequence_no
                from v$logfile lf
                join v$log l on lf.group# = l.group#
                order by l.group#, lf.member
                """
            )
            redo_rows = self._fetch_dict_rows(cursor)
            cursor.execute("select owner, object_name from all_objects where owner = 'PUBLIC' and object_name in ('DBMS_LOGMNR', 'DBMS_LOGMNR_D') order by object_name")
            package_rows = self._fetch_dict_rows(cursor)
        return {
            "capture_mode": "logminer",
            "admin_container_name": str(self._row_get(con_name_row, "con_name")),
            "log_mode": str(self._row_get(database_row, "log_mode")),
            "open_mode": str(self._row_get(database_row, "open_mode")),
            "supplemental_log_data_min": str(self._row_get(database_row, "supplemental_log_data_min")),
            "supplemental_log_data_pk": str(self._row_get(database_row, "supplemental_log_data_pk")),
            "redo_members": [
                {
                    "member": str(self._row_get(row, "member")),
                    "status": str(self._row_get(row, "status")),
                    "sequence_no": int(self._row_get(row, "sequence_no")),
                }
                for row in redo_rows
            ],
            "logminer_packages": [str(self._row_get(row, "object_name")) for row in package_rows],
        }

    def _read_source_container_name(self, connection: Any) -> str:
        with connection.cursor() as cursor:
            cursor.execute("select sys_context('userenv', 'con_name') as con_name from dual")
            row = self._fetch_dict_rows(cursor)[0]
        return str(self._row_get(row, "con_name"))

    def _current_root_scn(self, admin_connection: Any) -> int:
        with admin_connection.cursor() as cursor:
            cursor.execute("select current_scn from v$database")
            row = self._fetch_dict_rows(cursor)[0]
        return int(self._row_get(row, "current_scn"))

    def _start_logminer_session(self, admin_connection: Any, start_scn: int) -> None:
        redo_members = list(self._read_logminer_summary(admin_connection).get("redo_members") or [])
        if not redo_members:
            raise ValueError(f"{self.source_type} logminer admin connection could not discover online redo members")
        with admin_connection.cursor() as cursor:
            for index, entry in enumerate(redo_members):
                option = "dbms_logmnr.new" if index == 0 else "dbms_logmnr.addfile"
                cursor.execute(
                    f"begin dbms_logmnr.add_logfile(logfilename => :logfilename, options => {option}); end;",
                    logfilename=str(entry["member"]),
                )
            try:
                cursor.execute(
                    """
                    begin
                        dbms_logmnr.start_logmnr(
                            startScn => :start_scn,
                            options => dbms_logmnr.dict_from_online_catalog + dbms_logmnr.committed_data_only
                        );
                    end;
                    """,
                    start_scn=int(start_scn),
                )
            except Exception as exc:
                message = str(exc)
                if "ORA-01291" in message:
                    raise ValueError(
                        "oracle LogMiner checkpoint is older than the current online redo window; "
                        "bootstrap a fresh source or reset the source checkpoint"
                    ) from exc
                raise

    def _end_logminer_session(self, admin_connection: Any) -> None:
        with admin_connection.cursor() as cursor:
            try:
                cursor.execute("begin dbms_logmnr.end_logmnr; end;")
            except Exception:
                return None
        return None

    def _fetch_logminer_rows(
        self,
        admin_connection: Any,
        specs: list[OracleTableSpec],
        source_container_name: str,
        checkpoint: OracleLogMinerCheckpoint,
    ) -> list[dict[str, Any]]:
        filters: list[str] = []
        binds: dict[str, Any] = {
            "source_container_name": source_container_name,
            "checkpoint_scn": int(checkpoint.scn),
            "checkpoint_rs_id": checkpoint.rs_id,
            "checkpoint_ssn": int(checkpoint.ssn),
        }
        for index, spec in enumerate(specs):
            filters.append(f"(seg_owner = :owner_{index} and table_name = :table_{index})")
            binds[f"owner_{index}"] = spec.schema_name
            binds[f"table_{index}"] = spec.table_name
        if not filters:
            return []
        sql = f"""
            select
                scn,
                rs_id,
                ssn,
                timestamp,
                operation_code,
                operation,
                seg_owner,
                table_name,
                sql_redo,
                sql_undo,
                src_con_name
            from v$logmnr_contents
            where src_con_name = :source_container_name
              and operation_code in (1, 2, 3)
              and ({' or '.join(filters)})
              and (
                    scn > :checkpoint_scn
                    or (
                        scn = :checkpoint_scn
                        and (
                            rs_id > :checkpoint_rs_id
                            or (rs_id = :checkpoint_rs_id and ssn >= :checkpoint_ssn)
                        )
                    )
              )
            order by scn, rs_id, ssn
        """
        row_limit = int(self.config.options.get("row_limit", self.config.batch_size))
        if row_limit > 0:
            sql += f" fetch first {row_limit} rows only"
        with admin_connection.cursor() as cursor:
            cursor.execute(sql, binds)
            return self._fetch_dict_rows(cursor)

    def _find_spec(self, specs: list[OracleTableSpec], schema_name: str, table_name: str) -> OracleTableSpec | None:
        for spec in specs:
            if spec.schema_name == schema_name and spec.table_name == table_name:
                return spec
        return None

    def _build_logminer_change_event(self, spec: OracleTableSpec, row: dict[str, Any], row_index: int) -> ChangeEvent:
        operation = str(self._row_get(row, "operation")).lower()
        sql_redo = str(self._row_get(row, "sql_redo") or "")
        sql_undo = str(self._row_get(row, "sql_undo") or "")
        if operation == "insert":
            before = None
            after = self._parse_insert_values(sql_redo)
        elif operation == "delete":
            before = self._parse_delete_values(sql_redo)
            after = None
        elif operation == "update":
            before = self._parse_update_before(sql_redo, sql_undo)
            after = self._parse_update_after(sql_redo, sql_undo)
        else:
            raise ValueError(f"unsupported oracle logminer operation: {operation}")
        primary_key_source = after or before or {}
        primary_key = {
            column: primary_key_source.get(column)
            for column in spec.primary_key_columns
        }
        checkpoint = OracleLogMinerCheckpoint(
            scn=int(self._row_get(row, "scn")),
            rs_id=str(self._row_get(row, "rs_id")),
            ssn=int(self._row_get(row, "ssn")),
            row_index=row_index,
        )
        return ChangeEvent(
            source_id=self.config.id,
            source_type=self.source_type,
            source_instance=self.config.source_instance,
            database_name=self.config.database_name,
            schema_or_namespace=spec.schema_name,
            table_or_collection=spec.table_name,
            operation=operation,  # type: ignore[arg-type]
            primary_key={key: self._normalize_value(value) for key, value in primary_key.items()},
            change_version=f"{checkpoint.scn}:{checkpoint.rs_id}:{checkpoint.ssn}:{row_index}",
            commit_timestamp=self._normalize_timestamp(self._row_get(row, "timestamp")),
            before=self._normalize_document(before),
            after=self._normalize_document(after),
            metadata={
                "capture_mode": "logminer-cdc",
                "scn": checkpoint.scn,
                "rs_id": checkpoint.rs_id,
                "ssn": checkpoint.ssn,
                "row_index": row_index,
                "src_con_name": str(self._row_get(row, "src_con_name")),
                "sql_redo": sql_redo,
                "sql_undo": sql_undo,
                "primary_key_columns": list(spec.primary_key_columns),
            },
            checkpoint_token=checkpoint.token,
        )

    def _validate_logminer_prerequisites(self, admin_connection: Any) -> None:
        summary = self._read_logminer_summary(admin_connection)
        if summary["admin_container_name"] != "CDB$ROOT":
            raise ValueError(
                f"{self.source_type} logminer admin connection must target CDB$ROOT, got {summary['admin_container_name']}"
            )
        if str(summary["supplemental_log_data_min"]).upper() not in {"YES", "IMPLICIT"}:
            raise ValueError(f"{self.source_type} database supplemental logging must be enabled for capture_mode=logminer")
        if not summary["redo_members"]:
            raise ValueError(f"{self.source_type} logminer admin connection could not discover online redo members")
        packages = set(str(item).upper() for item in summary["logminer_packages"])
        if {"DBMS_LOGMNR", "DBMS_LOGMNR_D"} - packages:
            raise ValueError(f"{self.source_type} LogMiner packages are not accessible on the admin connection")

    def _parse_logminer_checkpoint(self, checkpoint: SourceCheckpoint | None) -> OracleLogMinerCheckpoint | None:
        if checkpoint is None or not checkpoint.token:
            if self._logminer_initial_scn is None:
                return None
            return OracleLogMinerCheckpoint(scn=self._logminer_initial_scn, rs_id="", ssn=0, row_index=0)
        payload = json.loads(checkpoint.token)
        if not isinstance(payload, dict):
            raise ValueError(f"{self.source_type} logminer checkpoint token must be a JSON object")
        if payload.get("mode") != "logminer":
            if self._logminer_initial_scn is None:
                return None
            return OracleLogMinerCheckpoint(scn=self._logminer_initial_scn, rs_id="", ssn=0, row_index=0)
        return OracleLogMinerCheckpoint(
            scn=int(payload["scn"]),
            rs_id=str(payload.get("rs_id", "")),
            ssn=int(payload.get("ssn", 0)),
            row_index=int(payload.get("row_index", 0)),
        )

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

    def _load_table_specs(self, connection: Any) -> list[OracleTableSpec]:
        include = self.config.include or {str(self.config.connection["username"]).upper(): []}
        watermark_column = str(self.config.options.get("watermark_column", "updated_at"))
        commit_timestamp_column = str(self.config.options.get("commit_timestamp_column", watermark_column))
        specs: list[OracleTableSpec] = []
        with connection.cursor() as cursor:
            for schema_name, tables in include.items():
                target_schema = (schema_name or str(self.config.connection["username"])).upper()
                if not tables:
                    raise ValueError(f"{self.source_type} include must list explicit tables for the current baseline")
                for table_name in tables:
                    target_table = str(table_name).upper()
                    cursor.execute(
                        """
                        select
                            c.column_name as column_name,
                            c.data_type as data_type,
                            c.column_id as ordinal_position,
                            case when pk.column_name is not null then 1 else 0 end as is_primary_key
                        from all_tab_columns c
                        left join (
                            select acc.owner, acc.table_name, acc.column_name
                            from all_constraints ac
                            join all_cons_columns acc
                              on ac.owner = acc.owner
                             and ac.constraint_name = acc.constraint_name
                             and ac.table_name = acc.table_name
                            where ac.constraint_type = 'P'
                        ) pk
                          on pk.owner = c.owner
                         and pk.table_name = c.table_name
                         and pk.column_name = c.column_name
                        where c.owner = :owner and c.table_name = :table_name
                        order by c.column_id
                        """,
                        owner=target_schema,
                        table_name=target_table,
                    )
                    rows = self._fetch_dict_rows(cursor)
                    if not rows:
                        raise ValueError(f"{self.source_type} table {target_schema}.{target_table} was not found")
                    columns = [str(self._row_get(row, "column_name")) for row in rows]
                    column_lookup = {column.lower(): column for column in columns}
                    actual_watermark_column = column_lookup.get(watermark_column.lower())
                    if actual_watermark_column is None:
                        raise ValueError(
                            f"{self.source_type} table {target_schema}.{target_table} does not contain watermark column {watermark_column}"
                        )
                    actual_commit_timestamp_column = column_lookup.get(commit_timestamp_column.lower())
                    if actual_commit_timestamp_column is None:
                        raise ValueError(
                            f"{self.source_type} table {target_schema}.{target_table} does not contain commit timestamp column {commit_timestamp_column}"
                        )
                    primary_key_columns = [
                        str(self._row_get(row, "column_name"))
                        for row in rows
                        if self._row_get(row, "is_primary_key")
                    ]
                    if not primary_key_columns:
                        raise ValueError(f"{self.source_type} table {target_schema}.{target_table} must have a primary key")
                    specs.append(
                        OracleTableSpec(
                            schema_name=target_schema,
                            table_name=target_table,
                            watermark_column=actual_watermark_column,
                            commit_timestamp_column=actual_commit_timestamp_column,
                            primary_key_columns=primary_key_columns,
                            selected_columns=columns,
                        )
                    )
        return specs

    def _fetch_rows(self, connection: Any, spec: OracleTableSpec, table_state: dict[str, Any] | None) -> list[dict[str, Any]]:
        select_columns = ", ".join(self._quote_identifier(column) for column in spec.selected_columns)
        order_columns = [spec.watermark_column, *spec.primary_key_columns]
        order_by = ", ".join(self._quote_identifier(column) for column in order_columns)
        sql = (
            f"select {select_columns} "
            f"from {self._qualified_table(spec.schema_name, spec.table_name)} "
            f"where {self._quote_identifier(spec.watermark_column)} is not null"
        )
        binds: dict[str, Any] = {}
        if table_state:
            pk_values = list(table_state.get("pk") or [])
            if len(pk_values) != len(spec.primary_key_columns):
                raise ValueError(f"{self.source_type} checkpoint for {spec.key} has invalid primary key state")
            watermark_value = self._watermark_bind_value(table_state.get("watermark"))
            binds["watermark_gt"] = watermark_value
            binds["watermark_eq"] = watermark_value
            predicates: list[str] = []
            for index, column in enumerate(spec.primary_key_columns):
                predicate_parts: list[str] = []
                for previous_index, previous in enumerate(spec.primary_key_columns[:index]):
                    bind_name = f"pk_eq_{index}_{previous_index}"
                    binds[bind_name] = pk_values[previous_index]
                    predicate_parts.append(f"{self._quote_identifier(previous)} = :{bind_name}")
                bind_name = f"pk_gt_{index}"
                binds[bind_name] = pk_values[index]
                predicate_parts.append(f"{self._quote_identifier(column)} > :{bind_name}")
                predicates.append("(" + " and ".join(predicate_parts) + ")")
            sql += (
                f" and ("
                f"{self._quote_identifier(spec.watermark_column)} > :watermark_gt "
                f"or ("
                f"{self._quote_identifier(spec.watermark_column)} = :watermark_eq and ("
                + " or ".join(predicates)
                + ")))"
            )
        sql += f" order by {order_by}"
        row_limit = int(self.config.options.get("row_limit", self.config.batch_size))
        if row_limit > 0:
            sql += f" fetch first {row_limit} rows only"
        with connection.cursor() as cursor:
            cursor.execute(sql, binds)
            return self._fetch_dict_rows(cursor)

    def _watermark_bind_value(self, value: Any) -> Any:
        if isinstance(value, str) and value.endswith("Z"):
            return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
        return value

    def _fetch_dict_rows(self, cursor: Any) -> list[dict[str, Any]]:
        rows = list(cursor.fetchall())
        if not rows:
            return []
        if isinstance(rows[0], dict):
            return rows
        description = getattr(cursor, "description", None) or []
        column_names = [str(column[0]) for column in description]
        return [dict(zip(column_names, row)) for row in rows]

    def _normalize_document(self, payload: dict[str, Any] | None) -> dict[str, Any] | None:
        if payload is None:
            return None
        return {str(key): self._normalize_value(value) for key, value in payload.items()}

    def _parse_insert_values(self, sql_redo: str) -> dict[str, Any]:
        sql = sql_redo.strip().rstrip(";")
        values_index = sql.lower().find(" values ")
        if values_index == -1:
            return {}
        columns_start = sql.find("(", sql.lower().find("into "))
        columns_sql, _ = self._extract_parenthesized(sql, columns_start)
        values_sql, _ = self._extract_parenthesized(sql, sql.find("(", values_index))
        columns = [self._parse_identifier(item) for item in self._split_top_level(columns_sql, ",")]
        values = [self._parse_sql_value(item) for item in self._split_top_level(values_sql, ",")]
        return {column: value for column, value in zip(columns, values)}

    def _parse_delete_values(self, sql_redo: str) -> dict[str, Any]:
        where_index = sql_redo.lower().find(" where ")
        if where_index == -1:
            return {}
        return self._parse_conditions(sql_redo[where_index + 7 :])

    def _parse_update_after(self, sql_redo: str, sql_undo: str) -> dict[str, Any]:
        values = self._parse_update_set_values(sql_redo)
        values.update(self._parse_conditions(self._extract_where_clause(sql_undo)))
        values.pop("ROWID", None)
        return values

    def _parse_update_before(self, sql_redo: str, sql_undo: str) -> dict[str, Any]:
        values = self._parse_conditions(self._extract_where_clause(sql_redo))
        undo_values = self._parse_update_set_values(sql_undo)
        values.update(undo_values)
        values.pop("ROWID", None)
        return values

    def _parse_update_set_values(self, sql_text: str) -> dict[str, Any]:
        lower_sql = sql_text.lower()
        set_index = lower_sql.find(" set ")
        where_index = lower_sql.find(" where ")
        if set_index == -1 or where_index == -1:
            return {}
        assignments = sql_text[set_index + 5 : where_index]
        values: dict[str, Any] = {}
        for item in self._split_top_level(assignments, ","):
            if "=" not in item:
                continue
            column, value = item.split("=", 1)
            values[self._parse_identifier(column)] = self._parse_sql_value(value)
        return values

    def _extract_where_clause(self, sql_text: str) -> str:
        where_index = sql_text.lower().find(" where ")
        if where_index == -1:
            return ""
        return sql_text[where_index + 7 :].strip().rstrip(";")

    def _parse_conditions(self, expression: str) -> dict[str, Any]:
        values: dict[str, Any] = {}
        for item in self._split_top_level(expression, " and "):
            if "=" not in item:
                continue
            column, value = item.split("=", 1)
            identifier = self._parse_identifier(column)
            if identifier.upper() == "ROWID":
                continue
            values[identifier] = self._parse_sql_value(value)
        return values

    def _extract_parenthesized(self, text: str, start_index: int) -> tuple[str, int]:
        if start_index < 0 or start_index >= len(text) or text[start_index] != "(":
            raise ValueError("expected parenthesized SQL segment")
        depth = 0
        in_string = False
        start = start_index + 1
        for index in range(start_index, len(text)):
            char = text[index]
            if char == "'" and (index == 0 or text[index - 1] != "\\"):
                if in_string and index + 1 < len(text) and text[index + 1] == "'":
                    continue
                in_string = not in_string
            if in_string:
                continue
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
                if depth == 0:
                    return text[start:index], index
        raise ValueError("unterminated parenthesized SQL segment")

    def _split_top_level(self, text: str, delimiter: str) -> list[str]:
        parts: list[str] = []
        current: list[str] = []
        depth = 0
        in_string = False
        index = 0
        lower_text = text.lower()
        lower_delimiter = delimiter.lower()
        while index < len(text):
            char = text[index]
            if char == "'":
                current.append(char)
                if in_string and index + 1 < len(text) and text[index + 1] == "'":
                    current.append(text[index + 1])
                    index += 2
                    continue
                in_string = not in_string
                index += 1
                continue
            if not in_string:
                if char == "(":
                    depth += 1
                elif char == ")":
                    depth -= 1
                if depth == 0 and lower_text.startswith(lower_delimiter, index):
                    part = "".join(current).strip()
                    if part:
                        parts.append(part)
                    current = []
                    index += len(delimiter)
                    continue
            current.append(char)
            index += 1
        tail = "".join(current).strip()
        if tail:
            parts.append(tail)
        return parts

    def _parse_identifier(self, value: str) -> str:
        cleaned = value.strip()
        if cleaned.startswith('"') and cleaned.endswith('"'):
            return cleaned[1:-1].replace('""', '"')
        return cleaned

    def _parse_sql_value(self, value: str) -> Any:
        cleaned = value.strip().rstrip(";").strip()
        if cleaned.upper() == "NULL":
            return None
        if cleaned.upper().startswith("TO_TIMESTAMP("):
            match = re.search(r"TO_TIMESTAMP\('((?:''|[^'])*)'", cleaned, re.IGNORECASE)
            if not match:
                return cleaned
            timestamp_text = match.group(1).replace("''", "'")
            parsed: datetime | None = None
            for format_string in ("%d-%b-%y %I.%M.%S.%f %p", "%d-%b-%y %I.%M.%S %p"):
                try:
                    parsed = datetime.strptime(timestamp_text, format_string)
                    break
                except ValueError:
                    continue
            if parsed is None:
                return timestamp_text
            return parsed.replace(tzinfo=timezone.utc)
        if cleaned.startswith("'") and cleaned.endswith("'"):
            return cleaned[1:-1].replace("''", "'")
        return cleaned

    def _parse_checkpoint(self, checkpoint: SourceCheckpoint | None) -> dict[str, dict[str, Any]]:
        if checkpoint is None or not checkpoint.token:
            return {}
        payload = json.loads(checkpoint.token)
        if not isinstance(payload, dict):
            raise ValueError(f"{self.source_type} checkpoint token must be a JSON object")
        return payload

    def _sort_key(self, spec: OracleTableSpec, row: dict[str, Any]) -> tuple[Any, ...]:
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

    def _qualified_table(self, schema_name: str, table_name: str) -> str:
        return f"{self._quote_identifier(schema_name)}.{self._quote_identifier(table_name)}"

    def _quote_identifier(self, value: str) -> str:
        return f'"{value.replace(chr(34), chr(34) * 2)}"'

    def _spec_summary(self, spec: OracleTableSpec) -> dict[str, Any]:
        return {
            "schema_name": spec.schema_name,
            "table_name": spec.table_name,
            "watermark_column": spec.watermark_column,
            "commit_timestamp_column": spec.commit_timestamp_column,
            "primary_key_columns": list(spec.primary_key_columns),
            "selected_columns": list(spec.selected_columns),
        }

    def _spec_signature_entry(self, spec: OracleTableSpec) -> dict[str, Any]:
        return {
            "key": spec.key,
            "watermark_column": spec.watermark_column,
            "commit_timestamp_column": spec.commit_timestamp_column,
            "primary_key_columns": list(spec.primary_key_columns),
            "selected_columns": list(spec.selected_columns),
        }
