from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from denotary_db_agent.adapters.oracle import OracleAdapter
from denotary_db_agent.config import SourceConfig
from denotary_db_agent.models import SourceCheckpoint


class FakeCursor:
    def __init__(self, results: list[list[dict[str, object] | tuple[object, ...]]], description: list[tuple[str]] | None = None):
        self._results = results
        self.executed: list[tuple[str, dict[str, object] | None]] = []
        self._current: list[dict[str, object] | tuple[object, ...]] = []
        self.description = description or []

    def execute(self, sql: str, params: dict[str, object] | None = None, **kwargs: object) -> None:
        normalized = dict(params or {})
        if kwargs:
            normalized.update(kwargs)
        self.executed.append((sql, normalized or None))
        self._current = self._results.pop(0)

    def fetchone(self):
        return self._current[0] if self._current else None

    def fetchall(self):
        return list(self._current)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class FakeConnection:
    def __init__(self, results: list[list[dict[str, object] | tuple[object, ...]]], description: list[tuple[str]] | None = None):
        self.cursor_instance = FakeCursor(results, description=description)
        self.closed = False

    def cursor(self):
        return self.cursor_instance

    def close(self) -> None:
        self.closed = True


class OracleAdapterTest(unittest.TestCase):
    def setUp(self) -> None:
        self.config = SourceConfig(
            id="oracle-core-ledger",
            adapter="oracle",
            enabled=True,
            source_instance="erp-eu-1",
            database_name="ledger",
            backfill_mode="full",
            include={"LEDGER": ["INVOICES"]},
            connection={
                "host": "127.0.0.1",
                "port": 1521,
                "username": "denotary",
                "service_name": "XEPDB1",
            },
            options={
                "capture_mode": "watermark",
                "watermark_column": "updated_at",
                "commit_timestamp_column": "updated_at",
                "row_limit": 100,
            },
        )

    def test_discover_capabilities_describes_watermark_baseline(self) -> None:
        adapter = OracleAdapter(self.config)
        capabilities = adapter.discover_capabilities()

        self.assertEqual(capabilities.source_type, "oracle")
        self.assertEqual(capabilities.minimum_version, "19c")
        self.assertTrue(capabilities.supports_cdc)
        self.assertTrue(capabilities.supports_snapshot)
        self.assertEqual(capabilities.operations, ("snapshot",))
        self.assertEqual(capabilities.capture_modes, ("watermark", "logminer"))
        self.assertEqual(capabilities.cdc_modes, ("logminer",))
        self.assertEqual(
            capabilities.bootstrap_requirements,
            ("tracked tables visible", "watermark columns configured"),
        )
        self.assertIn("LogMiner", capabilities.notes)

    def test_validate_connection_rejects_missing_fields(self) -> None:
        config = SourceConfig(
            id="oracle-core-ledger",
            adapter="oracle",
            enabled=True,
            source_instance="erp-eu-1",
            database_name="ledger",
            connection={},
        )
        adapter = OracleAdapter(config)

        with self.assertRaisesRegex(
            ValueError,
            "oracle connection is missing required fields: host, port, username, service_name",
        ):
            adapter.validate_connection()

    def test_bootstrap_and_inspect_report_tracked_tables(self) -> None:
        adapter = OracleAdapter(self.config)
        connection = FakeConnection(
            [
                [{"OK": 1}],
                [
                    {
                        "COLUMN_NAME": "ID",
                        "DATA_TYPE": "NUMBER",
                        "ORDINAL_POSITION": 1,
                        "IS_PRIMARY_KEY": 1,
                    },
                    {
                        "COLUMN_NAME": "STATUS",
                        "DATA_TYPE": "VARCHAR2",
                        "ORDINAL_POSITION": 2,
                        "IS_PRIMARY_KEY": 0,
                    },
                    {
                        "COLUMN_NAME": "UPDATED_AT",
                        "DATA_TYPE": "TIMESTAMP",
                        "ORDINAL_POSITION": 3,
                        "IS_PRIMARY_KEY": 0,
                    },
                ],
                [
                    {
                        "COLUMN_NAME": "ID",
                        "DATA_TYPE": "NUMBER",
                        "ORDINAL_POSITION": 1,
                        "IS_PRIMARY_KEY": 1,
                    },
                    {
                        "COLUMN_NAME": "STATUS",
                        "DATA_TYPE": "VARCHAR2",
                        "ORDINAL_POSITION": 2,
                        "IS_PRIMARY_KEY": 0,
                    },
                    {
                        "COLUMN_NAME": "UPDATED_AT",
                        "DATA_TYPE": "TIMESTAMP",
                        "ORDINAL_POSITION": 3,
                        "IS_PRIMARY_KEY": 0,
                    },
                ],
            ]
        )

        with patch("denotary_db_agent.adapters.oracle.oracledb", object()), patch.object(adapter, "_connect") as connect:
            connect.return_value.__enter__.return_value = connection
            connect.return_value.__exit__.return_value = False
            bootstrap = adapter.bootstrap()

        self.assertEqual(bootstrap["capture_mode"], "watermark")
        self.assertEqual(len(bootstrap["tracked_tables"]), 1)
        self.assertEqual(bootstrap["tracked_tables"][0]["table_name"], "INVOICES")

        connection = FakeConnection(
            [
                [
                    {
                        "COLUMN_NAME": "ID",
                        "DATA_TYPE": "NUMBER",
                        "ORDINAL_POSITION": 1,
                        "IS_PRIMARY_KEY": 1,
                    },
                    {
                        "COLUMN_NAME": "STATUS",
                        "DATA_TYPE": "VARCHAR2",
                        "ORDINAL_POSITION": 2,
                        "IS_PRIMARY_KEY": 0,
                    },
                    {
                        "COLUMN_NAME": "UPDATED_AT",
                        "DATA_TYPE": "TIMESTAMP",
                        "ORDINAL_POSITION": 3,
                        "IS_PRIMARY_KEY": 0,
                    },
                ]
            ]
        )
        with patch.object(adapter, "_connect") as connect:
            connect.return_value.__enter__.return_value = connection
            connect.return_value.__exit__.return_value = False
            inspect = adapter.inspect()

        self.assertEqual(inspect["capture_modes"], ["watermark", "logminer"])
        self.assertEqual(inspect["bootstrap_requirements"], ["tracked tables visible", "watermark columns configured"])
        self.assertEqual(inspect["tracked_tables"][0]["primary_key_columns"], ["ID"])

    def test_read_snapshot_emits_rows_and_checkpoint_state(self) -> None:
        adapter = OracleAdapter(self.config)
        spec_rows = [
            {
                "COLUMN_NAME": "ID",
                "DATA_TYPE": "NUMBER",
                "ORDINAL_POSITION": 1,
                "IS_PRIMARY_KEY": 1,
            },
            {
                "COLUMN_NAME": "STATUS",
                "DATA_TYPE": "VARCHAR2",
                "ORDINAL_POSITION": 2,
                "IS_PRIMARY_KEY": 0,
            },
            {
                "COLUMN_NAME": "UPDATED_AT",
                "DATA_TYPE": "TIMESTAMP",
                "ORDINAL_POSITION": 3,
                "IS_PRIMARY_KEY": 0,
            },
        ]
        data_rows = [
            {
                "ID": 1,
                "STATUS": "issued",
                "UPDATED_AT": "2026-04-18T10:00:00Z",
            },
            {
                "ID": 2,
                "STATUS": "paid",
                "UPDATED_AT": "2026-04-18T10:05:00Z",
            },
        ]
        connection = FakeConnection([spec_rows, data_rows])

        with patch.object(adapter, "_connect") as connect:
            connect.return_value.__enter__.return_value = connection
            connect.return_value.__exit__.return_value = False
            events = list(adapter.read_snapshot())

        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].operation, "snapshot")
        self.assertEqual(events[0].primary_key, {"ID": 1})
        self.assertEqual(events[0].after["STATUS"], "issued")
        self.assertIn('"LEDGER.INVOICES"', events[-1].checkpoint_token)

    def test_read_snapshot_respects_checkpoint_resume(self) -> None:
        adapter = OracleAdapter(self.config)
        checkpoint = SourceCheckpoint(
            source_id=self.config.id,
            token='{"LEDGER.INVOICES":{"watermark":"2026-04-18T10:00:00Z","pk":[1]}}',
            updated_at="2026-04-18T10:01:00Z",
        )
        spec_rows = [
            {
                "COLUMN_NAME": "ID",
                "DATA_TYPE": "NUMBER",
                "ORDINAL_POSITION": 1,
                "IS_PRIMARY_KEY": 1,
            },
            {
                "COLUMN_NAME": "STATUS",
                "DATA_TYPE": "VARCHAR2",
                "ORDINAL_POSITION": 2,
                "IS_PRIMARY_KEY": 0,
            },
            {
                "COLUMN_NAME": "UPDATED_AT",
                "DATA_TYPE": "TIMESTAMP",
                "ORDINAL_POSITION": 3,
                "IS_PRIMARY_KEY": 0,
            },
        ]
        data_rows = [
            {
                "ID": 2,
                "STATUS": "paid",
                "UPDATED_AT": "2026-04-18T10:05:00Z",
            }
        ]
        connection = FakeConnection([spec_rows, data_rows])

        with patch.object(adapter, "_connect") as connect:
            connect.return_value.__enter__.return_value = connection
            connect.return_value.__exit__.return_value = False
            events = list(adapter.read_snapshot(checkpoint))

        self.assertEqual(len(events), 1)
        executed_sql, executed_params = connection.cursor_instance.executed[-1]
        self.assertIn('"UPDATED_AT" > :watermark_gt', executed_sql)
        self.assertIn("fetch first 100 rows only", executed_sql.lower())
        self.assertIsNotNone(executed_params)
        self.assertEqual(str(executed_params["watermark_gt"]), "2026-04-18 10:00:00")

    def test_start_stream_raises_until_cdc_is_added(self) -> None:
        adapter = OracleAdapter(self.config)
        with self.assertRaisesRegex(
            NotImplementedError,
            "oracle CDC streaming is only available when capture_mode is set to logminer",
        ):
            list(adapter.start_stream(None))

    def test_logminer_bootstrap_and_inspect_report_admin_prereqs(self) -> None:
        config = SourceConfig(
            **{
                **self.config.__dict__,
                "connection": {
                    **self.config.connection,
                    "admin_username": "system",
                    "admin_password": "oraclepw",
                    "admin_service_name": "FREE",
                },
                "options": {
                    **self.config.options,
                    "capture_mode": "logminer",
                },
            }
        )
        adapter = OracleAdapter(config)
        summary = {
            "capture_mode": "logminer",
            "admin_container_name": "CDB$ROOT",
            "log_mode": "NOARCHIVELOG",
            "open_mode": "READ WRITE",
            "supplemental_log_data_min": "YES",
            "supplemental_log_data_pk": "NO",
            "current_root_scn": 424242,
            "runtime": {
                "transport": "polling",
                "cursor": {
                    "baseline_scn": None,
                    "current_root_scn": 424242,
                },
            },
            "redo_members": [{"member": "/redo01.log", "status": "", "sequence_no": 11}],
            "logminer_packages": ["DBMS_LOGMNR", "DBMS_LOGMNR_D"],
        }
        with patch.object(adapter, "validate_connection"), patch.object(adapter, "_connect") as connect, patch.object(
            adapter, "_load_table_specs"
        ) as load_specs, patch.object(adapter, "_cdc_summary", return_value=summary):
            connect.return_value.__enter__.return_value = FakeConnection([])
            connect.return_value.__exit__.return_value = False
            load_specs.return_value = [
                type(
                    "Spec",
                    (),
                    {
                        "schema_name": "DENOTARY",
                        "table_name": "INVOICES",
                        "watermark_column": "UPDATED_AT",
                        "commit_timestamp_column": "UPDATED_AT",
                        "primary_key_columns": ["ID"],
                        "selected_columns": ["ID", "STATUS", "UPDATED_AT"],
                    },
                )()
            ]
            bootstrap = adapter.bootstrap()
            inspect = adapter.inspect()

        self.assertEqual(bootstrap["capture_mode"], "logminer")
        self.assertEqual(bootstrap["cdc"]["admin_container_name"], "CDB$ROOT")
        self.assertEqual(bootstrap["cdc"]["runtime"]["transport"], "polling")
        self.assertEqual(bootstrap["cdc"]["runtime"]["cursor"]["current_root_scn"], bootstrap["cdc"]["current_root_scn"])
        self.assertEqual(inspect["cdc_modes"], ["logminer"])
        self.assertTrue(inspect["supports_cdc"])

    def test_validate_logminer_prerequisites_requires_root_and_supplemental_logging(self) -> None:
        config = SourceConfig(
            **{
                **self.config.__dict__,
                "connection": {
                    **self.config.connection,
                    "admin_username": "system",
                    "admin_password": "oraclepw",
                    "admin_service_name": "FREE",
                },
                "options": {
                    **self.config.options,
                    "capture_mode": "logminer",
                },
            }
        )
        adapter = OracleAdapter(config)
        with patch.object(
            adapter,
            "_read_logminer_summary",
            return_value={
                "capture_mode": "logminer",
                "admin_container_name": "FREEPDB1",
                "log_mode": "NOARCHIVELOG",
                "open_mode": "READ WRITE",
                "supplemental_log_data_min": "NO",
                "supplemental_log_data_pk": "NO",
                "redo_members": [],
                "logminer_packages": ["DBMS_LOGMNR"],
            },
        ):
            with self.assertRaisesRegex(ValueError, "logminer admin connection must target CDB\\$ROOT"):
                adapter._validate_logminer_prerequisites(object())

    def test_start_stream_logminer_without_checkpoint_sets_initial_scn_baseline(self) -> None:
        config = SourceConfig(
            **{
                **self.config.__dict__,
                "connection": {
                    **self.config.connection,
                    "admin_username": "system",
                    "admin_password": "oraclepw",
                    "admin_service_name": "FREE",
                },
                "options": {
                    **self.config.options,
                    "capture_mode": "logminer",
                },
            }
        )
        adapter = OracleAdapter(config)
        with patch.object(adapter, "_connect") as connect, patch.object(adapter, "_connect_logminer_admin") as admin_connect, patch.object(
            adapter, "_load_table_specs"
        ) as load_specs, patch.object(adapter, "_validate_logminer_prerequisites"), patch.object(
            adapter, "_read_source_container_name", return_value="FREEPDB1"
        ), patch.object(adapter, "_current_root_scn", return_value=424242):
            connect.return_value.__enter__.return_value = FakeConnection([])
            connect.return_value.__exit__.return_value = False
            admin_connect.return_value.__enter__.return_value = FakeConnection([])
            admin_connect.return_value.__exit__.return_value = False
            load_specs.return_value = []
            events = list(adapter.start_stream(None))

        self.assertEqual(events, [])
        self.assertEqual(adapter._logminer_initial_scn, 424242)

    def test_start_stream_logminer_emits_insert_update_delete_events(self) -> None:
        config = SourceConfig(
            **{
                **self.config.__dict__,
                "connection": {
                    **self.config.connection,
                    "admin_username": "system",
                    "admin_password": "oraclepw",
                    "admin_service_name": "FREE",
                },
                "options": {
                    **self.config.options,
                    "capture_mode": "logminer",
                },
            }
        )
        adapter = OracleAdapter(config)
        checkpoint = SourceCheckpoint(
            source_id=config.id,
            token=json.dumps({"mode": "logminer", "scn": 1000, "rs_id": "", "ssn": 0, "row_index": 0}),
            updated_at="2026-04-18T12:00:00Z",
        )
        spec = type(
            "Spec",
            (),
            {
                "schema_name": "DENOTARY",
                "table_name": "INVOICES",
                "primary_key_columns": ["ID"],
            },
        )()
        rows = [
            {
                "SCN": 1001,
                "RS_ID": "0x001",
                "SSN": 1,
                "TIMESTAMP": "2026-04-18T12:00:01Z",
                "OPERATION": "INSERT",
                "SEG_OWNER": "DENOTARY",
                "TABLE_NAME": "INVOICES",
                "SQL_REDO": "insert into \"DENOTARY\".\"INVOICES\"(\"ID\",\"STATUS\") values ('1','issued');",
                "SQL_UNDO": "",
                "SRC_CON_NAME": "FREEPDB1",
            },
            {
                "SCN": 1002,
                "RS_ID": "0x002",
                "SSN": 1,
                "TIMESTAMP": "2026-04-18T12:00:02Z",
                "OPERATION": "UPDATE",
                "SEG_OWNER": "DENOTARY",
                "TABLE_NAME": "INVOICES",
                "SQL_REDO": "update \"DENOTARY\".\"INVOICES\" set \"STATUS\" = 'paid' where \"ID\" = '1' and \"STATUS\" = 'issued';",
                "SQL_UNDO": "update \"DENOTARY\".\"INVOICES\" set \"STATUS\" = 'issued' where \"ID\" = '1' and \"STATUS\" = 'paid';",
                "SRC_CON_NAME": "FREEPDB1",
            },
            {
                "SCN": 1003,
                "RS_ID": "0x003",
                "SSN": 1,
                "TIMESTAMP": "2026-04-18T12:00:03Z",
                "OPERATION": "DELETE",
                "SEG_OWNER": "DENOTARY",
                "TABLE_NAME": "INVOICES",
                "SQL_REDO": "delete from \"DENOTARY\".\"INVOICES\" where \"ID\" = '1' and \"STATUS\" = 'paid';",
                "SQL_UNDO": "",
                "SRC_CON_NAME": "FREEPDB1",
            },
        ]
        with patch.object(adapter, "_connect") as connect, patch.object(adapter, "_connect_logminer_admin") as admin_connect, patch.object(
            adapter, "_load_table_specs", return_value=[spec]
        ), patch.object(adapter, "_read_source_container_name", return_value="FREEPDB1"), patch.object(
            adapter, "_validate_logminer_prerequisites"
        ), patch.object(adapter, "_start_logminer_session"), patch.object(adapter, "_end_logminer_session"), patch.object(
            adapter, "_fetch_logminer_rows", return_value=rows
        ):
            connect.return_value.__enter__.return_value = FakeConnection([])
            connect.return_value.__exit__.return_value = False
            admin_connect.return_value.__enter__.return_value = FakeConnection([])
            admin_connect.return_value.__exit__.return_value = False
            events = list(adapter.start_stream(checkpoint))

        self.assertEqual([event.operation for event in events], ["insert", "update", "delete"])
        self.assertEqual(events[0].after, {"ID": "1", "STATUS": "issued"})
        self.assertEqual(events[1].before, {"ID": "1", "STATUS": "issued"})
        self.assertEqual(events[1].after, {"STATUS": "paid", "ID": "1"})
        self.assertEqual(events[2].before, {"ID": "1", "STATUS": "paid"})
        self.assertEqual(events[2].primary_key, {"ID": "1"})

    def test_start_stream_logminer_raises_clear_runtime_message_when_redo_window_is_gone(self) -> None:
        config = SourceConfig(
            **{
                **self.config.__dict__,
                "connection": {
                    **self.config.connection,
                    "admin_username": "system",
                    "admin_password": "oraclepw",
                    "admin_service_name": "FREE",
                },
                "options": {
                    **self.config.options,
                    "capture_mode": "logminer",
                },
            }
        )
        adapter = OracleAdapter(config)
        checkpoint = SourceCheckpoint(
            source_id=config.id,
            token=json.dumps({"mode": "logminer", "scn": 999, "rs_id": "", "ssn": 0, "row_index": 0}),
            updated_at="2026-04-18T12:00:00Z",
        )
        with patch.object(adapter, "_connect") as connect, patch.object(adapter, "_connect_logminer_admin") as admin_connect, patch.object(
            adapter, "_load_table_specs", return_value=[]
        ), patch.object(adapter, "_read_source_container_name", return_value="FREEPDB1"), patch.object(
            adapter, "_validate_logminer_prerequisites"
        ), patch.object(
            adapter, "_start_logminer_session", side_effect=ValueError("oracle LogMiner checkpoint is older than the current online redo window")
        ):
            connect.return_value.__enter__.return_value = FakeConnection([])
            connect.return_value.__exit__.return_value = False
            admin_connect.return_value.__enter__.return_value = FakeConnection([])
            admin_connect.return_value.__exit__.return_value = False
            with self.assertRaisesRegex(ValueError, "older than the current online redo window"):
                list(adapter.start_stream(checkpoint))
