from __future__ import annotations

import json
import os
import socket
import tempfile
import threading
import unittest
import urllib.parse
from types import SimpleNamespace
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from unittest.mock import patch

from denotary_db_agent.config import load_config
from denotary_db_agent.engine import AgentEngine, SourceRuntime
from denotary_db_agent.models import ChangeEvent, DeliveryAttempt, ProofArtifact


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as handle:
        handle.bind(("127.0.0.1", 0))
        return int(handle.getsockname()[1])


class MockIngressHandler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802
        body = self.rfile.read(int(self.headers.get("Content-Length", "0"))).decode("utf-8")
        payload = json.loads(body)
        response = {
            "request_id": "request-1",
            "trace_id": "trace-1",
            "external_ref_hash": "1" * 64,
            "object_hash": "2" * 64,
            "verification_account": "verif",
            "prepared_action": {
                "contract": "verifbill",
                "action": "submit",
                "data": {
                    "payer": payload["submitter"],
                    "submitter": payload["submitter"],
                    "schema_id": 1,
                    "policy_id": 1,
                    "object_hash": "2" * 64,
                    "external_ref": "1" * 64,
                },
            },
        }
        body_out = json.dumps(response).encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body_out)))
        self.end_headers()
        self.wfile.write(body_out)

    def log_message(self, format: str, *args: Any) -> None:
        return


class MockWatcherHandler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802
        if self.path != "/v1/watch/register":
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        body = self.rfile.read(int(self.headers.get("Content-Length", "0"))).decode("utf-8")
        payload = json.loads(body)
        response = {"ok": True, "request_id": payload["request_id"]}
        body_out = json.dumps(response).encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body_out)))
        self.end_headers()
        self.wfile.write(body_out)

    def log_message(self, format: str, *args: Any) -> None:
        return


class EngineTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        ingress_port = free_port()
        watcher_port = free_port()
        self.ingress_server = ThreadingHTTPServer(("127.0.0.1", ingress_port), MockIngressHandler)
        self.watcher_server = ThreadingHTTPServer(("127.0.0.1", watcher_port), MockWatcherHandler)
        self.threads = [
            threading.Thread(target=self.ingress_server.serve_forever, daemon=True),
            threading.Thread(target=self.watcher_server.serve_forever, daemon=True),
        ]
        for thread in self.threads:
            thread.start()

        self.config_path = Path(self.temp_dir.name) / "config.json"
        config = {
            "agent_name": "test-agent",
            "log_level": "INFO",
            "denotary": {
                "ingress_url": f"http://127.0.0.1:{ingress_port}",
                "watcher_url": f"http://127.0.0.1:{watcher_port}",
                "watcher_auth_token": "token",
                "submitter": "enterpriseac1",
                "schema_id": 1,
                "policy_id": 1
            },
            "storage": {
                "state_db": str(Path(self.temp_dir.name) / "state.sqlite3"),
                "proof_dir": str(Path(self.temp_dir.name) / "proofs"),
            },
            "sources": [
                {
                    "id": "pg-core-ledger",
                    "adapter": "postgresql",
                    "enabled": True,
                    "source_instance": "erp-eu-1",
                    "database_name": "ledger",
                    "connection": {
                        "host": "127.0.0.1",
                        "port": 5432,
                        "username": "denotary",
                        "database": "ledger"
                    },
                    "options": {
                        "dry_run_events": [
                            {
                                "schema_or_namespace": "public",
                                "table_or_collection": "invoices",
                                "operation": "update",
                                "primary_key": {"id": 1},
                                "change_version": "lsn:1",
                                "checkpoint_token": "lsn:1",
                                "commit_timestamp": "2026-04-17T10:11:12Z",
                                "after": {"id": 1, "status": "issued"},
                                "before": {"id": 1, "status": "draft"},
                                "metadata": {"txid": 100}
                            }
                        ]
                    }
                }
            ]
        }
        self.config_path.write_text(json.dumps(config), encoding="utf-8")

    def tearDown(self) -> None:
        self.ingress_server.shutdown()
        self.watcher_server.shutdown()
        self.ingress_server.server_close()
        self.watcher_server.server_close()
        for thread in self.threads:
            thread.join(timeout=2)
        self.temp_dir.cleanup()

    def test_run_once_processes_event_and_updates_checkpoint(self) -> None:
        engine = AgentEngine(load_config(self.config_path))
        result = engine.run_once()
        self.assertEqual(result["processed"], 1)
        self.assertEqual(result["failed"], 0)
        checkpoints = engine.checkpoint_summary()
        self.assertEqual(len(checkpoints), 1)
        self.assertEqual(checkpoints[0]["token"], "lsn:1")
        deliveries = engine.store.list_deliveries("pg-core-ledger")
        self.assertEqual(len(deliveries), 1)
        self.assertEqual(deliveries[0]["status"], "prepared_registered")

    def test_run_forever_stops_after_max_loops(self) -> None:
        engine = AgentEngine(load_config(self.config_path))
        result = engine.run_forever(interval_sec=0.01, max_loops=2)
        self.assertEqual(result["processed"], 2)
        self.assertEqual(result["failed"], 0)
        self.assertEqual(result["loops"], 2)

    def test_pause_and_resume_source_controls_processing(self) -> None:
        engine = AgentEngine(load_config(self.config_path))
        engine.pause_source("pg-core-ledger")
        paused_status = engine.status()
        self.assertTrue(paused_status["sources"][0]["paused"])

        paused_result = engine.run_once()
        self.assertEqual(paused_result["processed"], 0)
        self.assertEqual(paused_result["failed"], 0)

        engine.resume_source("pg-core-ledger")
        resumed_status = engine.health()
        self.assertFalse(resumed_status["sources"][0]["paused"])

        resumed_result = engine.run_once()
        self.assertEqual(resumed_result["processed"], 1)
        self.assertEqual(resumed_result["failed"], 0)

    def test_run_once_stores_runtime_signature(self) -> None:
        engine = AgentEngine(load_config(self.config_path))
        self.assertIsNone(engine.store.get_runtime_signature("pg-core-ledger"))
        result = engine.run_once()
        self.assertEqual(result["processed"], 1)
        self.assertIsNotNone(engine.store.get_runtime_signature("pg-core-ledger"))

    def test_run_once_uses_adapter_event_iteration_contract(self) -> None:
        engine = AgentEngine(load_config(self.config_path))
        source_config = engine.config.sources[0]
        source_config.options = {}
        captured = {"iter_calls": 0, "processed_events": []}
        event = ChangeEvent(
            source_id=source_config.id,
            source_type="mongodb",
            source_instance=source_config.source_instance,
            database_name=source_config.database_name,
            schema_or_namespace="ledger",
            table_or_collection="invoices",
            operation="insert",
            primary_key={"id": 1},
            change_version="change-1",
            commit_timestamp="2026-04-18T10:00:00Z",
            before=None,
            after={"id": 1, "status": "issued"},
            metadata={"capture_mode": "change_streams"},
            checkpoint_token="change-1",
        )
        fake_adapter = SimpleNamespace(
            runtime_signature=lambda: "runtime-signature",
            refresh_runtime=lambda: {"source_id": source_config.id},
            resume_from_checkpoint=lambda checkpoint: None,
            iter_events=lambda checkpoint: captured.__setitem__("iter_calls", captured["iter_calls"] + 1) or iter([event]),
            should_wait_for_activity=lambda: True,
            stop_stream=lambda: None,
        )
        runtime = SourceRuntime(config=source_config, adapter=fake_adapter)

        with patch.object(engine, "_ensure_runtime_ready", return_value=None), patch.object(
            engine,
            "_process_event",
            side_effect=lambda runtime_obj, current_event: captured["processed_events"].append(current_event.change_version),
        ):
            result = engine._run_runtimes_once([runtime])

        self.assertEqual(result["processed"], 1)
        self.assertEqual(result["failed"], 0)
        self.assertEqual(captured["iter_calls"], 1)
        self.assertEqual(captured["processed_events"], ["change-1"])

    def test_wait_for_activity_uses_adapter_wait_contract(self) -> None:
        engine = AgentEngine(load_config(self.config_path))
        source_config = engine.config.sources[0]
        source_config.options = {}
        wait_calls: list[float] = []
        fake_adapter = SimpleNamespace(
            should_wait_for_activity=lambda: True,
            wait_for_changes=lambda timeout_sec: wait_calls.append(timeout_sec) or True,
            stop_stream=lambda: None,
        )
        runtime = SourceRuntime(config=source_config, adapter=fake_adapter)

        engine._wait_for_activity([runtime], interval_sec=0.25)

        self.assertEqual(len(wait_calls), 1)
        self.assertGreater(wait_calls[0], 0.0)

    def test_process_event_reuses_prepared_request_across_retries(self) -> None:
        engine = AgentEngine(load_config(self.config_path))
        runtime = engine.runtimes()[0]
        engine.config.denotary.wait_for_finality = True
        event_payload = engine.config.sources[0].options["dry_run_events"][0]
        event = ChangeEvent(
            source_id=runtime.config.id,
            source_type="postgresql",
            source_instance=runtime.config.source_instance,
            database_name=runtime.config.database_name,
            **event_payload,
        )

        prepared = SimpleNamespace(
            request_id="request-retry-1",
            trace_id="trace-retry-1",
            prepared_action={"contract": "verifbill", "action": "submit", "data": {"submitter": "enterpriseac1"}},
        )
        prepare_calls: list[dict[str, object]] = []
        register_calls: list[tuple[str, str]] = []
        included_calls: list[tuple[str, str, int]] = []
        failed_calls: list[tuple[str, str]] = []

        def prepare_single(payload):
            prepare_calls.append(payload)
            return prepared

        class FlakyChain:
            def __init__(self) -> None:
                self.calls = 0

            def push_prepared_action(self, prepared_action):
                self.calls += 1
                if self.calls == 1:
                    raise RuntimeError("transient push failure")
                return SimpleNamespace(tx_id="a" * 64, block_num=321)

        chain = FlakyChain()
        engine.ingress = SimpleNamespace(prepare_single=prepare_single)
        engine.chain = chain
        engine.watcher = SimpleNamespace(
            register=lambda prepared_obj, envelope: register_calls.append((prepared_obj.request_id, prepared_obj.trace_id)),
            mark_failed=lambda request_id, reason, details=None: failed_calls.append((request_id, reason)),
            mark_included=lambda request_id, tx_id, block_num: included_calls.append((request_id, tx_id, block_num)),
        )

        engine._process_event(runtime, event)

        self.assertEqual(len(prepare_calls), 1)
        self.assertEqual(register_calls, [("request-retry-1", "trace-retry-1"), ("request-retry-1", "trace-retry-1")])
        self.assertEqual(failed_calls, [("request-retry-1", "db_agent_delivery_failed")])
        self.assertEqual(included_calls, [("request-retry-1", "a" * 64, 321)])
        checkpoints = engine.checkpoint_summary()
        self.assertEqual(len(checkpoints), 1)
        self.assertEqual(checkpoints[0]["token"], "lsn:1")

    def test_process_event_recovers_existing_prepared_request_after_duplicate_submit(self) -> None:
        engine = AgentEngine(load_config(self.config_path))
        runtime = engine.runtimes()[0]
        engine.config.denotary.wait_for_finality = True
        event_payload = engine.config.sources[0].options["dry_run_events"][0]
        event = ChangeEvent(
            source_id=runtime.config.id,
            source_type="postgresql",
            source_instance=runtime.config.source_instance,
            database_name=runtime.config.database_name,
            **event_payload,
        )

        engine.store.upsert_delivery(
            DeliveryAttempt(
                request_id="request-existing-1",
                trace_id="trace-existing-1",
                source_id=runtime.config.id,
                external_ref="external-ref-1",
                tx_id=None,
                status="prepared_registered",
                prepared_action={
                    "contract": "verifbill",
                    "action": "submit",
                    "data": {
                        "payer": "enterpriseac1",
                        "submitter": "enterpriseac1",
                        "schema_id": 1,
                        "policy_id": 1,
                        "object_hash": "2" * 64,
                        "external_ref": "1" * 64,
                    },
                },
                last_error=None,
                updated_at="2026-04-18T08:00:00Z",
            )
        )

        original_canonicalize = event_payload["checkpoint_token"]
        event_payload["checkpoint_token"] = "lsn:dup"
        try:
            prepare_calls: list[dict[str, object]] = []
            wait_calls: list[str] = []
            register_calls: list[str] = []

            engine.ingress = SimpleNamespace(
                prepare_single=lambda payload: prepare_calls.append(payload) or None
            )

            class DuplicateChain:
                def push_prepared_action(self, prepared_action):
                    raise RuntimeError("assertion failure with message: duplicate request for submitter")

            engine.chain = DuplicateChain()
            engine.watcher = SimpleNamespace(
                register=lambda prepared_obj, envelope: register_calls.append(prepared_obj.request_id),
                mark_failed=lambda request_id, reason, details=None: None,
                wait_for_finalized=lambda request_id, timeout_sec, poll_interval_sec: wait_calls.append(request_id),
            )
            engine.receipt = SimpleNamespace(
                get_receipt=lambda request_id: {"request_id": request_id, "tx_id": "b" * 64, "block_num": 777}
            )
            engine.audit = SimpleNamespace(get_chain=lambda request_id: {"request_id": request_id, "links": []})

            with patch("denotary_db_agent.engine.canonicalize_event") as canonicalize_mock:
                canonicalize_mock.return_value = SimpleNamespace(
                    source_type="postgresql",
                    source_instance=runtime.config.source_instance,
                    database_name=runtime.config.database_name,
                    schema_or_namespace="public",
                    table_or_collection="invoices",
                    operation="update",
                    primary_key={"id": 1},
                    change_version="lsn:dup",
                    commit_timestamp="2026-04-18T08:00:00Z",
                    before_hash=None,
                    after_hash="a" * 64,
                    metadata_hash="b" * 64,
                    external_ref="external-ref-1",
                    trace_id="trace-live-1",
                    to_prepare_payload=lambda submitter, schema_id, policy_id: {
                        "submitter": submitter,
                        "payload": {"mode": "single"},
                        "external_ref": "external-ref-1",
                    },
                )
                engine._process_event(runtime, event)
        finally:
            event_payload["checkpoint_token"] = original_canonicalize

        self.assertEqual(prepare_calls, [])
        self.assertEqual(register_calls, [])
        self.assertEqual(wait_calls, ["request-existing-1"])
        proof = engine.store.get_proof("request-existing-1")
        self.assertIsNotNone(proof)
        delivery = engine.store.list_deliveries_by_external_ref(runtime.config.id, "external-ref-1")[0]
        self.assertEqual(delivery["status"], "finalized_exported")
        self.assertEqual(delivery["tx_id"], "b" * 64)
        checkpoints = engine.checkpoint_summary()
        self.assertEqual(len(checkpoints), 1)
        self.assertEqual(checkpoints[0]["token"], "lsn:1")

    def test_health_reports_logical_runtime_warnings(self) -> None:
        engine = AgentEngine(load_config(self.config_path))
        source_config = engine.config.sources[0]
        source_config.options["logical_warn_retained_wal_bytes"] = 100
        source_config.options["logical_warn_flush_lag_bytes"] = 50

        fake_adapter = SimpleNamespace(
            inspect=lambda: {
                "capture_mode": "logical",
                "cdc": {
                    "slot_exists": True,
                    "publication_in_sync": False,
                    "replica_identity_in_sync": False,
                    "stream_backoff_active": True,
                    "stream_backoff_remaining_sec": 3.5,
                    "stream_backoff_until": "2026-04-18T12:00:01Z",
                    "stream_last_error_kind": "connection_lost",
                    "stream_failure_streak": 2,
                    "retained_wal_bytes": 128,
                    "flush_lag_bytes": 64,
                },
            }
        )

        with patch.object(
            engine,
            "runtimes",
            return_value=[SourceRuntime(config=source_config, adapter=fake_adapter)],
        ):
            health = engine.health()

        self.assertFalse(health["sources"][0]["ok"])
        self.assertEqual(health["sources"][0]["severity"], "degraded")
        self.assertEqual(health["sources"][0]["capture_mode"], "logical")
        self.assertIn("pgoutput publication tables are out of sync with tracked tables", health["sources"][0]["warnings"])
        self.assertIn(
            "tracked logical tables are out of sync with expected REPLICA IDENTITY mode",
            health["sources"][0]["warnings"],
        )
        self.assertIn(
            "postgres stream is in reconnect cooldown after connection_lost; retry after 2026-04-18T12:00:01Z",
            health["sources"][0]["warnings"],
        )

    def test_health_marks_missing_logical_slot_as_critical(self) -> None:
        engine = AgentEngine(load_config(self.config_path))
        source_config = engine.config.sources[0]

        fake_adapter = SimpleNamespace(
            inspect=lambda: {
                "capture_mode": "logical",
                "cdc": {
                    "slot_exists": False,
                    "publication_in_sync": True,
                    "replica_identity_in_sync": True,
                    "stream_backoff_active": False,
                    "stream_failure_streak": 0,
                    "retained_wal_bytes": 0,
                    "flush_lag_bytes": 0,
                },
            }
        )

        with patch.object(
            engine,
            "runtimes",
            return_value=[SourceRuntime(config=source_config, adapter=fake_adapter)],
        ):
            health = engine.health()

        self.assertFalse(health["sources"][0]["ok"])
        self.assertEqual(health["sources"][0]["severity"], "critical")
        self.assertIn("logical slot is missing", health["sources"][0]["warnings"])

    def test_health_marks_stream_fallback_as_degraded(self) -> None:
        engine = AgentEngine(load_config(self.config_path))
        source_config = engine.config.sources[0]

        fake_adapter = SimpleNamespace(
            inspect=lambda: {
                "capture_mode": "logical",
                "cdc": {
                    "slot_exists": True,
                    "publication_in_sync": True,
                    "replica_identity_in_sync": True,
                    "stream_fallback_active": True,
                    "stream_fallback_until": "2026-04-18T12:05:00Z",
                    "stream_fallback_reason": "connection_lost",
                    "stream_backoff_active": False,
                    "stream_failure_streak": 0,
                    "retained_wal_bytes": 0,
                    "flush_lag_bytes": 0,
                },
            }
        )

        with patch.object(
            engine,
            "runtimes",
            return_value=[SourceRuntime(config=source_config, adapter=fake_adapter)],
        ):
            health = engine.health()

        self.assertFalse(health["sources"][0]["ok"])
        self.assertEqual(health["sources"][0]["severity"], "degraded")
        self.assertIn(
            "postgres stream is temporarily using peek fallback after connection_lost; retry stream after 2026-04-18T12:05:00Z",
            health["sources"][0]["warnings"],
        )

    def test_health_marks_stream_probation_as_degraded(self) -> None:
        engine = AgentEngine(load_config(self.config_path))
        source_config = engine.config.sources[0]

        fake_adapter = SimpleNamespace(
            inspect=lambda: {
                "capture_mode": "logical",
                "cdc": {
                    "slot_exists": True,
                    "publication_in_sync": True,
                    "replica_identity_in_sync": True,
                    "stream_fallback_active": False,
                    "stream_probation_active": True,
                    "stream_probation_until": "2026-04-18T12:06:00Z",
                    "stream_probation_reason": "connection_lost",
                    "stream_backoff_active": False,
                    "stream_failure_streak": 0,
                    "retained_wal_bytes": 0,
                    "flush_lag_bytes": 0,
                },
            }
        )

        with patch.object(
            engine,
            "runtimes",
            return_value=[SourceRuntime(config=source_config, adapter=fake_adapter)],
        ):
            health = engine.health()

        self.assertFalse(health["sources"][0]["ok"])
        self.assertEqual(health["sources"][0]["severity"], "degraded")
        self.assertIn(
            "postgres stream is in probation after returning from fallback due to connection_lost; observe until 2026-04-18T12:06:00Z",
            health["sources"][0]["warnings"],
        )

    def test_diagnostics_reports_compact_stream_runtime_state(self) -> None:
        engine = AgentEngine(load_config(self.config_path))
        source_config = engine.config.sources[0]

        fake_adapter = SimpleNamespace(
            inspect=lambda: {
                "source_id": source_config.id,
                "adapter": source_config.adapter,
                "capture_mode": "logical",
                "cdc": {
                    "runtime_mode": "stream",
                    "effective_runtime_mode": "peek",
                    "stream_session_active": False,
                    "stream_start_lsn": "0/16B6A28",
                    "stream_acknowledged_lsn": "0/16B6A40",
                    "stream_connect_count": 4,
                    "stream_reconnect_count": 3,
                    "stream_last_connect_at": "2026-04-18T12:00:00Z",
                    "stream_last_reconnect_at": "2026-04-18T12:01:00Z",
                    "stream_last_reconnect_reason": "connection_lost",
                    "stream_last_error": "broken stream",
                    "stream_last_error_kind": "connection_lost",
                    "stream_last_error_at": "2026-04-18T12:01:00Z",
                    "stream_error_history": [{"at": "2026-04-18T12:01:00Z", "kind": "connection_lost", "message": "broken stream"}],
                    "stream_failure_streak": 3,
                    "stream_backoff_active": True,
                    "stream_backoff_remaining_sec": 2.5,
                    "stream_backoff_until": "2026-04-18T12:01:03Z",
                    "stream_fallback_active": True,
                    "stream_fallback_remaining_sec": 20.0,
                    "stream_fallback_until": "2026-04-18T12:01:20Z",
                    "stream_fallback_reason": "connection_lost",
                    "stream_probation_active": False,
                    "stream_probation_remaining_sec": 0.0,
                    "stream_probation_until": "",
                    "stream_probation_reason": "",
                    "slot_exists": True,
                    "slot_active": False,
                    "restart_lsn": "0/16B6A28",
                    "confirmed_flush_lsn": "0/16B6A40",
                    "pending_changes": False,
                    "retained_wal_bytes": 0,
                    "flush_lag_bytes": 0,
                },
            }
        )

        with patch.object(
            engine,
            "runtimes",
            return_value=[SourceRuntime(config=source_config, adapter=fake_adapter)],
        ):
            diagnostics = engine.diagnostics(source_config.id)

        self.assertEqual(len(diagnostics["sources"]), 1)
        source = diagnostics["sources"][0]
        self.assertEqual(source["severity"], "degraded")
        self.assertEqual(source["stream"]["configured_runtime_mode"], "stream")
        self.assertEqual(source["stream"]["effective_runtime_mode"], "peek")
        self.assertEqual(source["stream"]["reconnect_count"], 3)
        self.assertEqual(source["stream"]["fallback_reason"], "connection_lost")
        self.assertEqual(source["logical_slot"]["confirmed_flush_lsn"], "0/16B6A40")

    def test_diagnostics_returns_inspect_error_instead_of_raising(self) -> None:
        engine = AgentEngine(load_config(self.config_path))
        source_config = engine.config.sources[0]

        fake_adapter = SimpleNamespace(
            inspect=lambda: (_ for _ in ()).throw(RuntimeError("db offline"))
        )

        with patch.object(
            engine,
            "runtimes",
            return_value=[SourceRuntime(config=source_config, adapter=fake_adapter)],
        ):
            diagnostics = engine.diagnostics(source_config.id)

        self.assertEqual(len(diagnostics["sources"]), 1)
        source = diagnostics["sources"][0]
        self.assertFalse(source["ok"])
        self.assertEqual(source["severity"], "error")
        self.assertEqual(source["inspect_error"], "db offline")

    def test_runtimes_reuse_adapter_instances_between_calls(self) -> None:
        engine = AgentEngine(load_config(self.config_path))

        first = engine.runtimes()
        second = engine.runtimes()

        self.assertIs(first[0].adapter, second[0].adapter)

    def test_apply_runtime_retention_prunes_store_rows_and_deletes_proof_files(self) -> None:
        config = json.loads(self.config_path.read_text(encoding="utf-8"))
        config["storage"]["delivery_retention"] = 1
        config["storage"]["proof_retention"] = 1
        config["storage"]["dlq_retention"] = 1
        self.config_path.write_text(json.dumps(config), encoding="utf-8")

        engine = AgentEngine(load_config(self.config_path))
        runtime = engine.runtimes()[0]
        proof_root = Path(engine.config.storage.proof_dir)
        proof_root.mkdir(parents=True, exist_ok=True)
        stale_proof = proof_root / "old-proof.json"
        fresh_proof = proof_root / "new-proof.json"
        stale_proof.write_text("{}", encoding="utf-8")
        fresh_proof.write_text("{}", encoding="utf-8")

        engine.store.upsert_delivery(
            DeliveryAttempt(
                request_id="req-old",
                trace_id="trace-old",
                source_id=runtime.config.id,
                external_ref="ext-old",
                tx_id=None,
                status="failed",
                prepared_action=None,
                last_error="old",
                updated_at="2026-04-17T10:00:00Z",
            )
        )
        engine.store.upsert_delivery(
            DeliveryAttempt(
                request_id="req-new",
                trace_id="trace-new",
                source_id=runtime.config.id,
                external_ref="ext-new",
                tx_id=None,
                status="prepared_registered",
                prepared_action=None,
                last_error=None,
                updated_at="2026-04-17T10:00:01Z",
            )
        )
        engine.store.upsert_proof(
            ProofArtifact(
                request_id="proof-old",
                source_id=runtime.config.id,
                receipt={"ok": True},
                audit_chain={"ok": True},
                export_path=str(stale_proof),
                updated_at="2026-04-17T10:00:00Z",
            )
        )
        engine.store.upsert_proof(
            ProofArtifact(
                request_id="proof-new",
                source_id=runtime.config.id,
                receipt={"ok": True},
                audit_chain={"ok": True},
                export_path=str(fresh_proof),
                updated_at="2026-04-17T10:00:01Z",
            )
        )
        engine.store.push_dlq(runtime.config.id, "old-error", {"index": 1}, "2026-04-17T10:00:00Z")
        engine.store.push_dlq(runtime.config.id, "new-error", {"index": 2}, "2026-04-17T10:00:01Z")

        engine._apply_runtime_retention(runtime)

        deliveries = engine.store.list_deliveries(runtime.config.id)
        proofs = engine.store.list_proofs(runtime.config.id)
        dlq = engine.store.list_dlq(runtime.config.id)
        self.assertEqual([row["request_id"] for row in deliveries], ["req-new"])
        self.assertEqual([row["request_id"] for row in proofs], ["proof-new"])
        self.assertEqual([row["reason"] for row in dlq], ["new-error"])
        self.assertFalse(os.path.exists(stale_proof))
        self.assertTrue(os.path.exists(fresh_proof))

    def test_periodic_diagnostics_snapshot_is_written_and_pruned(self) -> None:
        config = json.loads(self.config_path.read_text(encoding="utf-8"))
        config["storage"]["diagnostics_snapshot_interval_sec"] = 1.0
        config["storage"]["diagnostics_snapshot_retention"] = 1
        self.config_path.write_text(json.dumps(config), encoding="utf-8")

        engine = AgentEngine(load_config(self.config_path))
        diagnostics_dir = Path(engine.config.storage.state_db).resolve().parent / "diagnostics"
        diagnostics_dir.mkdir(parents=True, exist_ok=True)
        old_snapshot = diagnostics_dir / "diagnostics-all-20260417T100000Z.json"
        old_snapshot.write_text("{}", encoding="utf-8")

        snapshot_path = engine._maybe_write_periodic_diagnostics_snapshot()
        self.assertIsNotNone(snapshot_path)
        assert snapshot_path is not None
        self.assertTrue(Path(snapshot_path).exists())
        self.assertFalse(old_snapshot.exists())

        second_snapshot = engine._maybe_write_periodic_diagnostics_snapshot()
        self.assertIsNone(second_snapshot)

    def test_metrics_collects_single_source_snapshot_once(self) -> None:
        engine = AgentEngine(load_config(self.config_path))
        source_config = engine.config.sources[0]
        inspect_calls = {"count": 0}

        def inspect_payload() -> dict[str, object]:
            inspect_calls["count"] += 1
            return {
                "source_id": source_config.id,
                "adapter": source_config.adapter,
                "capture_mode": "logical",
                "cdc": {
                    "runtime": {
                        "transport": "stream",
                        "configured_runtime_mode": "stream",
                        "effective_runtime_mode": "stream",
                        "active": True,
                        "reconnect_count": 0,
                        "failure_streak": 0,
                        "backoff_active": False,
                        "fallback_active": False,
                        "probation_active": False,
                    },
                    "slot_exists": True,
                    "pending_changes": False,
                    "retained_wal_bytes": 0,
                    "flush_lag_bytes": 0,
                },
            }

        fake_adapter = SimpleNamespace(inspect=inspect_payload)

        with patch.object(
            engine,
            "runtimes",
            return_value=[SourceRuntime(config=source_config, adapter=fake_adapter)],
        ):
            metrics = engine.metrics(source_config.id)

        self.assertEqual(len(metrics["sources"]), 1)
        self.assertEqual(inspect_calls["count"], 1)

    def test_metrics_reports_compact_source_counters(self) -> None:
        engine = AgentEngine(load_config(self.config_path))
        source_config = engine.config.sources[0]

        fake_adapter = SimpleNamespace(
            inspect=lambda: {
                "source_id": source_config.id,
                "adapter": source_config.adapter,
                "capture_mode": "logical",
                "cdc": {
                    "runtime": {
                        "transport": "stream",
                        "configured_runtime_mode": "stream",
                        "effective_runtime_mode": "peek",
                        "active": False,
                        "reconnect_count": 2,
                        "failure_streak": 1,
                        "backoff_active": True,
                        "fallback_active": True,
                        "probation_active": False,
                    },
                    "slot_exists": True,
                    "pending_changes": True,
                    "retained_wal_bytes": 1234,
                    "flush_lag_bytes": 567,
                },
            }
        )

        with patch.object(
            engine,
            "runtimes",
            return_value=[SourceRuntime(config=source_config, adapter=fake_adapter)],
        ):
            metrics = engine.metrics(source_config.id)

        self.assertEqual(metrics["totals"]["source_count"], 1)
        self.assertEqual(metrics["totals"]["degraded_count"], 1)
        self.assertEqual(len(metrics["sources"]), 1)
        source = metrics["sources"][0]
        self.assertEqual(source["source_id"], source_config.id)
        self.assertEqual(source["severity"], "degraded")
        self.assertTrue(source["logical_slot_pending_changes"])
        self.assertEqual(source["logical_slot_retained_wal_bytes"], 1234)
        self.assertEqual(source["stream_effective_runtime_mode"], "peek")
        self.assertEqual(source["stream_reconnect_count"], 2)
        self.assertTrue(source["stream_backoff_active"])
        self.assertEqual(source["cdc_transport"], "stream")
        self.assertEqual(source["cdc_runtime_mode"], "peek")

    def test_metrics_reports_polling_cdc_runtime_contract(self) -> None:
        engine = AgentEngine(load_config(self.config_path))
        source_config = engine.config.sources[0]

        fake_adapter = SimpleNamespace(
            inspect=lambda: {
                "source_id": source_config.id,
                "adapter": source_config.adapter,
                "capture_mode": "change_tracking",
                "cdc": {
                    "configured_capture_mode": "change_tracking",
                    "is_cdc_mode": True,
                    "checkpoint_strategy": "change_tracking_version",
                    "activity_model": "polling",
                    "cdc_modes": ["change_tracking"],
                    "runtime": {
                        "transport": "polling",
                        "configured_runtime_mode": "change_tracking",
                        "effective_runtime_mode": "change_tracking",
                        "active": False,
                        "notification_aware": False,
                        "cursor": {"version": 42},
                    },
                },
            }
        )

        with patch.object(
            engine,
            "runtimes",
            return_value=[SourceRuntime(config=source_config, adapter=fake_adapter)],
        ):
            metrics = engine.metrics(source_config.id)

        source = metrics["sources"][0]
        self.assertEqual(source["cdc_transport"], "polling")
        self.assertEqual(source["cdc_runtime_mode"], "change_tracking")
        self.assertFalse(source["cdc_notification_aware"])
        self.assertEqual(source["cdc_cursor"], {"version": 42})

    def test_diagnostics_and_doctor_surface_shared_cdc_contract(self) -> None:
        engine = AgentEngine(load_config(self.config_path))
        source_config = engine.config.sources[0]

        fake_adapter = SimpleNamespace(
            validate_connection=lambda: None,
            inspect=lambda: {
                "source_id": source_config.id,
                "adapter": source_config.adapter,
                "capture_mode": "binlog",
                "tracked_tables": [{"table_name": "invoices"}],
                "cdc": {
                    "configured_capture_mode": "binlog",
                    "is_cdc_mode": True,
                    "checkpoint_strategy": "binlog_cursor",
                    "activity_model": "stream",
                    "cdc_modes": ["binlog"],
                    "runtime": {
                        "transport": "stream",
                        "active": True,
                        "configured_runtime_mode": "binlog",
                        "effective_runtime_mode": "binlog",
                    },
                },
            },
        )

        with patch.object(
            engine,
            "runtimes",
            return_value=[SourceRuntime(config=source_config, adapter=fake_adapter)],
        ):
            diagnostics = engine.diagnostics(source_config.id)
            doctor = engine.doctor(source_config.id)

        self.assertEqual(diagnostics["sources"][0]["cdc_contract"]["checkpoint_strategy"], "binlog_cursor")
        self.assertEqual(diagnostics["sources"][0]["cdc_contract"]["activity_model"], "stream")
        self.assertTrue(diagnostics["sources"][0]["cdc_contract"]["is_cdc_mode"])
        self.assertEqual(diagnostics["sources"][0]["cdc_runtime"]["transport"], "stream")
        self.assertTrue(diagnostics["sources"][0]["stream"]["session_active"])
        self.assertEqual(doctor["sources"][0]["cdc_contract"]["checkpoint_strategy"], "binlog_cursor")
        self.assertEqual(doctor["sources"][0]["cdc_contract"]["activity_model"], "stream")
        self.assertEqual(doctor["sources"][0]["cdc_runtime"]["configured_runtime_mode"], "binlog")

    def test_report_combines_doctor_metrics_diagnostics_and_status(self) -> None:
        engine = AgentEngine(load_config(self.config_path))

        report = engine.report("pg-core-ledger")

        self.assertEqual(report["agent_name"], "test-agent")
        self.assertEqual(report["source_filter"], "pg-core-ledger")
        self.assertIn("doctor", report)
        self.assertIn("metrics", report)
        self.assertIn("diagnostics", report)
        self.assertIn("status", report)
        self.assertEqual(len(report["status"]["sources"]), 1)
        self.assertEqual(report["status"]["sources"][0]["source_id"], "pg-core-ledger")

    def test_doctor_reports_submitter_key_gap_and_service_probe(self) -> None:
        engine = AgentEngine(load_config(self.config_path))

        report = engine.doctor("pg-core-ledger")

        self.assertEqual(report["agent_name"], "test-agent")
        self.assertEqual(report["services"]["ingress"]["severity"], "healthy")
        self.assertTrue(report["services"]["ingress"]["reachable"])
        self.assertEqual(report["services"]["watcher"]["severity"], "healthy")
        self.assertEqual(report["signer"]["severity"], "critical")
        self.assertFalse(report["signer"]["private_key_present"])
        self.assertFalse(report["signer"]["broadcast_ready"])
        self.assertEqual(len(report["sources"]), 1)
        self.assertTrue(report["sources"][0]["connectivity_ok"])
        self.assertIn("submitter_private_key is not configured", report["errors"])

    def test_doctor_marks_owner_permission_as_degraded_when_otherwise_ready(self) -> None:
        config = json.loads(self.config_path.read_text(encoding="utf-8"))
        config["denotary"]["chain_rpc_url"] = "https://history.denotary.io"
        config["denotary"]["submitter_private_key"] = "test-wif"
        config["denotary"]["submitter_permission"] = "owner"
        self.config_path.write_text(json.dumps(config), encoding="utf-8")

        engine = AgentEngine(load_config(self.config_path))
        engine.chain = SimpleNamespace(
            health=lambda: {"server_version_string": "v1", "chain_id": "abc"},
            get_account=lambda account: {"account_name": account, "permissions": [{"perm_name": "owner"}]},
        )

        with patch("denotary_db_agent.engine.derive_public_key_candidates", return_value={"EOS": "EOS_OWNER"}):
            report = engine.doctor("pg-core-ledger")

        self.assertEqual(report["signer"]["severity"], "degraded")
        self.assertTrue(report["signer"]["broadcast_ready"])
        self.assertIn("submitter_permission is owner; a dedicated hot permission such as dnanchor is recommended", report["warnings"])

    def test_doctor_accepts_cleos_wallet_backend_when_wallet_is_unlocked(self) -> None:
        config = json.loads(self.config_path.read_text(encoding="utf-8"))
        config["denotary"]["chain_rpc_url"] = "https://history.denotary.io"
        config["denotary"]["broadcast_backend"] = "cleos_wallet"
        config["denotary"]["wallet_command"] = ["wsl", "cleos"]
        self.config_path.write_text(json.dumps(config), encoding="utf-8")

        engine = AgentEngine(load_config(self.config_path))
        engine.chain = SimpleNamespace(
            health=lambda: {"server_version_string": "v1", "chain_id": "abc"},
            get_account=lambda account: {"account_name": account, "permissions": [{"perm_name": "dnanchor"}]},
            probe_wallet=lambda: {
                "ok": True,
                "command": ["wsl", "cleos"],
                "unlocked_wallet_count": 1,
                "has_unlocked_wallet": True,
                "raw": 'Wallets: ["dbagentstest *"]',
            },
        )

        report = engine.doctor("pg-core-ledger")

        self.assertEqual(report["signer"]["severity"], "degraded")
        self.assertEqual(report["signer"]["broadcast_backend"], "cleos_wallet")
        self.assertTrue(report["signer"]["broadcast_ready"])
        self.assertTrue(report["signer"]["wallet_probe"]["has_unlocked_wallet"])
        self.assertIn("wallet-backed broadcaster depends on an unlocked cleos wallet on the local host", report["warnings"])

    def test_doctor_accepts_env_file_hot_key_backend(self) -> None:
        env_path = Path(self.temp_dir.name) / "agent.env"
        env_path.write_text("DENOTARY_SUBMITTER_PRIVATE_KEY=test-wif\n", encoding="utf-8")
        config = json.loads(self.config_path.read_text(encoding="utf-8"))
        config["denotary"]["chain_rpc_url"] = "https://history.denotary.io"
        config["denotary"]["broadcast_backend"] = "private_key_env"
        config["denotary"]["env_file"] = "agent.env"
        self.config_path.write_text(json.dumps(config), encoding="utf-8")

        engine = AgentEngine(load_config(self.config_path))
        engine.chain = SimpleNamespace(
            health=lambda: {"server_version_string": "v1", "chain_id": "abc"},
            get_account=lambda account: {
                "account_name": account,
                "permissions": [
                    {
                        "perm_name": "dnanchor",
                        "required_auth": {"keys": [{"key": "EOS_MATCH", "weight": 1}]},
                    }
                ],
            },
        )

        with patch("denotary_db_agent.engine.derive_public_key_candidates", return_value={"EOS": "EOS_MATCH"}), patch(
            "denotary_db_agent.engine.inspect_secret_file_permissions",
            return_value={
                "path": str(env_path),
                "exists": True,
                "checked": True,
                "ok": True,
                "platform": "posix",
                "mode_octal": "0600",
                "severity": "healthy",
                "issues": [],
            },
        ):
            report = engine.doctor("pg-core-ledger")

        self.assertEqual(report["signer"]["severity"], "healthy")
        self.assertEqual(report["signer"]["effective_broadcast_backend"], "private_key_env")
        self.assertEqual(report["signer"]["private_key_source"], "env")
        self.assertTrue(report["signer"]["env_file_exists"])
        self.assertTrue(report["signer"]["env_file_permissions_checked"])
        self.assertTrue(report["signer"]["env_file_permissions_ok"])
        self.assertEqual(report["signer"]["env_file_mode_octal"], "0600")
        self.assertEqual(report["signer"]["permission_public_keys"], ["EOS_MATCH"])
        self.assertEqual(report["signer"]["derived_public_keys"], {"EOS": "EOS_MATCH"})
        self.assertTrue(report["signer"]["private_key_matches_permission"])
        self.assertTrue(report["signer"]["broadcast_ready"])
        self.assertEqual(report["signer"]["warnings"], [])

    def test_doctor_flags_overly_permissive_env_file_permissions(self) -> None:
        env_path = Path(self.temp_dir.name) / "agent.env"
        env_path.write_text("DENOTARY_SUBMITTER_PRIVATE_KEY=test-wif\n", encoding="utf-8")
        config = json.loads(self.config_path.read_text(encoding="utf-8"))
        config["denotary"]["chain_rpc_url"] = "https://history.denotary.io"
        config["denotary"]["broadcast_backend"] = "private_key_env"
        config["denotary"]["env_file"] = "agent.env"
        self.config_path.write_text(json.dumps(config), encoding="utf-8")

        engine = AgentEngine(load_config(self.config_path))
        engine.chain = SimpleNamespace(
            health=lambda: {"server_version_string": "v1", "chain_id": "abc"},
            get_account=lambda account: {"account_name": account, "permissions": [{"perm_name": "dnanchor"}]},
        )

        with patch(
            "denotary_db_agent.engine.inspect_secret_file_permissions",
            return_value={
                "path": str(env_path),
                "exists": True,
                "checked": True,
                "ok": False,
                "platform": "posix",
                "mode_octal": "0660",
                "severity": "critical",
                "issues": ["env_file is group-writable"],
            },
        ):
            report = engine.doctor("pg-core-ledger")

        self.assertEqual(report["signer"]["severity"], "critical")
        self.assertFalse(report["signer"]["env_file_permissions_ok"])
        self.assertEqual(report["signer"]["env_file_permission_severity"], "critical")
        self.assertEqual(report["signer"]["env_file_permission_issues"], ["env_file is group-writable"])
        self.assertFalse(report["signer"]["broadcast_ready"])
        self.assertIn("env_file permissions are too broad (0660)", " ".join(report["errors"]))

    def test_doctor_flags_hot_key_that_does_not_match_permission_keys(self) -> None:
        env_path = Path(self.temp_dir.name) / "agent.env"
        env_path.write_text("DENOTARY_SUBMITTER_PRIVATE_KEY=test-wif\n", encoding="utf-8")
        config = json.loads(self.config_path.read_text(encoding="utf-8"))
        config["denotary"]["chain_rpc_url"] = "https://history.denotary.io"
        config["denotary"]["broadcast_backend"] = "private_key_env"
        config["denotary"]["env_file"] = "agent.env"
        self.config_path.write_text(json.dumps(config), encoding="utf-8")

        engine = AgentEngine(load_config(self.config_path))
        engine.chain = SimpleNamespace(
            health=lambda: {"server_version_string": "v1", "chain_id": "abc"},
            get_account=lambda account: {
                "account_name": account,
                "permissions": [
                    {
                        "perm_name": "dnanchor",
                        "required_auth": {"keys": [{"key": "EOS_EXPECTED", "weight": 1}]},
                    }
                ],
            },
        )

        with patch("denotary_db_agent.engine.derive_public_key_candidates", return_value={"EOS": "EOS_OTHER"}), patch(
            "denotary_db_agent.engine.inspect_secret_file_permissions",
            return_value={
                "path": str(env_path),
                "exists": True,
                "checked": True,
                "ok": True,
                "platform": "posix",
                "mode_octal": "0600",
                "severity": "healthy",
                "issues": [],
            },
        ):
            report = engine.doctor("pg-core-ledger")

        self.assertEqual(report["signer"]["severity"], "critical")
        self.assertEqual(report["signer"]["permission_public_keys"], ["EOS_EXPECTED"])
        self.assertEqual(report["signer"]["derived_public_keys"], {"EOS": "EOS_OTHER"})
        self.assertFalse(report["signer"]["private_key_matches_permission"])
        self.assertFalse(report["signer"]["broadcast_ready"])
        self.assertIn("configured hot key does not match any key", " ".join(report["errors"]))

    def test_doctor_marks_broad_hot_permission_as_degraded(self) -> None:
        env_path = Path(self.temp_dir.name) / "agent.env"
        env_path.write_text("DENOTARY_SUBMITTER_PRIVATE_KEY=test-wif\n", encoding="utf-8")
        config = json.loads(self.config_path.read_text(encoding="utf-8"))
        config["denotary"]["chain_rpc_url"] = "https://history.denotary.io"
        config["denotary"]["broadcast_backend"] = "private_key_env"
        config["denotary"]["env_file"] = "agent.env"
        self.config_path.write_text(json.dumps(config), encoding="utf-8")

        engine = AgentEngine(load_config(self.config_path))
        engine.chain = SimpleNamespace(
            health=lambda: {"server_version_string": "v1", "chain_id": "abc"},
            get_account=lambda account: {
                "account_name": account,
                "permissions": [
                    {
                        "perm_name": "dnanchor",
                        "required_auth": {
                            "threshold": 2,
                            "keys": [
                                {"key": "EOS_MATCH", "weight": 1},
                                {"key": "EOS_EXTRA", "weight": 1},
                            ],
                            "accounts": [
                                {
                                    "permission": {
                                        "actor": "opsaccount",
                                        "permission": "active",
                                    },
                                    "weight": 1,
                                }
                            ],
                            "waits": [{"wait_sec": 30, "weight": 1}],
                        },
                    }
                ],
            },
        )

        with patch("denotary_db_agent.engine.derive_public_key_candidates", return_value={"EOS": "EOS_MATCH"}), patch(
            "denotary_db_agent.engine.inspect_secret_file_permissions",
            return_value={
                "path": str(env_path),
                "exists": True,
                "checked": True,
                "ok": True,
                "platform": "posix",
                "mode_octal": "0600",
                "severity": "healthy",
                "issues": [],
            },
        ):
            report = engine.doctor("pg-core-ledger")

        self.assertEqual(report["signer"]["severity"], "degraded")
        self.assertEqual(report["signer"]["permission_threshold"], 2)
        self.assertEqual(report["signer"]["permission_public_keys"], ["EOS_MATCH", "EOS_EXTRA"])
        self.assertEqual(report["signer"]["permission_account_links"], ["opsaccount@active"])
        self.assertEqual(report["signer"]["permission_waits"], [30])
        self.assertFalse(report["signer"]["permission_is_minimal_hot_key"])
        self.assertTrue(report["signer"]["private_key_matches_permission"])
        self.assertTrue(report["signer"]["broadcast_ready"])
        self.assertIn("uses threshold 2", " ".join(report["signer"]["warnings"]))
        self.assertIn("has 2 keys", " ".join(report["signer"]["warnings"]))
        self.assertIn("linked account permission", " ".join(report["signer"]["warnings"]))
        self.assertIn("delayed wait entries", " ".join(report["signer"]["warnings"]))
