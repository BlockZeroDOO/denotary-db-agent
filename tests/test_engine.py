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

from denotary_db_agent.config import load_config
from denotary_db_agent.engine import AgentEngine, SourceRuntime
from denotary_db_agent.models import DeliveryAttempt, ProofArtifact


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

        with unittest.mock.patch.object(
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

        with unittest.mock.patch.object(
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

        with unittest.mock.patch.object(
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

        with unittest.mock.patch.object(
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

        with unittest.mock.patch.object(
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

        with unittest.mock.patch.object(
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
