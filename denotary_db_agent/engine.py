from __future__ import annotations

import os
import time
from dataclasses import dataclass
from time import monotonic

from denotary_db_agent.adapters.base import BaseAdapter
from denotary_db_agent.adapters.registry import build_adapter
from denotary_db_agent.canonical import build_batch_external_ref, canonicalize_event
from denotary_db_agent.checkpoint_store import CheckpointStore
from denotary_db_agent.config import AgentConfig, SourceConfig
from denotary_db_agent.diagnostics_snapshots import export_diagnostics_snapshot
from denotary_db_agent.models import DeliveryAttempt, ProofArtifact, event_debug_dict, utc_now
from denotary_db_agent.transport import (
    AuditClient,
    ChainClient,
    IngressClient,
    ReceiptClient,
    WatcherClient,
    export_batch_proof_bundle,
    export_proof_bundle,
)


@dataclass
class SourceRuntime:
    config: SourceConfig
    adapter: BaseAdapter


class RetryPolicy:
    def __init__(self, max_attempts: int = 3, base_delay_seconds: float = 0.1):
        self.max_attempts = max_attempts
        self.base_delay_seconds = base_delay_seconds

    def delays(self) -> list[float]:
        return [self.base_delay_seconds * (2**index) for index in range(max(self.max_attempts - 1, 0))]


class AgentEngine:
    def __init__(self, config: AgentConfig):
        self.config = config
        self.store = CheckpointStore(config.storage.state_db)
        self.ingress = IngressClient(config.denotary.ingress_url)
        self.watcher = WatcherClient(
            config.denotary.watcher_url,
            config.denotary.watcher_auth_token,
            config.denotary.chain_rpc_url,
        )
        self.receipt = ReceiptClient(config.denotary.receipt_url) if config.denotary.receipt_url else None
        self.audit = AuditClient(config.denotary.audit_url) if config.denotary.audit_url else None
        self.chain = (
            ChainClient(
                config.denotary.chain_rpc_url,
                config.denotary.submitter,
                config.denotary.submitter_permission,
                config.denotary.submitter_private_key,
            )
            if config.denotary.chain_rpc_url and config.denotary.submitter_private_key
            else None
        )
        self.retry_policy = RetryPolicy()
        self._runtime_cache: dict[str, SourceRuntime] = {}
        self._last_diagnostics_snapshot_monotonic: float | None = None

    def close(self) -> None:
        for runtime in self._runtime_cache.values():
            runtime.adapter.stop_stream()
        self._runtime_cache.clear()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    def runtimes(self) -> list[SourceRuntime]:
        enabled_ids = {item.id for item in self.config.sources if item.enabled}
        stale_ids = [source_id for source_id in self._runtime_cache if source_id not in enabled_ids]
        for source_id in stale_ids:
            runtime = self._runtime_cache.pop(source_id)
            runtime.adapter.stop_stream()

        runtimes: list[SourceRuntime] = []
        for item in self.config.sources:
            if not item.enabled:
                continue
            runtime = self._runtime_cache.get(item.id)
            if runtime is None or runtime.config is not item:
                if runtime is not None:
                    runtime.adapter.stop_stream()
                runtime = SourceRuntime(config=item, adapter=build_adapter(item))
                self._runtime_cache[item.id] = runtime
            runtimes.append(runtime)
        return runtimes

    def _is_paused(self, source_id: str) -> bool:
        return self.store.is_source_paused(source_id)

    def validate(self) -> list[dict[str, str]]:
        results: list[dict[str, str]] = []
        if self.chain is not None:
            chain_info = self.chain.health()
            results.append(
                {
                    "source_id": "_chain",
                    "adapter": "antelope-enterprise",
                    "source_type": "chain",
                    "minimum_version": str(chain_info.get("server_version_string") or chain_info.get("server_version") or "unknown"),
                    "supports_cdc": "false",
                    "supports_snapshot": "false",
                }
            )
        if self.receipt is not None:
            self.receipt.health()
        if self.audit is not None:
            self.audit.health()
        for runtime in self.runtimes():
            runtime.adapter.validate_connection()
            capabilities = runtime.adapter.discover_capabilities()
            results.append(
                {
                    "source_id": runtime.config.id,
                    "adapter": runtime.config.adapter,
                    "source_type": capabilities.source_type,
                    "minimum_version": capabilities.minimum_version,
                    "supports_cdc": str(capabilities.supports_cdc).lower(),
                    "supports_snapshot": str(capabilities.supports_snapshot).lower(),
                }
            )
        return results

    def status(self) -> dict:
        return {
            "agent_name": self.config.agent_name,
            "sources": [
                {
                    "source_id": runtime.config.id,
                    "adapter": runtime.config.adapter,
                    "paused": self._is_paused(runtime.config.id),
                    "runtime_signature_stored": self.store.get_runtime_signature(runtime.config.id) is not None,
                    "checkpoint": self._checkpoint_value(runtime.config.id),
                    "deliveries": len(self.store.list_deliveries(runtime.config.id)),
                    "proofs": len(self.store.list_proofs(runtime.config.id)),
                    "dlq": len(self.store.list_dlq(runtime.config.id)),
                }
                for runtime in self.runtimes()
            ],
        }

    def health(self) -> dict:
        services: dict[str, dict[str, object]] = {
            "ingress": {"configured": bool(self.config.denotary.ingress_url), "url": self.config.denotary.ingress_url},
            "watcher": {"configured": bool(self.config.denotary.watcher_url), "url": self.config.denotary.watcher_url},
            "receipt": {"configured": bool(self.receipt), "url": self.config.denotary.receipt_url},
            "audit": {"configured": bool(self.audit), "url": self.config.denotary.audit_url},
            "chain": {"configured": bool(self.chain), "url": self.config.denotary.chain_rpc_url},
        }
        if self.chain is not None:
            try:
                chain_info = self.chain.health()
                services["chain"].update({"ok": True, "server_version": chain_info.get("server_version_string") or chain_info.get("server_version")})
            except Exception as exc:  # noqa: BLE001
                services["chain"].update({"ok": False, "error": str(exc)})
        if self.receipt is not None:
            try:
                services["receipt"].update({"ok": True, "response": self.receipt.health()})
            except Exception as exc:  # noqa: BLE001
                services["receipt"].update({"ok": False, "error": str(exc)})
        if self.audit is not None:
            try:
                services["audit"].update({"ok": True, "response": self.audit.health()})
            except Exception as exc:  # noqa: BLE001
                services["audit"].update({"ok": False, "error": str(exc)})
        source_entries = []
        for runtime in self.runtimes():
            source_entry: dict[str, object] = {
                "source_id": runtime.config.id,
                "adapter": runtime.config.adapter,
                "paused": self._is_paused(runtime.config.id),
                "runtime_signature_stored": self.store.get_runtime_signature(runtime.config.id) is not None,
                "checkpoint": self._checkpoint_value(runtime.config.id),
                "deliveries": len(self.store.list_deliveries(runtime.config.id)),
                "proofs": len(self.store.list_proofs(runtime.config.id)),
                "dlq": len(self.store.list_dlq(runtime.config.id)),
            }
            try:
                inspect_details = runtime.adapter.inspect()
                source_entry["capture_mode"] = inspect_details.get("capture_mode")
                cdc = inspect_details.get("cdc")
                warnings = self._build_source_health_warnings(runtime, cdc if isinstance(cdc, dict) else None)
                if isinstance(cdc, dict):
                    source_entry["cdc"] = cdc
                source_entry["warnings"] = warnings
                severity = self._classify_source_health_severity(cdc if isinstance(cdc, dict) else None, warnings)
                source_entry["severity"] = severity
                source_entry["ok"] = severity == "healthy"
            except Exception as exc:  # noqa: BLE001
                source_entry["ok"] = False
                source_entry["severity"] = "error"
                source_entry["warnings"] = [str(exc)]
            source_entries.append(source_entry)
        return {
            "agent_name": self.config.agent_name,
            "services": services,
            "sources": source_entries,
        }

    def _classify_source_health_severity(self, cdc: dict[str, object] | None, warnings: list[str]) -> str:
        if not warnings:
            return "healthy"
        if cdc is None:
            return "degraded"
        if cdc.get("slot_exists") is False:
            return "critical"
        if cdc.get("stream_fallback_active") is True:
            return "degraded"
        if cdc.get("stream_probation_active") is True:
            return "degraded"
        if cdc.get("stream_backoff_active") is True:
            failure_streak = int(cdc.get("stream_failure_streak") or 0)
            return "critical" if failure_streak >= 3 else "degraded"
        return "degraded"

    def _build_source_health_warnings(self, runtime: SourceRuntime, cdc: dict[str, object] | None) -> list[str]:
        if cdc is None:
            return []
        warnings: list[str] = []
        retained_warn_bytes = int(runtime.config.options.get("logical_warn_retained_wal_bytes", 268_435_456))
        flush_warn_bytes = int(runtime.config.options.get("logical_warn_flush_lag_bytes", 67_108_864))
        retained_wal_bytes = cdc.get("retained_wal_bytes")
        flush_lag_bytes = cdc.get("flush_lag_bytes")
        if cdc.get("slot_exists") is False:
            warnings.append("logical slot is missing")
        if cdc.get("publication_in_sync") is False:
            warnings.append("pgoutput publication tables are out of sync with tracked tables")
        if cdc.get("replica_identity_in_sync") is False:
            warnings.append("tracked logical tables are out of sync with expected REPLICA IDENTITY mode")
        if cdc.get("stream_fallback_active") is True:
            fallback_reason = cdc.get("stream_fallback_reason") or "runtime_error"
            fallback_until = cdc.get("stream_fallback_until") or "unknown"
            warnings.append(f"postgres stream is temporarily using peek fallback after {fallback_reason}; retry stream after {fallback_until}")
        if cdc.get("stream_probation_active") is True:
            probation_reason = cdc.get("stream_probation_reason") or "fallback_recovery"
            probation_until = cdc.get("stream_probation_until") or "unknown"
            warnings.append(
                f"postgres stream is in probation after returning from fallback due to {probation_reason}; observe until {probation_until}"
            )
        if cdc.get("stream_backoff_active") is True:
            error_kind = cdc.get("stream_last_error_kind") or "runtime_error"
            backoff_until = cdc.get("stream_backoff_until") or "unknown"
            warnings.append(f"postgres stream is in reconnect cooldown after {error_kind}; retry after {backoff_until}")
        elif isinstance(cdc.get("stream_failure_streak"), int) and int(cdc["stream_failure_streak"]) > 0:
            error_kind = cdc.get("stream_last_error_kind") or "runtime_error"
            warnings.append(
                f"postgres stream has {int(cdc['stream_failure_streak'])} consecutive failure(s); last error kind: {error_kind}"
            )
        if isinstance(retained_wal_bytes, int) and retained_wal_bytes > retained_warn_bytes:
            warnings.append(
                f"logical slot retained WAL is above threshold: {retained_wal_bytes} bytes > {retained_warn_bytes} bytes"
            )
        if isinstance(flush_lag_bytes, int) and flush_lag_bytes > flush_warn_bytes:
            warnings.append(
                f"logical slot flush lag is above threshold: {flush_lag_bytes} bytes > {flush_warn_bytes} bytes"
            )
        return warnings

    def bootstrap(self, source_id: str | None = None) -> dict:
        results = []
        for runtime in self.runtimes():
            if source_id and runtime.config.id != source_id:
                continue
            summary = runtime.adapter.bootstrap()
            self.store.set_runtime_signature(runtime.config.id, runtime.adapter.runtime_signature(), utc_now().isoformat())
            results.append(summary)
        return {"agent_name": self.config.agent_name, "sources": results}

    def inspect(self, source_id: str | None = None) -> dict:
        results = []
        for runtime in self.runtimes():
            if source_id and runtime.config.id != source_id:
                continue
            results.append(runtime.adapter.inspect())
        return {"agent_name": self.config.agent_name, "sources": results}

    def diagnostics(self, source_id: str | None = None) -> dict:
        health = self.health()
        results: list[dict[str, object]] = []
        runtime_by_source = {runtime.config.id: runtime for runtime in self.runtimes()}
        for source in health["sources"]:
            current_source_id = str(source["source_id"])
            if source_id and current_source_id != source_id:
                continue
            inspect_source: dict[str, object] = {}
            runtime = runtime_by_source.get(current_source_id)
            if runtime is not None:
                try:
                    inspect_source = runtime.adapter.inspect()
                except Exception as exc:  # noqa: BLE001
                    inspect_source = {
                        "source_id": current_source_id,
                        "adapter": runtime.config.adapter,
                        "capture_mode": source.get("capture_mode"),
                        "inspect_error": str(exc),
                    }
            cdc = inspect_source.get("cdc") if isinstance(inspect_source, dict) else None
            entry: dict[str, object] = {
                "source_id": current_source_id,
                "adapter": source.get("adapter"),
                "capture_mode": source.get("capture_mode") or inspect_source.get("capture_mode"),
                "severity": source.get("severity", "unknown"),
                "ok": source.get("ok", False),
                "warnings": source.get("warnings", []),
                "paused": source.get("paused", False),
            }
            if "inspect_error" in inspect_source:
                entry["inspect_error"] = inspect_source["inspect_error"]
            if isinstance(cdc, dict):
                entry["stream"] = {
                    "configured_runtime_mode": cdc.get("runtime_mode"),
                    "effective_runtime_mode": cdc.get("effective_runtime_mode", cdc.get("runtime_mode")),
                    "session_active": cdc.get("stream_session_active"),
                    "start_lsn": cdc.get("stream_start_lsn"),
                    "acknowledged_lsn": cdc.get("stream_acknowledged_lsn"),
                    "connect_count": cdc.get("stream_connect_count"),
                    "reconnect_count": cdc.get("stream_reconnect_count"),
                    "last_connect_at": cdc.get("stream_last_connect_at"),
                    "last_reconnect_at": cdc.get("stream_last_reconnect_at"),
                    "last_reconnect_reason": cdc.get("stream_last_reconnect_reason"),
                    "last_error": cdc.get("stream_last_error"),
                    "last_error_kind": cdc.get("stream_last_error_kind"),
                    "last_error_at": cdc.get("stream_last_error_at"),
                    "error_history": cdc.get("stream_error_history", []),
                    "failure_streak": cdc.get("stream_failure_streak"),
                    "backoff_active": cdc.get("stream_backoff_active"),
                    "backoff_remaining_sec": cdc.get("stream_backoff_remaining_sec"),
                    "backoff_until": cdc.get("stream_backoff_until"),
                    "fallback_active": cdc.get("stream_fallback_active"),
                    "fallback_remaining_sec": cdc.get("stream_fallback_remaining_sec"),
                    "fallback_until": cdc.get("stream_fallback_until"),
                    "fallback_reason": cdc.get("stream_fallback_reason"),
                    "probation_active": cdc.get("stream_probation_active"),
                    "probation_remaining_sec": cdc.get("stream_probation_remaining_sec"),
                    "probation_until": cdc.get("stream_probation_until"),
                    "probation_reason": cdc.get("stream_probation_reason"),
                }
                entry["logical_slot"] = {
                    "slot_exists": cdc.get("slot_exists"),
                    "slot_active": cdc.get("slot_active"),
                    "restart_lsn": cdc.get("restart_lsn"),
                    "confirmed_flush_lsn": cdc.get("confirmed_flush_lsn"),
                    "pending_changes": cdc.get("pending_changes"),
                    "retained_wal_bytes": cdc.get("retained_wal_bytes"),
                    "flush_lag_bytes": cdc.get("flush_lag_bytes"),
                }
            results.append(entry)
        return {"agent_name": self.config.agent_name, "sources": results}

    def metrics(self, source_id: str | None = None) -> dict:
        diagnostics = self.diagnostics(source_id)
        sources: list[dict[str, object]] = []
        totals = {
            "source_count": 0,
            "ok_count": 0,
            "paused_count": 0,
            "delivery_count": 0,
            "proof_count": 0,
            "dlq_count": 0,
            "degraded_count": 0,
            "critical_count": 0,
            "error_count": 0,
        }

        for health_source in self.health()["sources"]:
            current_source_id = str(health_source["source_id"])
            if source_id and current_source_id != source_id:
                continue
            diagnostics_source = next(
                (item for item in diagnostics["sources"] if str(item.get("source_id")) == current_source_id),
                {},
            )
            stream = diagnostics_source.get("stream", {}) if isinstance(diagnostics_source, dict) else {}
            logical_slot = diagnostics_source.get("logical_slot", {}) if isinstance(diagnostics_source, dict) else {}
            severity = str(health_source.get("severity", "unknown"))

            entry = {
                "source_id": current_source_id,
                "adapter": health_source.get("adapter"),
                "capture_mode": health_source.get("capture_mode"),
                "severity": severity,
                "ok": bool(health_source.get("ok", False)),
                "paused": bool(health_source.get("paused", False)),
                "runtime_signature_stored": bool(health_source.get("runtime_signature_stored", False)),
                "delivery_count": int(health_source.get("deliveries", 0) or 0),
                "proof_count": int(health_source.get("proofs", 0) or 0),
                "dlq_count": int(health_source.get("dlq", 0) or 0),
                "warning_count": len(list(health_source.get("warnings", []))),
                "logical_slot_pending_changes": bool(logical_slot.get("pending_changes", False)),
                "logical_slot_retained_wal_bytes": int(logical_slot.get("retained_wal_bytes", 0) or 0),
                "logical_slot_flush_lag_bytes": int(logical_slot.get("flush_lag_bytes", 0) or 0),
                "stream_effective_runtime_mode": stream.get("effective_runtime_mode"),
                "stream_session_active": bool(stream.get("session_active", False)),
                "stream_reconnect_count": int(stream.get("reconnect_count", 0) or 0),
                "stream_failure_streak": int(stream.get("failure_streak", 0) or 0),
                "stream_backoff_active": bool(stream.get("backoff_active", False)),
                "stream_fallback_active": bool(stream.get("fallback_active", False)),
                "stream_probation_active": bool(stream.get("probation_active", False)),
            }
            sources.append(entry)

            totals["source_count"] += 1
            totals["ok_count"] += 1 if entry["ok"] else 0
            totals["paused_count"] += 1 if entry["paused"] else 0
            totals["delivery_count"] += int(entry["delivery_count"])
            totals["proof_count"] += int(entry["proof_count"])
            totals["dlq_count"] += int(entry["dlq_count"])
            if severity == "degraded":
                totals["degraded_count"] += 1
            elif severity == "critical":
                totals["critical_count"] += 1
            elif severity == "error":
                totals["error_count"] += 1

        return {
            "agent_name": self.config.agent_name,
            "totals": totals,
            "sources": sources,
        }

    def run_once(self) -> dict[str, int]:
        return self._run_runtimes_once(self.runtimes())

    def _run_runtimes_once(self, runtimes: list[SourceRuntime]) -> dict[str, int]:
        processed = 0
        failed = 0
        for runtime in runtimes:
            if self._is_paused(runtime.config.id):
                continue
            self._ensure_runtime_ready(runtime)
            checkpoint = self.store.get_checkpoint(runtime.config.id)
            runtime.adapter.resume_from_checkpoint(checkpoint)
            event_iter = runtime.adapter.read_snapshot(checkpoint)
            if runtime.config.options.get("capture_mode") in {"trigger", "logical"}:
                event_iter = runtime.adapter.start_stream(checkpoint)
            events = list(event_iter)
            if runtime.config.batch_enabled and events:
                chunk_size = max(1, runtime.config.batch_size)
                for start in range(0, len(events), chunk_size):
                    chunk = events[start : start + chunk_size]
                    try:
                        self._process_batch(runtime, chunk)
                        processed += len(chunk)
                    except Exception as exc:  # noqa: BLE001
                        failed += len(chunk)
                        for event in chunk:
                            self.store.push_dlq(runtime.config.id, str(exc), event_debug_dict(event), utc_now().isoformat())
                self._apply_runtime_retention(runtime)
                continue
            for event in events:
                try:
                    self._process_event(runtime, event)
                    processed += 1
                except Exception as exc:  # noqa: BLE001
                    failed += 1
                    self.store.push_dlq(runtime.config.id, str(exc), event_debug_dict(event), utc_now().isoformat())
            self._apply_runtime_retention(runtime)
        return {"processed": processed, "failed": failed}

    def run_forever(self, interval_sec: float, max_loops: int | None = None) -> dict[str, int]:
        if interval_sec <= 0:
            raise ValueError("interval_sec must be positive")

        total_processed = 0
        total_failed = 0
        loops = 0
        runtimes = self.runtimes()
        try:
            while True:
                result = self._run_runtimes_once(runtimes)
                total_processed += result["processed"]
                total_failed += result["failed"]
                loops += 1
                self._maybe_write_periodic_diagnostics_snapshot()
                if max_loops is not None and loops >= max_loops:
                    return {"processed": total_processed, "failed": total_failed, "loops": loops}
                self._wait_for_activity(runtimes, interval_sec)
        finally:
            for runtime in runtimes:
                try:
                    runtime.adapter.stop_stream()
                except Exception:
                    pass

    def _process_event(self, runtime: SourceRuntime, event) -> None:
        envelope = canonicalize_event(event)
        payload = envelope.to_prepare_payload(
            submitter=self.config.denotary.submitter,
            schema_id=self.config.denotary.schema_id,
            policy_id=self.config.denotary.policy_id,
        )

        last_error: str | None = None
        for delay in [0.0, *self.retry_policy.delays()]:
            if delay:
                time.sleep(delay)
            try:
                prepared = self.ingress.prepare_single(payload)
                self.watcher.register(prepared, envelope)
                now = utc_now().isoformat()
                self.store.upsert_delivery(
                    DeliveryAttempt(
                        request_id=prepared.request_id,
                        trace_id=prepared.trace_id,
                        source_id=runtime.config.id,
                        external_ref=envelope.external_ref,
                        tx_id=None,
                        status="prepared_registered",
                        prepared_action=prepared.prepared_action,
                        last_error=None,
                        updated_at=now,
                    )
                )

                if self.chain is None:
                    token = runtime.adapter.serialize_checkpoint(event)
                    self.store.set_checkpoint(runtime.config.id, token, now)
                    runtime.adapter.after_checkpoint_advanced(token)
                    return

                broadcast = self.chain.push_prepared_action(prepared.prepared_action)
                self.watcher.mark_included(prepared.request_id, broadcast.tx_id, broadcast.block_num)
                now = utc_now().isoformat()
                self.store.upsert_delivery(
                    DeliveryAttempt(
                        request_id=prepared.request_id,
                        trace_id=prepared.trace_id,
                        source_id=runtime.config.id,
                        external_ref=envelope.external_ref,
                        tx_id=broadcast.tx_id,
                        status="broadcast_included",
                        prepared_action=prepared.prepared_action,
                        last_error=None,
                        updated_at=now,
                    )
                )

                if self.config.denotary.wait_for_finality and self.receipt is not None and self.audit is not None:
                    self.watcher.wait_for_finalized(
                        prepared.request_id,
                        self.config.denotary.finality_timeout_sec,
                        self.config.denotary.finality_poll_interval_sec,
                    )
                    receipt = self.receipt.get_receipt(prepared.request_id)
                    audit_chain = self.audit.get_chain(prepared.request_id)
                    export_path = export_proof_bundle(
                        self.config.storage.proof_dir,
                        runtime.config.id,
                        prepared.request_id,
                        envelope,
                        prepared,
                        broadcast,
                        receipt,
                        audit_chain,
                    )
                    self.store.upsert_proof(
                        ProofArtifact(
                            request_id=prepared.request_id,
                            source_id=runtime.config.id,
                            receipt=receipt,
                            audit_chain=audit_chain,
                            export_path=export_path,
                            updated_at=utc_now().isoformat(),
                        )
                    )
                    now = utc_now().isoformat()
                    self.store.upsert_delivery(
                        DeliveryAttempt(
                            request_id=prepared.request_id,
                            trace_id=prepared.trace_id,
                            source_id=runtime.config.id,
                            external_ref=envelope.external_ref,
                            tx_id=broadcast.tx_id,
                            status="finalized_exported",
                            prepared_action=prepared.prepared_action,
                            last_error=None,
                            updated_at=now,
                        )
                    )

                token = runtime.adapter.serialize_checkpoint(event)
                self.store.set_checkpoint(runtime.config.id, token, now)
                runtime.adapter.after_checkpoint_advanced(token)
                return
            except Exception as exc:  # noqa: BLE001
                last_error = str(exc)
                try:
                    if "prepared" in locals():
                        self.watcher.mark_failed(
                            prepared.request_id,
                            "db_agent_delivery_failed",
                            {"source_id": runtime.config.id, "error": last_error},
                        )
                except Exception:
                    pass

        now = utc_now().isoformat()
        self.store.upsert_delivery(
            DeliveryAttempt(
                request_id=envelope.external_ref,
                trace_id=envelope.trace_id,
                source_id=runtime.config.id,
                external_ref=envelope.external_ref,
                tx_id=None,
                status="failed",
                prepared_action=None,
                last_error=last_error,
                updated_at=now,
            )
        )
        raise RuntimeError(last_error or "unknown delivery failure")

    def _process_batch(self, runtime: SourceRuntime, events) -> None:
        envelopes = [canonicalize_event(event) for event in events]
        payload = {
            "submitter": self.config.denotary.submitter,
            "external_ref": build_batch_external_ref(envelopes),
            "schema": {
                "id": self.config.denotary.schema_id,
                "version": "1.0.0",
                "active": True,
                "canonicalization_profile": "json-sorted-v1",
            },
            "policy": {
                "id": self.config.denotary.policy_id,
                "active": True,
                "allow_single": False,
                "allow_batch": True,
            },
            "items": [envelope.to_batch_item() for envelope in envelopes],
        }

        last_error: str | None = None
        prepared = None
        for delay in [0.0, *self.retry_policy.delays()]:
            if delay:
                time.sleep(delay)
            try:
                prepared = self.ingress.prepare_batch(payload)
                self.watcher.register(prepared, envelopes[0])
                now = utc_now().isoformat()
                self.store.upsert_delivery(
                    DeliveryAttempt(
                        request_id=prepared.request_id,
                        trace_id=prepared.trace_id,
                        source_id=runtime.config.id,
                        external_ref=payload["external_ref"],
                        tx_id=None,
                        status="prepared_registered",
                        prepared_action=prepared.prepared_action,
                        last_error=None,
                        updated_at=now,
                    )
                )

                if self.chain is None:
                    token = runtime.adapter.serialize_checkpoint(events[-1])
                    self.store.set_checkpoint(runtime.config.id, token, now)
                    runtime.adapter.after_checkpoint_advanced(token)
                    return

                broadcast = self.chain.push_prepared_action(prepared.prepared_action)
                self.watcher.mark_included(prepared.request_id, broadcast.tx_id, broadcast.block_num)
                now = utc_now().isoformat()
                self.store.upsert_delivery(
                    DeliveryAttempt(
                        request_id=prepared.request_id,
                        trace_id=prepared.trace_id,
                        source_id=runtime.config.id,
                        external_ref=payload["external_ref"],
                        tx_id=broadcast.tx_id,
                        status="broadcast_included",
                        prepared_action=prepared.prepared_action,
                        last_error=None,
                        updated_at=now,
                    )
                )

                if self.config.denotary.wait_for_finality and self.receipt is not None and self.audit is not None:
                    self.watcher.wait_for_finalized(
                        prepared.request_id,
                        self.config.denotary.finality_timeout_sec,
                        self.config.denotary.finality_poll_interval_sec,
                    )
                    receipt = self.receipt.get_receipt(prepared.request_id)
                    audit_chain = self.audit.get_chain(prepared.request_id)
                    export_path = export_batch_proof_bundle(
                        self.config.storage.proof_dir,
                        runtime.config.id,
                        prepared.request_id,
                        envelopes,
                        prepared,
                        broadcast,
                        receipt,
                        audit_chain,
                    )
                    self.store.upsert_proof(
                        ProofArtifact(
                            request_id=prepared.request_id,
                            source_id=runtime.config.id,
                            receipt=receipt,
                            audit_chain=audit_chain,
                            export_path=export_path,
                            updated_at=utc_now().isoformat(),
                        )
                    )
                    now = utc_now().isoformat()
                    self.store.upsert_delivery(
                        DeliveryAttempt(
                            request_id=prepared.request_id,
                            trace_id=prepared.trace_id,
                            source_id=runtime.config.id,
                            external_ref=payload["external_ref"],
                            tx_id=broadcast.tx_id,
                            status="finalized_exported",
                            prepared_action=prepared.prepared_action,
                            last_error=None,
                            updated_at=now,
                        )
                    )

                token = runtime.adapter.serialize_checkpoint(events[-1])
                self.store.set_checkpoint(runtime.config.id, token, now)
                runtime.adapter.after_checkpoint_advanced(token)
                return
            except Exception as exc:  # noqa: BLE001
                last_error = str(exc)
                try:
                    if prepared is not None:
                        self.watcher.mark_failed(
                            prepared.request_id,
                            "db_agent_batch_delivery_failed",
                            {"source_id": runtime.config.id, "error": last_error},
                        )
                except Exception:
                    pass
        raise RuntimeError(last_error or "unknown batch delivery failure")

    def reset_checkpoint(self, source_id: str) -> None:
        self.store.reset_checkpoint(source_id)

    def pause_source(self, source_id: str) -> None:
        self.store.set_source_paused(source_id, True, utc_now().isoformat())

    def resume_source(self, source_id: str) -> None:
        self.store.set_source_paused(source_id, False, utc_now().isoformat())

    def refresh_source(self, source_id: str | None = None) -> dict:
        results = []
        for runtime in self.runtimes():
            if source_id and runtime.config.id != source_id:
                continue
            summary = runtime.adapter.refresh_runtime()
            self.store.set_runtime_signature(runtime.config.id, runtime.adapter.runtime_signature(), utc_now().isoformat())
            results.append(summary)
        return {"agent_name": self.config.agent_name, "sources": results}

    def checkpoint_summary(self) -> list[dict[str, str]]:
        return [dict(item.__dict__) for item in self.store.list_checkpoints()]

    def _checkpoint_value(self, source_id: str) -> str | None:
        checkpoint = self.store.get_checkpoint(source_id)
        return checkpoint.token if checkpoint else None

    def _wait_for_activity(self, runtimes: list[SourceRuntime], interval_sec: float) -> None:
        waiting_runtimes = [
            runtime for runtime in runtimes if runtime.config.options.get("capture_mode") in {"trigger", "logical"}
        ]
        if not waiting_runtimes:
            time.sleep(interval_sec)
            return

        deadline = monotonic() + interval_sec
        slice_timeout = max(0.05, interval_sec / max(len(waiting_runtimes), 1))
        while monotonic() < deadline:
            for runtime in waiting_runtimes:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    return
                if runtime.adapter.wait_for_changes(min(slice_timeout, remaining)):
                    return

    def _ensure_runtime_ready(self, runtime: SourceRuntime) -> None:
        current_signature = runtime.adapter.runtime_signature()
        stored_signature = self.store.get_runtime_signature(runtime.config.id)
        if stored_signature == current_signature:
            return
        runtime.adapter.refresh_runtime()
        refreshed_signature = runtime.adapter.runtime_signature()
        self.store.set_runtime_signature(runtime.config.id, refreshed_signature, utc_now().isoformat())

    def _apply_runtime_retention(self, runtime: SourceRuntime) -> None:
        storage = self.config.storage
        if storage.delivery_retention > 0:
            self.store.prune_deliveries(runtime.config.id, storage.delivery_retention)
        if storage.dlq_retention > 0:
            self.store.prune_dlq(runtime.config.id, storage.dlq_retention)
        if storage.proof_retention > 0:
            removed = self.store.prune_proofs(runtime.config.id, storage.proof_retention)
            for row in removed:
                export_path = row.get("export_path")
                if not export_path:
                    continue
                try:
                    if os.path.exists(str(export_path)):
                        os.remove(str(export_path))
                except FileNotFoundError:
                    continue

    def _maybe_write_periodic_diagnostics_snapshot(self) -> str | None:
        interval_sec = self.config.storage.diagnostics_snapshot_interval_sec
        if interval_sec <= 0:
            return None
        now = monotonic()
        if self._last_diagnostics_snapshot_monotonic is not None and now - self._last_diagnostics_snapshot_monotonic < interval_sec:
            return None
        diagnostics = self.diagnostics()
        snapshot_path, _removed = export_diagnostics_snapshot(
            diagnostics,
            state_db=self.config.storage.state_db,
            source_id=None,
            retention=self.config.storage.diagnostics_snapshot_retention,
        )
        self._last_diagnostics_snapshot_monotonic = now
        return str(snapshot_path)
