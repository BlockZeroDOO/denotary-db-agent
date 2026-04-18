from __future__ import annotations

import os
import time
import urllib.error
import urllib.request
from pathlib import Path
from time import monotonic

import denotary_db_agent.pipeline as pipeline_module
from denotary_db_agent.canonical import canonicalize_event
from denotary_db_agent.checkpoint_store import CheckpointStore
from denotary_db_agent.config import AgentConfig
from denotary_db_agent.diagnostics_snapshots import export_diagnostics_snapshot
from denotary_db_agent.models import DeliveryAttempt, ProofArtifact, event_debug_dict, utc_now
from denotary_db_agent.pipeline import NotarizationPipeline
from denotary_db_agent.source_runtime import SourceRuntime, SourceRuntimeRegistry
from denotary_db_agent.transport import (
    AuditClient,
    IngressClient,
    ReceiptClient,
    WatcherClient,
    build_chain_client,
    derive_public_key_candidates,
    export_batch_proof_bundle,
    export_proof_bundle,
    inspect_secret_file_permissions,
    resolve_private_key,
)


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
        self.chain = build_chain_client(config.denotary)
        self.retry_policy = RetryPolicy()
        self.runtime_registry = SourceRuntimeRegistry(config)
        self.pipeline = NotarizationPipeline(
            config=config,
            store=self.store,
            ingress=self.ingress,
            watcher=self.watcher,
            receipt=self.receipt,
            audit=self.audit,
            chain=self.chain,
            retry_delays=self.retry_policy.delays(),
        )
        self._last_diagnostics_snapshot_monotonic: float | None = None

    def close(self) -> None:
        self.runtime_registry.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    def runtimes(self) -> list[SourceRuntime]:
        return self.runtime_registry.runtimes()

    def _sync_pipeline_dependencies(self) -> None:
        pipeline_module.canonicalize_event = canonicalize_event
        self.pipeline.ingress = self.ingress
        self.pipeline.watcher = self.watcher
        self.pipeline.receipt = self.receipt
        self.pipeline.audit = self.audit
        self.pipeline.chain = self.chain
        self.pipeline.retry_delays = self.retry_policy.delays()

    def _is_paused(self, source_id: str) -> bool:
        return self.store.is_source_paused(source_id)

    def validate(self) -> list[dict[str, object]]:
        results: list[dict[str, object]] = []
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
                    "capture_modes": [],
                    "bootstrap_requirements": [],
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
                    "capture_modes": list(capabilities.capture_modes),
                    "bootstrap_requirements": list(capabilities.bootstrap_requirements),
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

    def doctor(self, source_id: str | None = None) -> dict:
        services = {
            "ingress": self._probe_service_url(self.config.denotary.ingress_url, "/healthz"),
            "watcher": self._probe_service_url(self.config.denotary.watcher_url, "/healthz"),
            "receipt": self._probe_service_url(self.config.denotary.receipt_url, "/healthz"),
            "audit": self._probe_service_url(self.config.denotary.audit_url, "/healthz"),
            "chain_rpc": self._probe_chain_rpc(),
        }
        signer = self._doctor_signer()
        sources = self._doctor_sources(source_id)
        config_checks = self._doctor_config_paths()

        all_severities = [config_checks["severity"], signer["severity"]]
        all_severities.extend(item["severity"] for item in services.values())
        all_severities.extend(item["severity"] for item in sources)
        overall_severity = self._max_severity(all_severities)

        warnings: list[str] = []
        errors: list[str] = []
        warnings.extend(config_checks.get("warnings", []))
        warnings.extend(signer.get("warnings", []))
        errors.extend(signer.get("errors", []))
        for item in services.values():
            warnings.extend(item.get("warnings", []))
            errors.extend(item.get("errors", []))
        for item in sources:
            warnings.extend(item.get("warnings", []))
            errors.extend(item.get("errors", []))

        return {
            "agent_name": self.config.agent_name,
            "overall": {
                "severity": overall_severity,
                "ok": overall_severity == "healthy",
                "source_count": len(sources),
                "warning_count": len(warnings),
                "error_count": len(errors),
            },
            "config": config_checks,
            "services": services,
            "signer": signer,
            "sources": sources,
            "warnings": warnings,
            "errors": errors,
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

    def report(self, source_id: str | None = None) -> dict:
        doctor = self.doctor(source_id)
        metrics = self.metrics(source_id)
        diagnostics = self.diagnostics(source_id)
        status = self.status()
        if source_id:
            status = {
                "agent_name": status["agent_name"],
                "sources": [item for item in status["sources"] if str(item.get("source_id")) == source_id],
            }
        return {
            "agent_name": self.config.agent_name,
            "generated_at": utc_now().isoformat(),
            "source_filter": source_id,
            "doctor": doctor,
            "metrics": metrics,
            "diagnostics": diagnostics,
            "status": status,
        }

    def _doctor_config_paths(self) -> dict[str, object]:
        state_db = Path(self.config.storage.state_db).expanduser()
        proof_dir = Path(self.config.storage.proof_dir).expanduser()
        state_parent = state_db.parent
        warnings: list[str] = []
        if not state_parent.exists():
            warnings.append(f"state_db parent directory does not exist yet: {state_parent}")
        if not proof_dir.exists():
            warnings.append(f"proof_dir does not exist yet: {proof_dir}")
        return {
            "severity": "degraded" if warnings else "healthy",
            "state_db": str(state_db),
            "state_db_parent": str(state_parent),
            "state_db_parent_exists": state_parent.exists(),
            "proof_dir": str(proof_dir),
            "proof_dir_exists": proof_dir.exists(),
            "wait_for_finality": self.config.denotary.wait_for_finality,
            "warnings": warnings,
        }

    def _probe_service_url(self, base_url: str, probe_path: str) -> dict[str, object]:
        if not base_url:
            return {
                "configured": False,
                "severity": "degraded",
                "reachable": False,
                "warnings": ["service URL is not configured"],
                "errors": [],
            }
        url = f"{base_url.rstrip('/')}{probe_path}"
        request = urllib.request.Request(url, method="GET")
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                status_code = int(response.status)
            return {
                "configured": True,
                "reachable": True,
                "severity": "healthy",
                "probe_url": url,
                "status_code": status_code,
                "warnings": [],
                "errors": [],
            }
        except urllib.error.HTTPError as exc:
            return {
                "configured": True,
                "reachable": True,
                "severity": "healthy",
                "probe_url": url,
                "status_code": int(exc.code),
                "warnings": [f"probe endpoint returned HTTP {exc.code} but the service is reachable"],
                "errors": [],
            }
        except Exception as exc:  # noqa: BLE001
            return {
                "configured": True,
                "reachable": False,
                "severity": "critical",
                "probe_url": url,
                "warnings": [],
                "errors": [str(exc)],
            }

    def _probe_chain_rpc(self) -> dict[str, object]:
        if not self.config.denotary.chain_rpc_url:
            return {
                "configured": False,
                "severity": "critical",
                "reachable": False,
                "warnings": [],
                "errors": ["chain_rpc_url is not configured"],
            }
        if self.chain is not None:
            try:
                info = self.chain.health()
                return {
                    "configured": True,
                    "reachable": True,
                    "severity": "healthy",
                    "url": self.config.denotary.chain_rpc_url,
                    "server_version": info.get("server_version_string") or info.get("server_version"),
                    "chain_id": info.get("chain_id"),
                    "warnings": [],
                    "errors": [],
                }
            except Exception as exc:  # noqa: BLE001
                return {
                    "configured": True,
                    "reachable": False,
                    "severity": "critical",
                    "url": self.config.denotary.chain_rpc_url,
                    "warnings": [],
                    "errors": [str(exc)],
                }
        return self._probe_service_url(self.config.denotary.chain_rpc_url, "/v1/chain/get_info")

    def _doctor_signer(self) -> dict[str, object]:
        warnings: list[str] = []
        errors: list[str] = []
        permission = self.config.denotary.submitter_permission
        backend = self.config.denotary.broadcast_backend
        key_info = resolve_private_key(self.config.denotary)
        effective_backend = backend
        if backend == "auto":
            effective_backend = "private_key_env" if key_info["private_key"] else "private_key"
        private_key_present = bool(key_info["private_key"])
        env_file = str(key_info.get("env_file") or "")
        env_file_permissions = inspect_secret_file_permissions(env_file) if env_file else {
            "path": "",
            "exists": False,
            "checked": False,
            "ok": None,
            "platform": os.name,
            "mode_octal": None,
            "severity": "unknown",
            "issues": [],
        }
        wallet_command = list(self.config.denotary.wallet_command or [])
        wallet_command_effective = wallet_command or ["cleos"]
        chain_ready = bool(self.config.denotary.chain_rpc_url)
        if permission in {"owner", "active"}:
            warnings.append(f"submitter_permission is {permission}; a dedicated hot permission such as dnanchor is recommended")
        wallet_probe: dict[str, object] | None = None
        signer_material_ready = False
        if effective_backend == "private_key":
            if not bool(self.config.denotary.submitter_private_key.strip()):
                errors.append("submitter_private_key is not configured")
            else:
                signer_material_ready = True
        elif effective_backend == "private_key_env":
            env_var = str(key_info.get("env_var") or "")
            if env_file and not bool(key_info.get("env_file_exists")):
                errors.append(f"env_file does not exist: {env_file}")
            if not private_key_present:
                if env_file:
                    errors.append(f"private key env var {env_var} was not found in env_file or process environment")
                else:
                    errors.append(f"private key env var {env_var} was not found in process environment")
            else:
                signer_material_ready = True
                if env_file:
                    if bool(env_file_permissions.get("checked")):
                        permission_message = (
                            f"env_file permissions are too broad ({env_file_permissions.get('mode_octal')}); "
                            "expected 0600 or stricter for a hot key secret"
                        )
                        if env_file_permissions.get("severity") == "critical":
                            errors.append(permission_message)
                        elif env_file_permissions.get("severity") == "degraded":
                            warnings.append(permission_message)
                    elif not bool(env_file_permissions.get("exists")):
                        errors.append(f"env_file does not exist: {env_file}")
                else:
                    warnings.append("hot key is loaded from process environment; prefer an env_file or secret mount with restricted access")
        elif effective_backend == "cleos_wallet":
            if self.chain is None:
                errors.append("wallet-backed broadcaster is not configured")
            else:
                warnings.append("wallet-backed broadcaster depends on an unlocked cleos wallet on the local host")
                if hasattr(self.chain, "probe_wallet"):
                    try:
                        wallet_probe = self.chain.probe_wallet()
                    except Exception as exc:  # noqa: BLE001
                        wallet_probe = {"ok": False, "error": str(exc), "command": wallet_command_effective}
                    if not wallet_probe.get("ok", False):
                        errors.append(f"wallet command probe failed: {wallet_probe.get('error')}")
                    elif not wallet_probe.get("has_unlocked_wallet", False):
                        errors.append("no unlocked cleos wallet is available for wallet-backed broadcasting")
                    else:
                        signer_material_ready = True
                else:
                    signer_material_ready = True
        else:
            errors.append(f"unsupported broadcast backend: {effective_backend}")
        if not chain_ready:
            errors.append("chain_rpc_url is not configured")

        account_exists = False
        permission_exists = False
        billing_account_exists = False
        account_error = ""
        permission_public_keys: list[str] = []
        permission_accounts: list[str] = []
        permission_waits: list[int] = []
        permission_threshold: int | None = None
        permission_is_minimal_hot_key: bool | None = None
        derived_public_keys: dict[str, str] = {}
        private_key_matches_permission: bool | None = None
        if self.chain is not None:
            try:
                account = self.chain.get_account(self.config.denotary.submitter)
                account_exists = True
                permissions = account.get("permissions") or []
                permission_auth = next((item for item in permissions if str(item.get("perm_name") or "") == permission), None)
                permission_exists = permission_auth is not None
                if permission_auth is not None:
                    required_auth = permission_auth.get("required_auth") or permission_auth.get("auth") or {}
                    permission_threshold = int(required_auth.get("threshold") or 1)
                    permission_public_keys = [
                        str(item.get("key") or "")
                        for item in (required_auth.get("keys") or [])
                        if str(item.get("key") or "")
                    ]
                    permission_accounts = [
                        str((item.get("permission") or {}).get("actor") or "")
                        + "@"
                        + str((item.get("permission") or {}).get("permission") or "")
                        for item in (required_auth.get("accounts") or [])
                        if str((item.get("permission") or {}).get("actor") or "")
                        and str((item.get("permission") or {}).get("permission") or "")
                    ]
                    permission_waits = [
                        int(item.get("wait_sec") or 0)
                        for item in (required_auth.get("waits") or [])
                        if int(item.get("wait_sec") or 0) > 0
                    ]
                    permission_is_minimal_hot_key = (
                        permission_threshold == 1
                        and len(permission_public_keys) == 1
                        and not permission_accounts
                        and not permission_waits
                    )
                    if permission_threshold != 1:
                        warnings.append(
                            f"submitter permission {self.config.denotary.submitter}@{permission} uses threshold "
                            f"{permission_threshold}; threshold 1 is recommended for a dedicated hot permission"
                        )
                    if len(permission_public_keys) > 1:
                        warnings.append(
                            f"submitter permission {self.config.denotary.submitter}@{permission} has "
                            f"{len(permission_public_keys)} keys; a single dedicated hot key is recommended"
                        )
                    if permission_accounts:
                        warnings.append(
                            f"submitter permission {self.config.denotary.submitter}@{permission} delegates through "
                            f"{len(permission_accounts)} linked account permission(s); avoid linked accounts on the hot runtime permission"
                        )
                    if permission_waits:
                        warnings.append(
                            f"submitter permission {self.config.denotary.submitter}@{permission} has delayed wait entries; "
                            "a direct single-key hot permission is recommended"
                        )
                if not permission_exists:
                    errors.append(f"submitter permission {self.config.denotary.submitter}@{permission} does not exist on chain")
            except Exception as exc:  # noqa: BLE001
                account_error = str(exc)
                errors.append(f"unable to load submitter account {self.config.denotary.submitter}: {exc}")
            try:
                self.chain.get_account(self.config.denotary.billing_account)
                billing_account_exists = True
            except Exception as exc:  # noqa: BLE001
                errors.append(f"unable to load billing account {self.config.denotary.billing_account}: {exc}")

        if signer_material_ready and effective_backend in {"private_key", "private_key_env"} and private_key_present:
            try:
                derived_public_keys = derive_public_key_candidates(str(key_info.get("private_key") or ""))
            except Exception as exc:  # noqa: BLE001
                errors.append(f"unable to derive public key from configured hot key: {exc}")
            else:
                if account_exists and permission_exists and permission_public_keys:
                    private_key_matches_permission = any(
                        candidate in permission_public_keys for candidate in derived_public_keys.values()
                    )
                    if not private_key_matches_permission:
                        errors.append(
                            f"configured hot key does not match any key on submitter permission "
                            f"{self.config.denotary.submitter}@{permission}"
                        )

        severity = "healthy"
        if errors:
            severity = "critical"
        elif warnings:
            severity = "degraded"

        return {
            "severity": severity,
            "submitter": self.config.denotary.submitter,
            "permission": permission,
            "broadcast_backend": backend,
            "effective_broadcast_backend": effective_backend,
            "billing_account": self.config.denotary.billing_account,
            "schema_id": self.config.denotary.schema_id,
            "policy_id": self.config.denotary.policy_id,
            "private_key_present": private_key_present,
            "private_key_source": key_info.get("source") or None,
            "private_key_env": key_info.get("env_var") or None,
            "env_file": env_file or None,
            "env_file_exists": bool(key_info.get("env_file_exists")),
            "env_file_permissions_checked": bool(env_file_permissions.get("checked")),
            "env_file_permissions_ok": env_file_permissions.get("ok"),
            "env_file_permission_severity": env_file_permissions.get("severity"),
            "env_file_mode_octal": env_file_permissions.get("mode_octal"),
            "env_file_permission_issues": list(env_file_permissions.get("issues") or []),
            "wallet_command": wallet_command_effective,
            "wallet_probe": wallet_probe,
            "chain_rpc_configured": chain_ready,
            "uses_recommended_hot_permission": permission not in {"owner", "active"},
            "account_exists": account_exists,
            "permission_exists": permission_exists,
            "billing_account_exists": billing_account_exists,
            "permission_threshold": permission_threshold,
            "permission_public_keys": permission_public_keys,
            "permission_account_links": permission_accounts,
            "permission_waits": permission_waits,
            "permission_is_minimal_hot_key": permission_is_minimal_hot_key,
            "derived_public_keys": derived_public_keys,
            "private_key_matches_permission": private_key_matches_permission,
            "broadcast_ready": (not errors)
            and chain_ready
            and signer_material_ready
            and (self.chain is None or (account_exists and permission_exists)),
            "account_error": account_error or None,
            "warnings": warnings,
            "errors": errors,
        }

    def _doctor_sources(self, source_id: str | None) -> list[dict[str, object]]:
        results: list[dict[str, object]] = []
        for runtime in self.runtimes():
            if source_id and runtime.config.id != source_id:
                continue
            warnings: list[str] = []
            errors: list[str] = []
            inspect_payload: dict[str, object] = {}
            connectivity_ok = False
            try:
                runtime.adapter.validate_connection()
                connectivity_ok = True
            except Exception as exc:  # noqa: BLE001
                errors.append(str(exc))
            if connectivity_ok:
                try:
                    inspect_payload = runtime.adapter.inspect()
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"inspect failed: {exc}")
            capture_mode = inspect_payload.get("capture_mode") if inspect_payload else runtime.config.options.get("capture_mode", "watermark")
            tracked_tables = inspect_payload.get("tracked_tables") if inspect_payload else []
            cdc = inspect_payload.get("cdc") if inspect_payload else None
            if isinstance(cdc, dict):
                warnings.extend(self._build_source_health_warnings(runtime, cdc))
            severity = "healthy"
            if errors:
                severity = "critical"
            elif warnings:
                severity = "degraded"
            results.append(
                {
                    "source_id": runtime.config.id,
                    "adapter": runtime.config.adapter,
                    "paused": self._is_paused(runtime.config.id),
                    "capture_mode": capture_mode,
                    "connectivity_ok": connectivity_ok,
                    "inspect_ok": connectivity_ok and not errors,
                    "tracked_table_count": len(tracked_tables) if isinstance(tracked_tables, list) else 0,
                    "severity": severity,
                    "warnings": warnings,
                    "errors": errors,
                }
            )
        return results

    def _severity_rank(self, severity: str) -> int:
        return {"healthy": 0, "degraded": 1, "critical": 2, "error": 3}.get(severity, 3)

    def _max_severity(self, severities: list[str]) -> str:
        if not severities:
            return "healthy"
        return max(severities, key=self._severity_rank)

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
            events = list(runtime.adapter.iter_events(checkpoint))
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
        self._sync_pipeline_dependencies()
        self.pipeline.process_event(runtime, event)

    def _is_duplicate_submit_error(self, exc: Exception) -> bool:
        return self.pipeline.is_duplicate_submit_error(exc)

    def _find_resumable_delivery(self, source_id: str, external_ref: str) -> dict[str, object | None] | None:
        return self.pipeline.find_resumable_delivery(source_id, external_ref)

    def _prepared_from_delivery(self, delivery: dict[str, object | None], payload: dict[str, object]) -> PreparedRequest:
        return self.pipeline.prepared_from_delivery(delivery, payload)

    def _finalize_request(
        self,
        runtime: SourceRuntime,
        event,
        envelope,
        prepared: PreparedRequest,
        broadcast: BroadcastResult | None,
    ) -> None:
        self.pipeline.finalize_request(runtime, event, envelope, prepared, broadcast)

    def _recover_finalized_request(
        self,
        runtime: SourceRuntime,
        event,
        envelope,
        prepared: PreparedRequest,
        existing_delivery: dict[str, object | None] | None,
    ) -> None:
        self.pipeline.recover_finalized_request(runtime, event, envelope, prepared, existing_delivery)

    def _process_batch(self, runtime: SourceRuntime, events) -> None:
        self._sync_pipeline_dependencies()
        self.pipeline.process_batch(runtime, events)

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
            runtime for runtime in runtimes if runtime.adapter.should_wait_for_activity()
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
