from __future__ import annotations

import time
from dataclasses import dataclass
from time import monotonic

from denotary_db_agent.adapters.base import BaseAdapter
from denotary_db_agent.adapters.registry import build_adapter
from denotary_db_agent.canonical import build_batch_external_ref, canonicalize_event
from denotary_db_agent.checkpoint_store import CheckpointStore
from denotary_db_agent.config import AgentConfig, SourceConfig
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

    def runtimes(self) -> list[SourceRuntime]:
        return [
            SourceRuntime(config=item, adapter=build_adapter(item))
            for item in self.config.sources
            if item.enabled
        ]

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
        return {
            "agent_name": self.config.agent_name,
            "services": services,
            "sources": [
                {
                    "source_id": runtime.config.id,
                    "adapter": runtime.config.adapter,
                    "paused": self._is_paused(runtime.config.id),
                    "checkpoint": self._checkpoint_value(runtime.config.id),
                    "deliveries": len(self.store.list_deliveries(runtime.config.id)),
                    "proofs": len(self.store.list_proofs(runtime.config.id)),
                    "dlq": len(self.store.list_dlq(runtime.config.id)),
                }
                for runtime in self.runtimes()
            ],
        }

    def bootstrap(self, source_id: str | None = None) -> dict:
        results = []
        for runtime in self.runtimes():
            if source_id and runtime.config.id != source_id:
                continue
            results.append(runtime.adapter.bootstrap())
        return {"agent_name": self.config.agent_name, "sources": results}

    def inspect(self, source_id: str | None = None) -> dict:
        results = []
        for runtime in self.runtimes():
            if source_id and runtime.config.id != source_id:
                continue
            results.append(runtime.adapter.inspect())
        return {"agent_name": self.config.agent_name, "sources": results}

    def run_once(self) -> dict[str, int]:
        return self._run_runtimes_once(self.runtimes())

    def _run_runtimes_once(self, runtimes: list[SourceRuntime]) -> dict[str, int]:
        processed = 0
        failed = 0
        for runtime in runtimes:
            if self._is_paused(runtime.config.id):
                continue
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
                continue
            for event in events:
                try:
                    self._process_event(runtime, event)
                    processed += 1
                except Exception as exc:  # noqa: BLE001
                    failed += 1
                    self.store.push_dlq(runtime.config.id, str(exc), event_debug_dict(event), utc_now().isoformat())
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
