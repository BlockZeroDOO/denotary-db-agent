from __future__ import annotations

import time
from typing import Any

from denotary_db_agent.canonical import build_batch_external_ref, canonicalize_event
from denotary_db_agent.checkpoint_store import CheckpointStore
from denotary_db_agent.config import AgentConfig
from denotary_db_agent.models import DeliveryAttempt, ProofArtifact, utc_now
from denotary_db_agent.source_runtime import SourceRuntime
from denotary_db_agent.transport import (
    AuditClient,
    BroadcastResult,
    IngressClient,
    PreparedRequest,
    ReceiptClient,
    WatcherClient,
    export_batch_proof_bundle,
    export_proof_bundle,
)


class NotarizationPipeline:
    def __init__(
        self,
        config: AgentConfig,
        store: CheckpointStore,
        ingress: IngressClient,
        watcher: WatcherClient,
        receipt: ReceiptClient | None,
        audit: AuditClient | None,
        chain: Any,
        retry_delays: list[float],
    ):
        self.config = config
        self.store = store
        self.ingress = ingress
        self.watcher = watcher
        self.receipt = receipt
        self.audit = audit
        self.chain = chain
        self.retry_delays = retry_delays

    def process_event(self, runtime: SourceRuntime, event: Any) -> None:
        envelope = canonicalize_event(event)
        payload = envelope.to_prepare_payload(
            submitter=self.config.denotary.submitter,
            schema_id=self.config.denotary.schema_id,
            policy_id=self.config.denotary.policy_id,
        )

        last_error: str | None = None
        existing_delivery = self.find_resumable_delivery(runtime.config.id, envelope.external_ref)
        prepared = self.prepared_from_delivery(existing_delivery, payload) if existing_delivery is not None else None
        if existing_delivery is not None and existing_delivery.get("status") == "finalized_exported":
            token = runtime.adapter.serialize_checkpoint(event)
            now = utc_now().isoformat()
            self.store.set_checkpoint(runtime.config.id, token, now)
            runtime.adapter.after_checkpoint_advanced(token)
            return

        for delay in [0.0, *self.retry_delays]:
            if delay:
                time.sleep(delay)
            try:
                if prepared is None:
                    prepared = self.ingress.prepare_single(payload)
                if existing_delivery is None:
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
                else:
                    now = utc_now().isoformat()

                if self.chain is None:
                    token = runtime.adapter.serialize_checkpoint(event)
                    self.store.set_checkpoint(runtime.config.id, token, now)
                    runtime.adapter.after_checkpoint_advanced(token)
                    return

                broadcast: BroadcastResult | None = None
                if existing_delivery is not None and existing_delivery.get("status") == "broadcast_included" and existing_delivery.get("tx_id"):
                    broadcast = BroadcastResult(
                        tx_id=str(existing_delivery["tx_id"]),
                        block_num=0,
                        raw={"reused": True},
                    )
                else:
                    try:
                        broadcast = self.chain.push_prepared_action(prepared.prepared_action)
                    except Exception as exc:  # noqa: BLE001
                        if self.is_duplicate_submit_error(exc):
                            self.recover_finalized_request(runtime, event, envelope, prepared, existing_delivery)
                            return
                        raise

                if broadcast.block_num > 0:
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
                    self.finalize_request(runtime, event, envelope, prepared, broadcast)
                    return

                token = runtime.adapter.serialize_checkpoint(event)
                self.store.set_checkpoint(runtime.config.id, token, now)
                runtime.adapter.after_checkpoint_advanced(token)
                return
            except Exception as exc:  # noqa: BLE001
                last_error = str(exc)
                try:
                    if prepared is not None:
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

    def process_batch(self, runtime: SourceRuntime, events: list[Any]) -> None:
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
        existing_delivery = self.find_resumable_delivery(runtime.config.id, payload["external_ref"])
        prepared = self.prepared_from_delivery(existing_delivery, payload) if existing_delivery is not None else None
        if existing_delivery is not None and existing_delivery.get("status") == "finalized_exported":
            token = runtime.adapter.serialize_checkpoint(events[-1])
            now = utc_now().isoformat()
            self.store.set_checkpoint(runtime.config.id, token, now)
            runtime.adapter.after_checkpoint_advanced(token)
            return

        for delay in [0.0, *self.retry_delays]:
            if delay:
                time.sleep(delay)
            try:
                if prepared is None:
                    prepared = self.ingress.prepare_batch(payload)
                if existing_delivery is None:
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
                else:
                    now = utc_now().isoformat()

                if self.chain is None:
                    token = runtime.adapter.serialize_checkpoint(events[-1])
                    self.store.set_checkpoint(runtime.config.id, token, now)
                    runtime.adapter.after_checkpoint_advanced(token)
                    return

                broadcast: BroadcastResult | None = None
                if existing_delivery is not None and existing_delivery.get("status") == "broadcast_included" and existing_delivery.get("tx_id"):
                    broadcast = BroadcastResult(
                        tx_id=str(existing_delivery["tx_id"]),
                        block_num=0,
                        raw={"reused": True},
                    )
                else:
                    try:
                        broadcast = self.chain.push_prepared_action(prepared.prepared_action)
                    except Exception as exc:  # noqa: BLE001
                        if self.is_duplicate_submit_error(exc):
                            self.recover_finalized_batch_request(runtime, events, envelopes, prepared, existing_delivery)
                            return
                        raise

                if broadcast.block_num > 0:
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
                    self.finalize_batch_request(runtime, events, envelopes, payload["external_ref"], prepared, broadcast)
                    return

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
        if prepared is not None:
            now = utc_now().isoformat()
            self.store.upsert_delivery(
                DeliveryAttempt(
                    request_id=prepared.request_id,
                    trace_id=prepared.trace_id,
                    source_id=runtime.config.id,
                    external_ref=payload["external_ref"],
                    tx_id=None,
                    status="failed",
                    prepared_action=prepared.prepared_action,
                    last_error=last_error,
                    updated_at=now,
                )
            )
        raise RuntimeError(last_error or "unknown batch delivery failure")

    @staticmethod
    def is_duplicate_submit_error(exc: Exception) -> bool:
        message = str(exc or "")
        return "duplicate request for submitter" in message or "duplicate batch request for submitter" in message

    def find_resumable_delivery(self, source_id: str, external_ref: str) -> dict[str, object | None] | None:
        candidates = [
            item
            for item in self.store.list_deliveries_by_external_ref(source_id, external_ref)
            if item.get("status") in {"prepared_registered", "broadcast_included", "finalized_exported"}
            and item.get("prepared_action") is not None
        ]
        if not candidates:
            return None
        status_priority = {"finalized_exported": 3, "broadcast_included": 2, "prepared_registered": 1}
        candidates.sort(
            key=lambda item: (
                status_priority.get(str(item.get("status") or ""), 0),
                str(item.get("updated_at") or ""),
                str(item.get("request_id") or ""),
            ),
            reverse=True,
        )
        return candidates[0]

    @staticmethod
    def prepared_from_delivery(delivery: dict[str, object | None], payload: dict[str, object]) -> PreparedRequest:
        prepared_action = dict(delivery.get("prepared_action") or {})
        data = dict(prepared_action.get("data") or {})
        return PreparedRequest(
            request_id=str(delivery["request_id"]),
            trace_id=str(delivery["trace_id"]),
            external_ref_hash=str(payload.get("external_ref") or ""),
            object_hash=str(data.get("object_hash")) if data.get("object_hash") is not None else None,
            root_hash=str(data.get("root_hash")) if data.get("root_hash") is not None else None,
            manifest_hash=str(data.get("manifest_hash")) if data.get("manifest_hash") is not None else None,
            leaf_count=int(data.get("leaf_count")) if data.get("leaf_count") is not None else None,
            verification_account=str(prepared_action.get("contract") or prepared_action.get("account") or ""),
            prepared_action=prepared_action,
            raw={"reused_delivery": True},
        )

    def finalize_request(
        self,
        runtime: SourceRuntime,
        event: Any,
        envelope: Any,
        prepared: PreparedRequest,
        broadcast: BroadcastResult | None,
    ) -> None:
        self.watcher.wait_for_finalized(
            prepared.request_id,
            self.config.denotary.finality_timeout_sec,
            self.config.denotary.finality_poll_interval_sec,
        )
        receipt = self.receipt.get_receipt(prepared.request_id)
        audit_chain = self.audit.get_chain(prepared.request_id)
        receipt_tx_id = str(receipt.get("tx_id") or (broadcast.tx_id if broadcast is not None else "") or "")
        receipt_block_num = int(receipt.get("block_num") or (broadcast.block_num if broadcast is not None else 0) or 0)
        export_path = export_proof_bundle(
            self.config.storage.proof_dir,
            runtime.config.id,
            prepared.request_id,
            envelope,
            prepared,
            BroadcastResult(tx_id=receipt_tx_id, block_num=receipt_block_num, raw=receipt),
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
                tx_id=receipt_tx_id or None,
                status="finalized_exported",
                prepared_action=prepared.prepared_action,
                last_error=None,
                updated_at=now,
            )
        )
        token = runtime.adapter.serialize_checkpoint(event)
        self.store.set_checkpoint(runtime.config.id, token, now)
        runtime.adapter.after_checkpoint_advanced(token)

    def finalize_batch_request(
        self,
        runtime: SourceRuntime,
        events: list[Any],
        envelopes: list[Any],
        external_ref: str,
        prepared: PreparedRequest,
        broadcast: BroadcastResult | None,
    ) -> None:
        self.watcher.wait_for_finalized(
            prepared.request_id,
            self.config.denotary.finality_timeout_sec,
            self.config.denotary.finality_poll_interval_sec,
        )
        receipt = self.receipt.get_receipt(prepared.request_id)
        audit_chain = self.audit.get_chain(prepared.request_id)
        receipt_tx_id = str(receipt.get("tx_id") or (broadcast.tx_id if broadcast is not None else "") or "")
        receipt_block_num = int(receipt.get("block_num") or (broadcast.block_num if broadcast is not None else 0) or 0)
        export_path = export_batch_proof_bundle(
            self.config.storage.proof_dir,
            runtime.config.id,
            prepared.request_id,
            envelopes,
            prepared,
            BroadcastResult(tx_id=receipt_tx_id, block_num=receipt_block_num, raw=receipt),
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
                external_ref=external_ref,
                tx_id=receipt_tx_id or None,
                status="finalized_exported",
                prepared_action=prepared.prepared_action,
                last_error=None,
                updated_at=now,
            )
        )
        token = runtime.adapter.serialize_checkpoint(events[-1])
        self.store.set_checkpoint(runtime.config.id, token, now)
        runtime.adapter.after_checkpoint_advanced(token)

    def recover_finalized_request(
        self,
        runtime: SourceRuntime,
        event: Any,
        envelope: Any,
        prepared: PreparedRequest,
        existing_delivery: dict[str, object | None] | None,
    ) -> None:
        if not self.config.denotary.wait_for_finality or self.receipt is None or self.audit is None:
            raise RuntimeError("duplicate request for submitter without finality recovery path")
        existing_broadcast = None
        if existing_delivery is not None and existing_delivery.get("tx_id"):
            existing_broadcast = BroadcastResult(
                tx_id=str(existing_delivery["tx_id"]),
                block_num=0,
                raw={"recovered": True},
            )
        self.finalize_request(runtime, event, envelope, prepared, existing_broadcast)

    def recover_finalized_batch_request(
        self,
        runtime: SourceRuntime,
        events: list[Any],
        envelopes: list[Any],
        prepared: PreparedRequest,
        existing_delivery: dict[str, object | None] | None,
    ) -> None:
        if not self.config.denotary.wait_for_finality or self.receipt is None or self.audit is None:
            raise RuntimeError("duplicate batch request for submitter without finality recovery path")
        existing_broadcast = None
        if existing_delivery is not None and existing_delivery.get("tx_id"):
            existing_broadcast = BroadcastResult(
                tx_id=str(existing_delivery["tx_id"]),
                block_num=0,
                raw={"recovered": True},
            )
        self.finalize_batch_request(
            runtime,
            events,
            envelopes,
            build_batch_external_ref(envelopes),
            prepared,
            existing_broadcast,
        )
