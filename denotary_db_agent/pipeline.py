from __future__ import annotations

import time
from typing import Any

from denotary_db_agent.canonical import build_batch_external_ref, canonicalize_event
from denotary_db_agent.checkpoint_store import CheckpointStore
from denotary_db_agent.config import AgentConfig
from denotary_db_agent.models import CanonicalEnvelope, ChangeEvent, DeliveryAttempt, ProofArtifact, event_debug_dict, utc_now
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

    @staticmethod
    def _prepared_action_name(prepared_action: dict[str, Any]) -> str:
        return str(prepared_action.get("action") or prepared_action.get("name") or "")

    @staticmethod
    def _prepared_action_contract(prepared_action: dict[str, Any]) -> str:
        return str(prepared_action.get("contract") or prepared_action.get("account") or "")

    @classmethod
    def _validate_prepared_action_common(
        cls,
        prepared: PreparedRequest,
        payload: dict[str, object],
        expected_action: str,
    ) -> dict[str, Any]:
        prepared_action = dict(prepared.prepared_action or {})
        data = prepared_action.get("data")
        if not isinstance(data, dict):
            raise RuntimeError("prepared_action.data must be an object")
        actual_contract = cls._prepared_action_contract(prepared_action)
        if actual_contract != str(prepared.verification_account or ""):
            raise RuntimeError("prepared_action contract does not match verification_account")
        actual_action = cls._prepared_action_name(prepared_action)
        if actual_action != expected_action:
            raise RuntimeError(f"prepared_action action mismatch: expected {expected_action}, got {actual_action or 'empty'}")
        if str(prepared.external_ref_hash or "") != str(payload.get("external_ref") or ""):
            raise RuntimeError("prepared_action external_ref_hash does not match the locally expected external_ref")
        submitter = str(payload.get("submitter") or "")
        schema = payload.get("schema") if isinstance(payload.get("schema"), dict) else {}
        policy = payload.get("policy") if isinstance(payload.get("policy"), dict) else {}
        if str(data.get("submitter") or "") != submitter:
            raise RuntimeError("prepared_action submitter does not match the locally expected submitter")
        if int(data.get("schema_id") or 0) != int(schema.get("id") or 0):
            raise RuntimeError("prepared_action schema_id does not match the locally expected schema id")
        if int(data.get("policy_id") or 0) != int(policy.get("id") or 0):
            raise RuntimeError("prepared_action policy_id does not match the locally expected policy id")
        if str(data.get("external_ref") or "") != str(payload.get("external_ref") or ""):
            raise RuntimeError("prepared_action external_ref does not match the locally expected external_ref")
        return data

    @classmethod
    def validate_prepared_single(cls, prepared: PreparedRequest, payload: dict[str, object]) -> None:
        data = cls._validate_prepared_action_common(prepared, payload, "submit")
        expected_object_hash = str(prepared.object_hash or "")
        if not expected_object_hash:
            raise RuntimeError("prepared single request is missing object_hash")
        if str(data.get("object_hash") or "") != expected_object_hash:
            raise RuntimeError("prepared_action object_hash does not match the prepared single request")

    @classmethod
    def validate_prepared_batch(cls, prepared: PreparedRequest, payload: dict[str, object]) -> None:
        data = cls._validate_prepared_action_common(prepared, payload, "submitroot")
        expected_root_hash = str(prepared.root_hash or "")
        expected_manifest_hash = str(prepared.manifest_hash or "")
        expected_leaf_count = prepared.leaf_count
        if not expected_root_hash or not expected_manifest_hash or expected_leaf_count is None:
            raise RuntimeError("prepared batch request is missing root_hash, manifest_hash, or leaf_count")
        if str(data.get("root_hash") or "") != expected_root_hash:
            raise RuntimeError("prepared_action root_hash does not match the prepared batch request")
        if str(data.get("manifest_hash") or "") != expected_manifest_hash:
            raise RuntimeError("prepared_action manifest_hash does not match the prepared batch request")
        if int(data.get("leaf_count") or -1) != int(expected_leaf_count):
            raise RuntimeError("prepared_action leaf_count does not match the prepared batch request")

    def require_chain_ready(self) -> None:
        if self.chain is None:
            raise RuntimeError("live broadcast path is not ready: chain signer/broadcaster is not configured")

    def process_event(self, runtime: SourceRuntime, event: Any) -> None:
        envelope = canonicalize_event(event)
        payload = envelope.to_prepare_payload(
            submitter=self.config.denotary.submitter,
            schema_id=self.config.denotary.schema_id,
            policy_id=self.config.denotary.policy_id,
        )
        event_payload = event_debug_dict(event)

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
            broadcast: BroadcastResult | None = None
            try:
                self.require_chain_ready()
                if prepared is None:
                    prepared = self.ingress.prepare_single(payload)
                self.validate_prepared_single(prepared, payload)
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
                            event_payload=event_payload,
                        )
                    )
                    existing_delivery = {
                        "request_id": prepared.request_id,
                        "trace_id": prepared.trace_id,
                        "source_id": runtime.config.id,
                        "external_ref": envelope.external_ref,
                        "tx_id": None,
                        "status": "prepared_registered",
                        "prepared_action": prepared.prepared_action,
                        "event_payload": event_payload,
                        "last_error": None,
                        "updated_at": now,
                    }
                else:
                    now = utc_now().isoformat()

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
                            event_payload=event_payload,
                        )
                    )
                    existing_delivery = {
                        "request_id": prepared.request_id,
                        "trace_id": prepared.trace_id,
                        "source_id": runtime.config.id,
                        "external_ref": envelope.external_ref,
                        "tx_id": broadcast.tx_id,
                        "status": "broadcast_included",
                        "prepared_action": prepared.prepared_action,
                        "event_payload": event_payload,
                        "last_error": None,
                        "updated_at": now,
                    }

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
                    if prepared is not None and self.should_mark_failed(existing_delivery, broadcast, last_error):
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
            broadcast: BroadcastResult | None = None
            try:
                self.require_chain_ready()
                if prepared is None:
                    prepared = self.ingress.prepare_batch(payload)
                self.validate_prepared_batch(prepared, payload)
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
                    existing_delivery = {
                        "request_id": prepared.request_id,
                        "trace_id": prepared.trace_id,
                        "source_id": runtime.config.id,
                        "external_ref": payload["external_ref"],
                        "tx_id": None,
                        "status": "prepared_registered",
                        "prepared_action": prepared.prepared_action,
                        "last_error": None,
                        "updated_at": now,
                    }
                else:
                    now = utc_now().isoformat()

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
                    existing_delivery = {
                        "request_id": prepared.request_id,
                        "trace_id": prepared.trace_id,
                        "source_id": runtime.config.id,
                        "external_ref": payload["external_ref"],
                        "tx_id": broadcast.tx_id,
                        "status": "broadcast_included",
                        "prepared_action": prepared.prepared_action,
                        "last_error": None,
                        "updated_at": now,
                    }

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
                    if prepared is not None and self.should_mark_failed(existing_delivery, broadcast, last_error):
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

    @staticmethod
    def is_retryable_finality_error(error_text: str) -> bool:
        text = str(error_text or "").lower()
        if not text:
            return False
        retryable_markers = (
            "http request failed: 502",
            "http request failed: 503",
            "http request failed: 504",
            "rpc call failed:",
            "timed out",
            "did not finalize in time",
            "temporarily unavailable",
            "connection reset",
            "connection aborted",
            "connection refused",
        )
        return any(marker in text for marker in retryable_markers)

    @classmethod
    def should_mark_failed(
        cls,
        existing_delivery: dict[str, object | None] | None,
        broadcast: BroadcastResult | None,
        error_text: str,
    ) -> bool:
        if not cls.is_retryable_finality_error(error_text):
            return True
        if broadcast is not None and broadcast.block_num > 0:
            return False
        if existing_delivery is None:
            return True
        status = str(existing_delivery.get("status") or "")
        if status == "broadcast_included" and existing_delivery.get("tx_id"):
            return False
        return True

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
        receipt, audit_chain = self.wait_for_finalized_or_recover(prepared.request_id)
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

    @staticmethod
    def event_from_payload(payload: dict[str, Any]) -> ChangeEvent:
        event_payload = dict(payload)
        event_payload.pop("identity_key", None)
        return ChangeEvent(**event_payload)

    @staticmethod
    def envelope_from_event(event: ChangeEvent, trace_id: str) -> CanonicalEnvelope:
        envelope = canonicalize_event(event)
        envelope.trace_id = trace_id
        return envelope

    def finalize_batch_request(
        self,
        runtime: SourceRuntime,
        events: list[Any],
        envelopes: list[Any],
        external_ref: str,
        prepared: PreparedRequest,
        broadcast: BroadcastResult | None,
    ) -> None:
        receipt, audit_chain = self.wait_for_finalized_or_recover(prepared.request_id)
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

    def wait_for_finalized_or_recover(self, request_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
        try:
            self.watcher.wait_for_finalized(
                request_id,
                self.config.denotary.finality_timeout_sec,
                self.config.denotary.finality_poll_interval_sec,
            )
        except Exception as exc:  # noqa: BLE001
            if not self.is_retryable_finality_error(str(exc)):
                raise
            receipt = self.receipt.get_receipt(request_id)
            if not self.receipt_is_finalized(receipt):
                raise
            audit_chain = self.audit.get_chain(request_id)
            return receipt, audit_chain

        receipt = self.receipt.get_receipt(request_id)
        audit_chain = self.audit.get_chain(request_id)
        return receipt, audit_chain

    @staticmethod
    def receipt_is_finalized(receipt: dict[str, Any]) -> bool:
        trust_state = str(receipt.get("trust_state") or "").lower()
        if trust_state in {"finalized_verified", "finalized"}:
            return True
        if receipt.get("finality_flag") is True:
            return True
        if receipt.get("finalized_at"):
            return True
        return False

    def reconcile_delivery(self, runtime: SourceRuntime, delivery: dict[str, object | None]) -> bool:
        status = str(delivery.get("status") or "")
        if status not in {"prepared_registered", "broadcast_included"}:
            return False
        prepared_action = delivery.get("prepared_action")
        event_payload = delivery.get("event_payload")
        if not isinstance(prepared_action, dict) or not isinstance(event_payload, dict):
            return False
        event = self.event_from_payload(event_payload)
        envelope = self.envelope_from_event(event, str(delivery.get("trace_id") or ""))
        payload = envelope.to_prepare_payload(
            submitter=self.config.denotary.submitter,
            schema_id=self.config.denotary.schema_id,
            policy_id=self.config.denotary.policy_id,
        )
        prepared = self.prepared_from_delivery(delivery, payload)
        broadcast = None
        if delivery.get("tx_id"):
            broadcast = BroadcastResult(
                tx_id=str(delivery["tx_id"]),
                block_num=0,
                raw={"reconciled_delivery": True},
            )
        self.finalize_request(runtime, event, envelope, prepared, broadcast)
        return True
