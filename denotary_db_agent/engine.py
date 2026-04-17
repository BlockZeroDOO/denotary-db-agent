from __future__ import annotations

import time
from dataclasses import dataclass

from denotary_db_agent.adapters.base import BaseAdapter
from denotary_db_agent.adapters.registry import build_adapter
from denotary_db_agent.canonical import canonicalize_event
from denotary_db_agent.checkpoint_store import CheckpointStore
from denotary_db_agent.config import AgentConfig, SourceConfig
from denotary_db_agent.models import DeliveryAttempt, event_debug_dict, utc_now
from denotary_db_agent.transport import IngressClient, WatcherClient


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
        self.watcher = WatcherClient(config.denotary.watcher_url, config.denotary.watcher_auth_token)
        self.retry_policy = RetryPolicy()

    def runtimes(self) -> list[SourceRuntime]:
        return [
            SourceRuntime(config=item, adapter=build_adapter(item))
            for item in self.config.sources
            if item.enabled
        ]

    def validate(self) -> list[dict[str, str]]:
        results: list[dict[str, str]] = []
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
                    "checkpoint": self._checkpoint_value(runtime.config.id),
                    "deliveries": len(self.store.list_deliveries(runtime.config.id)),
                    "dlq": len(self.store.list_dlq(runtime.config.id)),
                }
                for runtime in self.runtimes()
            ],
        }

    def run_once(self) -> dict[str, int]:
        processed = 0
        failed = 0
        for runtime in self.runtimes():
            checkpoint = self.store.get_checkpoint(runtime.config.id)
            runtime.adapter.resume_from_checkpoint(checkpoint)
            for event in runtime.adapter.read_snapshot(checkpoint):
                try:
                    self._process_event(runtime, event)
                    processed += 1
                except Exception as exc:  # noqa: BLE001
                    failed += 1
                    self.store.push_dlq(runtime.config.id, str(exc), event_debug_dict(event), utc_now().isoformat())
        return {"processed": processed, "failed": failed}

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
                token = runtime.adapter.serialize_checkpoint(event)
                now = utc_now().isoformat()
                self.store.set_checkpoint(runtime.config.id, token, now)
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
                return
            except Exception as exc:  # noqa: BLE001
                last_error = str(exc)

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

    def reset_checkpoint(self, source_id: str) -> None:
        self.store.reset_checkpoint(source_id)

    def checkpoint_summary(self) -> list[dict[str, str]]:
        return [dict(item.__dict__) for item in self.store.list_checkpoints()]

    def _checkpoint_value(self, source_id: str) -> str | None:
        checkpoint = self.store.get_checkpoint(source_id)
        return checkpoint.token if checkpoint else None
