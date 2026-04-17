from __future__ import annotations

from collections.abc import Iterable

from denotary_db_agent.adapters.base import AdapterCapabilities, BaseAdapter
from denotary_db_agent.models import ChangeEvent, SourceCheckpoint


class OracleAdapter(BaseAdapter):
    source_type = "oracle"

    def discover_capabilities(self) -> AdapterCapabilities:
        return AdapterCapabilities(
            source_type=self.source_type,
            minimum_version="19c",
            supports_cdc=True,
            supports_snapshot=True,
            operations=("insert", "update", "delete"),
            notes="Expected CDC source is redo / LogMiner compatible pipeline or approved abstraction.",
        )

    def validate_connection(self) -> None:
        required = ("host", "port", "username", "service_name")
        missing = [name for name in required if not self.config.connection.get(name)]
        if missing:
            raise ValueError(f"oracle connection is missing required fields: {', '.join(missing)}")

    def start_stream(self, checkpoint: SourceCheckpoint | None) -> Iterable[ChangeEvent]:
        raise NotImplementedError("oracle CDC streaming is not implemented in the scaffold yet")

    def stop_stream(self) -> None:
        return None

    def read_snapshot(self, checkpoint: SourceCheckpoint | None = None) -> Iterable[ChangeEvent]:
        return iter(())

    def serialize_checkpoint(self, event: ChangeEvent) -> str:
        return event.checkpoint_token or event.change_version

    def resume_from_checkpoint(self, checkpoint: SourceCheckpoint | None) -> None:
        return None

