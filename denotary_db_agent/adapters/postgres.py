from __future__ import annotations

from collections.abc import Iterable

from denotary_db_agent.adapters.base import AdapterCapabilities, BaseAdapter
from denotary_db_agent.models import ChangeEvent, SourceCheckpoint


class PostgresAdapter(BaseAdapter):
    source_type = "postgresql"

    def discover_capabilities(self) -> AdapterCapabilities:
        return AdapterCapabilities(
            source_type=self.source_type,
            minimum_version="14",
            supports_cdc=True,
            supports_snapshot=True,
            operations=("insert", "update", "delete"),
            notes="Reference adapter for WAL / logical decoding based CDC. Current scaffold validates config and dry-run flows.",
        )

    def validate_connection(self) -> None:
        required = ("host", "port", "username", "database")
        missing = [name for name in required if not self.config.connection.get(name)]
        if missing:
            raise ValueError(f"postgresql connection is missing required fields: {', '.join(missing)}")

    def start_stream(self, checkpoint: SourceCheckpoint | None) -> Iterable[ChangeEvent]:
        raise NotImplementedError("postgresql CDC streaming is not implemented in the scaffold yet")

    def stop_stream(self) -> None:
        return None

    def read_snapshot(self, checkpoint: SourceCheckpoint | None = None) -> Iterable[ChangeEvent]:
        dry_events = self.config.options.get("dry_run_events") or []
        for item in dry_events:
            yield ChangeEvent(
                source_id=self.config.id,
                source_type=self.source_type,
                source_instance=self.config.source_instance,
                database_name=self.config.database_name,
                schema_or_namespace=str(item.get("schema_or_namespace", "public")),
                table_or_collection=str(item.get("table_or_collection", "records")),
                operation=str(item.get("operation", "snapshot")),  # type: ignore[arg-type]
                primary_key=dict(item.get("primary_key") or {}),
                change_version=str(item.get("change_version", item.get("checkpoint_token", "0"))),
                commit_timestamp=str(item.get("commit_timestamp", "1970-01-01T00:00:00Z")),
                before=item.get("before"),
                after=item.get("after") or {},
                metadata=dict(item.get("metadata") or {}),
                checkpoint_token=str(item.get("checkpoint_token", item.get("change_version", "0"))),
            )

    def serialize_checkpoint(self, event: ChangeEvent) -> str:
        return event.checkpoint_token or event.change_version

    def resume_from_checkpoint(self, checkpoint: SourceCheckpoint | None) -> None:
        return None

