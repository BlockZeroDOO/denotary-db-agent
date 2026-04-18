from __future__ import annotations

from denotary_db_agent.adapters.base import AdapterCapabilities, ScaffoldCdcAdapter


class MongoDbAdapter(ScaffoldCdcAdapter):
    source_type = "mongodb"
    required_connection_fields = ("uri",)

    def discover_capabilities(self) -> AdapterCapabilities:
        return AdapterCapabilities(
            source_type=self.source_type,
            minimum_version="6.0",
            supports_cdc=True,
            supports_snapshot=True,
            operations=("insert", "update", "delete"),
            capture_modes=("snapshot", "change_streams"),
            bootstrap_requirements=("replica set or sharded cluster", "tracked collections visible"),
            notes="Expected CDC source is MongoDB change streams.",
        )

    def _missing_connection_error(self, missing: list[str]) -> str:
        return f"mongodb connection is missing required field: {', '.join(missing)}"
