from __future__ import annotations

from denotary_db_agent.adapters.base import AdapterCapabilities, ScaffoldCdcAdapter


class OracleAdapter(ScaffoldCdcAdapter):
    source_type = "oracle"
    required_connection_fields = ("host", "port", "username", "service_name")

    def discover_capabilities(self) -> AdapterCapabilities:
        return AdapterCapabilities(
            source_type=self.source_type,
            minimum_version="19c",
            supports_cdc=True,
            supports_snapshot=True,
            operations=("insert", "update", "delete"),
            notes="Expected CDC source is redo / LogMiner compatible pipeline or approved abstraction.",
        )
