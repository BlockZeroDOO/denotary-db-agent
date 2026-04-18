from __future__ import annotations

from denotary_db_agent.adapters.base import AdapterCapabilities, ScaffoldCdcAdapter


class MySqlAdapter(ScaffoldCdcAdapter):
    source_type = "mysql"
    required_connection_fields = ("host", "port", "username", "database")

    def discover_capabilities(self) -> AdapterCapabilities:
        return AdapterCapabilities(
            source_type=self.source_type,
            minimum_version="8.0",
            supports_cdc=True,
            supports_snapshot=True,
            operations=("insert", "update", "delete"),
            capture_modes=("snapshot", "binlog"),
            bootstrap_requirements=("row-based binlog enabled", "tracked tables visible"),
            notes="Expected CDC source is row-based binlog.",
        )
