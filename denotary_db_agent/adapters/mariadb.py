from __future__ import annotations

from denotary_db_agent.adapters.base import AdapterCapabilities, ScaffoldCdcAdapter


class MariaDbAdapter(ScaffoldCdcAdapter):
    source_type = "mariadb"
    required_connection_fields = ("host", "port", "username", "database")

    def discover_capabilities(self) -> AdapterCapabilities:
        return AdapterCapabilities(
            source_type=self.source_type,
            minimum_version="10.6",
            supports_cdc=True,
            supports_snapshot=True,
            operations=("insert", "update", "delete"),
            notes="Expected CDC source is MariaDB binlog with adapter-specific profile.",
        )
