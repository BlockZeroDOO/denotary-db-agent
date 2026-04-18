from __future__ import annotations

from denotary_db_agent.adapters.base import AdapterCapabilities, ScaffoldCdcAdapter


class SqlServerAdapter(ScaffoldCdcAdapter):
    source_type = "sqlserver"
    required_connection_fields = ("host", "port", "username", "database")

    def discover_capabilities(self) -> AdapterCapabilities:
        return AdapterCapabilities(
            source_type=self.source_type,
            minimum_version="2019",
            supports_cdc=True,
            supports_snapshot=True,
            operations=("insert", "update", "delete"),
            capture_modes=("snapshot", "cdc", "change_tracking"),
            bootstrap_requirements=("cdc or change tracking enabled", "tracked tables visible"),
            notes="Expected CDC source is SQL Server CDC or Change Tracking depending on edition.",
        )
