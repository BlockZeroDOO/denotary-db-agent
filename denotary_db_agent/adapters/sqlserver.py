from __future__ import annotations

from denotary_db_agent.adapters.base import ScaffoldCdcAdapter


class SqlServerAdapter(ScaffoldCdcAdapter):
    source_type = "sqlserver"
    minimum_version = "2019"
    required_connection_fields = ("host", "port", "username", "database")
    scaffold_capture_modes = ("snapshot", "cdc", "change_tracking")
    scaffold_bootstrap_requirements = ("cdc or change tracking enabled", "tracked tables visible")
    scaffold_notes = "Expected CDC source is SQL Server CDC or Change Tracking depending on edition."
