from __future__ import annotations

from denotary_db_agent.adapters.base import ScaffoldCdcAdapter


class MySqlAdapter(ScaffoldCdcAdapter):
    source_type = "mysql"
    minimum_version = "8.0"
    required_connection_fields = ("host", "port", "username", "database")
    scaffold_capture_modes = ("snapshot", "binlog")
    scaffold_bootstrap_requirements = ("row-based binlog enabled", "tracked tables visible")
    scaffold_notes = "Expected CDC source is row-based binlog."
