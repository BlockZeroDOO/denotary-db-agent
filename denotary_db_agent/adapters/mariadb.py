from __future__ import annotations

from denotary_db_agent.adapters.base import ScaffoldCdcAdapter


class MariaDbAdapter(ScaffoldCdcAdapter):
    source_type = "mariadb"
    minimum_version = "10.6"
    required_connection_fields = ("host", "port", "username", "database")
    scaffold_capture_modes = ("snapshot", "binlog")
    scaffold_bootstrap_requirements = ("binlog access", "tracked tables visible")
    scaffold_notes = "Expected CDC source is MariaDB binlog with adapter-specific profile."
