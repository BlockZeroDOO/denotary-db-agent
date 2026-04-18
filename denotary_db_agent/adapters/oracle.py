from __future__ import annotations

from denotary_db_agent.adapters.base import ScaffoldCdcAdapter


class OracleAdapter(ScaffoldCdcAdapter):
    source_type = "oracle"
    minimum_version = "19c"
    required_connection_fields = ("host", "port", "username", "service_name")
    scaffold_capture_modes = ("snapshot", "logminer")
    scaffold_bootstrap_requirements = ("redo or logminer access", "tracked tables visible")
    scaffold_notes = "Expected CDC source is redo / LogMiner compatible pipeline or approved abstraction."
