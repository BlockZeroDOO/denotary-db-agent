from __future__ import annotations

from denotary_db_agent.adapters.base import ScaffoldCdcAdapter


class MongoDbAdapter(ScaffoldCdcAdapter):
    source_type = "mongodb"
    minimum_version = "6.0"
    required_connection_fields = ("uri",)
    scaffold_capture_modes = ("snapshot", "change_streams")
    scaffold_bootstrap_requirements = ("replica set or sharded cluster", "tracked collections visible")
    scaffold_notes = "Expected CDC source is MongoDB change streams."
