from __future__ import annotations

from denotary_db_agent.adapters.cassandra import CassandraAdapter


class ScyllaDbAdapter(CassandraAdapter):
    source_type = "scylladb"
    minimum_version = "5.0"
    adapter_notes = (
        "Wave 2 ScyllaDB support currently reuses the Cassandra-compatible baseline: "
        "connection-shape validation, live cluster ping, tracked-table introspection, "
        "watermark snapshot polling, deterministic checkpoint resume, and dry-run snapshot playback. "
        "Native Scylla-specific CDC can be added later if commercially justified."
    )
