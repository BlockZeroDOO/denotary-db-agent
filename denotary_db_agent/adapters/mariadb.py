from __future__ import annotations

from denotary_db_agent.adapters.mysql import MySqlAdapter


class MariaDbAdapter(MySqlAdapter):
    source_type = "mariadb"
    minimum_version = "10.6"
    adapter_notes = (
        "Live baseline implementation uses MariaDB watermark-based snapshot polling. "
        "MariaDB binlog CDC remains the next MariaDB-specific step."
    )
