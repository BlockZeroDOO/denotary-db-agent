from __future__ import annotations

from denotary_db_agent.adapters.mysql import MySqlAdapter


class MariaDbAdapter(MySqlAdapter):
    source_type = "mariadb"
    minimum_version = "10.6"
    adapter_notes = (
        "MariaDB supports both watermark-based snapshot polling and a shared binlog CDC baseline. "
        "Live MariaDB binlog harness validation remains the next MariaDB-specific step."
    )
