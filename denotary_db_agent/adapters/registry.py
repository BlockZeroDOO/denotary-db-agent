from __future__ import annotations

from denotary_db_agent.adapters.base import BaseAdapter
from denotary_db_agent.adapters.mariadb import MariaDbAdapter
from denotary_db_agent.adapters.mongodb import MongoDbAdapter
from denotary_db_agent.adapters.mysql import MySqlAdapter
from denotary_db_agent.adapters.oracle import OracleAdapter
from denotary_db_agent.adapters.postgres import PostgresAdapter
from denotary_db_agent.adapters.sqlserver import SqlServerAdapter
from denotary_db_agent.config import SourceConfig


ADAPTERS: dict[str, type[BaseAdapter]] = {
    "postgresql": PostgresAdapter,
    "mysql": MySqlAdapter,
    "mariadb": MariaDbAdapter,
    "sqlserver": SqlServerAdapter,
    "oracle": OracleAdapter,
    "mongodb": MongoDbAdapter,
}


def build_adapter(config: SourceConfig) -> BaseAdapter:
    adapter_type = ADAPTERS.get(config.adapter)
    if adapter_type is None:
        raise ValueError(f"unsupported adapter: {config.adapter}")
    return adapter_type(config)

