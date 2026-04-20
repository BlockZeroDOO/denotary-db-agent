from __future__ import annotations

from denotary_db_agent.adapters.base import BaseAdapter
from denotary_db_agent.adapters.cassandra import CassandraAdapter
from denotary_db_agent.adapters.db2 import Db2Adapter
from denotary_db_agent.adapters.elasticsearch import ElasticsearchAdapter
from denotary_db_agent.adapters.mariadb import MariaDbAdapter
from denotary_db_agent.adapters.mongodb import MongoDbAdapter
from denotary_db_agent.adapters.mysql import MySqlAdapter
from denotary_db_agent.adapters.oracle import OracleAdapter
from denotary_db_agent.adapters.postgres import PostgresAdapter
from denotary_db_agent.adapters.redis import RedisAdapter
from denotary_db_agent.adapters.snowflake import SnowflakeAdapter
from denotary_db_agent.adapters.sqlserver import SqlServerAdapter
from denotary_db_agent.config import SourceConfig


ADAPTERS: dict[str, type[BaseAdapter]] = {
    "postgresql": PostgresAdapter,
    "mysql": MySqlAdapter,
    "mariadb": MariaDbAdapter,
    "sqlserver": SqlServerAdapter,
    "oracle": OracleAdapter,
    "mongodb": MongoDbAdapter,
    "snowflake": SnowflakeAdapter,
    "redis": RedisAdapter,
    "db2": Db2Adapter,
    "cassandra": CassandraAdapter,
    "elasticsearch": ElasticsearchAdapter,
}


def build_adapter(config: SourceConfig) -> BaseAdapter:
    adapter_type = ADAPTERS.get(config.adapter)
    if adapter_type is None:
        raise ValueError(f"unsupported adapter: {config.adapter}")
    return adapter_type(config)
