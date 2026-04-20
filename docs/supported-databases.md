# Supported Database Targets

## Wave 1 Targets

| Adapter | Target path | Minimum version | CDC path | Current production-validation status |
| --- | --- | --- | --- | --- |
| PostgreSQL | enterprise | 14 | watermark polling, trigger CDC, logical decoding, pgoutput streaming | live local full-cycle, stable Jungle4 path, and real `denotary` mainnet validation |
| MySQL | enterprise | 8.0 | watermark polling baseline plus shared row-based binlog CDC baseline | live local full-cycle, source-restart validation, short-soak validation, long-soak validation, plus real `denotary` mainnet validation |
| MariaDB | enterprise | 10.6 | watermark polling baseline plus shared row-based binlog CDC baseline | live local full-cycle, source-restart validation, short-soak validation, long-soak validation, plus real `denotary` mainnet validation |
| SQL Server | enterprise | 2019 | watermark polling baseline plus native Change Tracking CDC baseline | live local full-cycle, source-restart validation, short-soak validation, long-soak validation, plus real `denotary` mainnet validation |
| Oracle | enterprise | 19c | watermark polling baseline plus LogMiner CDC baseline | live local full-cycle, source-restart validation, short-soak validation, long-soak validation, plus real `denotary` mainnet validation |
| MongoDB | enterprise | 6.0 | watermark polling baseline and change streams | live local full-cycle, source-restart validation, short-soak validation, long-soak validation, plus real `denotary` mainnet validation |

## Common v1 Rules

- `insert`, `update`, and `delete` are the target operations
- raw row payload remains off-chain
- canonical event hashes and source metadata are notarized
- request preparation uses `Ingress API`
- finality and receipts stay in the existing `deNotary` backend
- for PostgreSQL, MySQL, MariaDB, SQL Server, Oracle, and MongoDB `single` events, sign/broadcast and proof retrieval already happen inside the plugin

## Explicitly Out Of Scope For v1

- Redis
- SQLite
- ClickHouse
- Elasticsearch / OpenSearch
- Cassandra
- native in-database extensions
## Wave 2 Targets

| Adapter | Initial target path | Planned capture path | Current status |
| --- | --- | --- | --- |
| Redis | operational state | explicit key-pattern snapshot baseline, then local full-cycle proof export; later keyspace notifications where justified | adapter contract, config surface, live ping, snapshot baseline, local full-cycle validation, Docker-backed live validation, restart recovery, and short-soak validation implemented |
| ScyllaDB | distributed wide-column | Cassandra-compatible snapshot / watermark baseline, later optional Scylla-specific CDC | adapter contract, config surface, shared Cassandra-compatible snapshot baseline, dedicated adapter alias, local full-cycle proof export, Docker-backed live validation, restart recovery, short-soak validation, real `denotary` mainnet validation, and deployment/runbook layer implemented |
| IBM Db2 | enterprise | snapshot / watermark baseline, later native change capture if justified | adapter contract, config surface, live ping, tracked-table introspection, snapshot baseline, local full-cycle proof export, env-gated live validation harness, and unified live-validation checklist coverage implemented |
| Apache Cassandra | distributed wide-column | partition-aware snapshot baseline, later optional CDC | adapter contract, config surface, live ping, tracked-table introspection, snapshot baseline, local full-cycle proof export, env-gated live validation harness, and unified live-validation checklist coverage implemented |
| Elasticsearch | search / operational index | query-based snapshot and incremental baseline | adapter contract, config surface, live ping, tracked-index introspection, snapshot baseline, local full-cycle proof export, env-gated live validation harness, and unified live-validation checklist coverage implemented |
| SQLite | embedded / edge | local snapshot / watermark baseline | adapter contract, config surface, file-backed readiness validation, tracked-table introspection, snapshot baseline, local full-cycle proof export, cold restart recovery, and short-soak validation implemented |
